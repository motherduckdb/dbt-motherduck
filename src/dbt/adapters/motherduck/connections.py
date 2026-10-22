from contextlib import contextmanager
from threading import Event
from time import perf_counter
from traceback import format_exc

import psycopg
import sqlparse
from dbt.adapters.contracts.connection import AdapterResponse, ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import (
    ConnectionUsed,
    Rollback,
    RollbackFailed,
    SQLQuery,
    SQLQueryStatus,
)
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.clients.agate_helper import empty_table
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtDatabaseError, DbtInternalError, DbtRuntimeError
from dbt_common.utils import cast_to_str
from psycopg import sql as pg_sql
from sqlparse import lexer

from dbt.adapters.motherduck.browser_auth import (
    BrowserAuthenticator,
    BrowserAuthError,
    redact,
)


class MotherDuckConnectionManager(SQLConnectionManager):
    TYPE = "motherduck"

    def __init__(self, profile, mp_context):
        super().__init__(profile, mp_context)
        self.invocation_canceled = Event()
        # dbt-motherduck connects to MotherDuck on single attach mode
        # https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/attach-modes/
        # this callback is set by the MotherDuckAdapter (which owns the connection) and
        # returns which databases are needed to run the dbt project.
        self.databases_needed_callback = self._uninitialized_databases_needed
        # Shared by all threads so `authenticator: externalbrowser` logs in once per run.
        self.browser_authenticator = BrowserAuthenticator(
            canceled=self.invocation_canceled
        )

    def open(self, connection):
        if self.invocation_canceled.is_set():
            raise DbtRuntimeError(
                "The dbt invocation was cancelled. No further MotherDuck connections will be opened."
            )
        if connection.state == ConnectionState.OPEN:
            return connection
        credentials = connection.credentials
        databases = self.databases_needed_callback()
        handle = None
        setting_name = None
        try:
            handle = self._connect(credentials)

            # register the handle before any queries run so canceling the invocation
            # can close the connection
            with self.lock:
                if self.invocation_canceled.is_set():
                    handle.close()
                    raise DbtRuntimeError(
                        "The dbt invocation was cancelled. No further MotherDuck connections will be opened."
                    )
                connection.handle = handle
                connection.state = ConnectionState.OPEN
            self._attach_databases(handle, databases)
            with handle.cursor() as cursor:
                for setting_name, value in credentials.settings.items():
                    # Like dbt-duckdb, let DuckDB cast string values to setting types.
                    cursor.execute(
                        pg_sql.SQL("SET {} = {}").format(
                            pg_sql.Identifier(setting_name), pg_sql.Literal(str(value))
                        )
                    )
        except (psycopg.Error, BrowserAuthError) as exc:
            if handle is not None:
                handle.close()
            connection.handle = None
            connection.state = ConnectionState.FAIL
            message = str(exc)
            if setting_name is not None:
                message = (
                    f"Could not apply MotherDuck setting {setting_name!r}: {message}"
                )
            raise FailedToConnectError(self._redact(message, credentials)) from None
        return connection

    def _connect(self, credentials):
        def connect(password):
            return psycopg.connect(
                host=credentials.host,
                port=credentials.port,
                user=credentials.user,
                password=password,
                dbname=credentials.database,
                sslmode="verify-full",
                sslrootcert=credentials.sslrootcert,
                connect_timeout=credentials.connect_timeout,
                options="-c attach_mode=single",
                # dbt's connection manager owns explicit BEGIN/COMMIT boundaries.
                autocommit=True,
            )

        if credentials.authenticator != "externalbrowser":
            return connect(credentials.token)
        token = self.browser_authenticator.get_token()
        try:
            return connect(token)
        except psycopg.errors.InvalidPassword:
            # The cached login was revoked or expired early: log in again once.
            self.browser_authenticator.invalidate(token)
            return connect(self.browser_authenticator.get_token())

    @staticmethod
    def _redact(message, credentials):
        return redact(message, credentials.token)

    def release(self):
        # default .release() just .close()s the connections,
        # we want to reuse them within a dbt thread.
        # .set_connection_name() will find open connections per thread if they are not closed
        connection = self.get_if_exists()

        # only execute if connection is open
        if connection is None:
            return
        if connection.state == ConnectionState.FAIL:
            self.clear_thread_connection()
            return
        if connection.state != ConnectionState.OPEN:
            return
        if self.invocation_canceled.is_set():
            self.cancel(connection)
            return

        try:
            handle = connection.handle
            status = handle.info.transaction_status
            # something closed the transaction without updating dbt transaction state
            if handle.closed or status == psycopg.pq.TransactionStatus.UNKNOWN:
                connection.transaction_open = False
                self.close(connection)
                return

            if status == psycopg.pq.TransactionStatus.ACTIVE:
                raise DbtInternalError(
                    "The MotherDuck connection is not ready for release: "
                    "a query is still in progress. "
                    "This should never happen. Please contact MotherDuck support at support@motherduck.com."
                )
            if handle.pgconn.pipeline_status != psycopg.pq.PipelineStatus.OFF:
                raise DbtInternalError(
                    "The MotherDuck connection is not ready for release: "
                    "pipeline mode is still enabled. "
                    "This should never happen. Please contact MotherDuck support at support@motherduck.com."
                )

            if (
                connection.transaction_open
                and status == psycopg.pq.TransactionStatus.IDLE
            ):
                raise DbtInternalError(
                    "dbt expects an open transaction, but MotherDuck reports no open transaction. "
                    "A custom macro or hook may have run COMMIT or ROLLBACK directly. "
                    "Use adapter.commit() for commits and let dbt handle rollback on failure. "
                    "If this persists, contact MotherDuck support at support@motherduck.com."
                )
            if not connection.transaction_open and status in {
                psycopg.pq.TransactionStatus.INTRANS,
                psycopg.pq.TransactionStatus.INERROR,
            }:
                raise DbtInternalError(
                    "MotherDuck reports an open transaction that dbt is not tracking. "
                    "A custom macro or hook may have run BEGIN directly. "
                    "Use a statement block with auto_begin=True instead of raw BEGIN "
                    "so dbt can track the transaction. "
                    "If this persists, contact MotherDuck support at support@motherduck.com."
                )

            if connection.transaction_open:
                with self.exception_handler("ROLLBACK"):
                    fire_event(
                        Rollback(
                            conn_name=cast_to_str(connection.name),
                            node_info=get_node_info(),
                        )
                    )
                    # dbt's rollback helper suppresses errors, which is unsafe for reuse.
                    handle.rollback()
            connection.transaction_open = False
            if handle.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                self.close(connection)
        except Exception:
            connection.transaction_open = False
            try:
                self.close(connection)
            finally:
                self.clear_thread_connection()
            raise

    def cancel_open(self):
        # Stop late submissions before disconnecting workers; dbt then joins them.
        self.invocation_canceled.set()
        this_connection = self.get_if_exists()
        names = []
        with self.lock:
            for connection in list(self.thread_connections.values()):
                if connection is this_connection:
                    continue
                # Check state first: reading an INIT handle would open a lazy connection.
                if connection.state == ConnectionState.OPEN:
                    self.cancel(connection)
                if connection.name is not None:
                    names.append(connection.name)
        return names

    def cancel(self, connection):
        try:
            # direct close without explicit rollback to not wait for an active query
            connection.handle.close()
            connection.transaction_open = False
            connection.state = ConnectionState.CLOSED
        except Exception as exc:
            message = self._redact(str(exc), connection.credentials)
            AdapterLogger("MotherDuck").info(
                f"Could not close connection '{connection.name}' during cancellation: {message}. "
                "Continuing cancelling the remaining connections."
            )

    def execute(self, sql, auto_begin=False, fetch=False, limit=None):
        with self.exception_handler(sql):
            _, cursor = self.add_query(self._add_query_comment(sql), auto_begin)
            try:
                response = self.get_response(cursor)
                table = (
                    self.get_result_from_cursor(cursor, limit)
                    if fetch
                    else empty_table()
                )
                return response, table
            finally:
                cursor.close()

    def add_query(
        self,
        sql,
        auto_begin=True,
        bindings=None,
        abridge_sql_log=False,
        retryable_exceptions=(),
        retry_limit=1,
    ):
        if self.invocation_canceled.is_set():
            raise DbtRuntimeError(
                "The dbt invocation was cancelled. No further MotherDuck SQL will be executed."
            )
        # we cannot send multiple queries as a single string because the endpoint tries to
        # prepare and bind all statements before executing any.
        # e.g. if we send "create table t; insert into table t;", the insert statement
        # fails to prepare because the table doesn't exist yet.
        # we send the statements in postgres pipelines or one-by-one

        # filter-out non-executable statements, but preserve all comments
        # dbt-snowflake and dbt-redshift do drop trailing comments
        statements = []
        leading_comments = []
        for statement in sqlparse.split(sql):
            tokens = list(lexer.tokenize(statement))
            has_sql = any(
                token not in sqlparse.tokens.Whitespace
                and token not in sqlparse.tokens.Comment
                and value != ";"
                for token, value in tokens
            )
            if has_sql:
                statements.append("\n".join([*leading_comments, statement]))
                leading_comments.clear()
            else:
                comments = "".join(value for _, value in tokens if value != ";").strip()
                if not comments:
                    continue
                if statements:
                    statements[-1] += "\n" + comments
                else:
                    leading_comments.append(comments)

        if bindings is not None and len(statements) > 1:
            # duckdb accepts them, pg_endpoint doesn't
            raise DbtRuntimeError(
                "dbt-motherduck does not support multiple SQL statements with bindings "
                "in one call. Execute each statement separately with its own bindings."
            )

        if not statements:
            # if after splitting there's no executable statements, we send it to the engine.
            # alternatively, we could raise an error like other adapters do (redshift,
            # snowflake), but dbt-duckdb (and dbt-postgres) send them to the engines so
            # let's just do that to avoid thinking about edge cases of people potentially
            # producing non-executable statements on macros or hooks.
            return super().add_query(
                sql=sql,
                auto_begin=auto_begin,
                bindings=bindings,
                abridge_sql_log=abridge_sql_log,
                retryable_exceptions=retryable_exceptions,
                retry_limit=retry_limit,
            )

        connection = self.get_thread_connection()
        if (
            len(statements) > 1
            # pipeline errors cannot use dbt's per-statement retry path
            and not retryable_exceptions
            # only use pipelines if we're inside a transaction or we are opening one
            and (connection.transaction_open or auto_begin)
        ):
            if not connection.transaction_open:
                self.begin()
            return self._add_pipeline_queries(connection, statements, abridge_sql_log)

        cursor = None
        for statement in statements:
            if cursor is not None:
                cursor.close()
            connection, cursor = super().add_query(
                sql=self._without_statement_terminator(statement),
                auto_begin=auto_begin,
                bindings=bindings,
                abridge_sql_log=abridge_sql_log,
                retryable_exceptions=retryable_exceptions,
                retry_limit=retry_limit,
            )
        return connection, cursor

    @classmethod
    def get_response(cls, cursor):
        status = cursor.statusmessage or "OK"
        # DuckDB DML returns a Count row; cursor.rowcount counts that result row.
        affected = cursor.pgresult.command_tuples
        code = status.split()[0]
        # UPDATE/DELETE tags count the result row, and MERGE has an UNKNOWN tag.
        if code in {"UPDATE", "DELETE"} or (
            code == "UNKNOWN"
            and cursor.pgresult.nfields == 1
            and cursor.pgresult.fname(0) == b"Count"
        ):
            count = cursor.pgresult.get_value(0, 0)
            affected = int(count) if count is not None else None
            if code != "UNKNOWN":
                status = code
        return AdapterResponse(
            _message=status,
            code=code,
            rows_affected=affected,
        )

    @classmethod
    def _rollback_handle(cls, connection):
        try:
            connection.handle.rollback()
        except Exception:
            # redact token, the traceback can include the original query error
            # as well as the rollback error with the profile credentials.
            fire_event(
                RollbackFailed(
                    conn_name=cast_to_str(connection.name),
                    exc_info=cls._redact(format_exc(), connection.credentials),
                    node_info=get_node_info(),
                )
            )

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except psycopg.Error as exc:
            connection = self.get_thread_connection()
            message = self._redact(str(exc), connection.credentials)
            handle = connection.handle
            self.rollback_if_open()
            # dbt logs rollback failures; never reuse a session left in an unsafe state.
            if (
                handle.closed
                or handle.info.transaction_status != psycopg.pq.TransactionStatus.IDLE
                or handle.pgconn.pipeline_status != psycopg.pq.PipelineStatus.OFF
            ):
                handle.close()
                connection.state = ConnectionState.CLOSED
            raise DbtDatabaseError(message) from None

    @staticmethod
    def _attach_databases(handle, databases):
        requested_databases = {name.casefold(): name for name in databases}
        if not requested_databases:
            return
        with handle.cursor() as cursor:
            attached_databases = cursor.execute(
                "select alias from md_attached_databases()"
            ).fetchall()
            attached_database_names = {row[0].casefold() for row in attached_databases}
            pending_databases = requested_databases.keys() - attached_database_names
            if not pending_databases:
                return
            for key in sorted(pending_databases):
                # Let ATTACH resolve databases and shares and validate access.
                cursor.execute(
                    pg_sql.SQL("attach {}").format(
                        pg_sql.Literal(f"md:{requested_databases[key]}")
                    )
                )

    @staticmethod
    def _uninitialized_databases_needed():
        raise DbtInternalError(
            "The adapter must initialize databases_needed_callback before opening a connection. "
            "This code should never be reached. If you encounter this error, "
            "contact MotherDuck support at support@motherduck.com."
        )

    def _add_pipeline_queries(self, connection, statements, abridge_sql_log):
        cursors = []
        try:
            with self.exception_handler("\n".join(statements)):
                started_timer = perf_counter()
                with connection.handle.pipeline():
                    for statement in statements:
                        fire_event(
                            ConnectionUsed(
                                conn_type=self.TYPE,
                                conn_name=cast_to_str(connection.name),
                                node_info=get_node_info(),
                            )
                        )
                        fire_event(
                            SQLQuery(
                                conn_name=cast_to_str(connection.name),
                                sql=f"{statement[:512]}..."
                                if abridge_sql_log
                                else statement,
                                node_info=get_node_info(),
                            )
                        )
                        cursor = connection.handle.cursor()
                        cursors.append(cursor)
                        cursor.execute(
                            self._without_statement_terminator(statement), prepare=False
                        )
                # the client sends/received the whole pipeline at once.
                # pg_endpoint could in principle return metadata with runtime per statement.
                elapsed = perf_counter() - started_timer
                for cursor in cursors:
                    response = self.get_response(cursor)
                    fire_event(
                        SQLQueryStatus(
                            status=str(response),
                            elapsed=elapsed,
                            node_info=get_node_info(),
                            query_id=response.query_id,
                        )
                    )
                cursor = cursors.pop()
                return connection, cursor
        finally:
            for cursor in cursors:
                cursor.close()

    @staticmethod
    def _without_statement_terminator(statement):
        # e.g `select a; --comment` => `select a --comment`
        # pg_endpoint treats comments after a terminator as a statement
        tokens = list(lexer.tokenize(statement))
        for index in range(len(tokens) - 1, -1, -1):
            token, value = tokens[index]
            if token in sqlparse.tokens.Whitespace or token in sqlparse.tokens.Comment:
                continue
            if value == ";":
                tokens[index] = (token, "")
            break
        return "".join(value for _, value in tokens)
