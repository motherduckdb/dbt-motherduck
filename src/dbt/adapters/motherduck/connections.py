from contextlib import contextmanager

import psycopg
import sqlparse
from dbt.adapters.contracts.connection import AdapterResponse, ConnectionState
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.clients.agate_helper import empty_table
from dbt_common.exceptions import DbtDatabaseError, DbtInternalError, DbtRuntimeError
from psycopg import sql as pg_sql
from sqlparse import lexer


class MotherDuckConnectionManager(SQLConnectionManager):
    TYPE = "motherduck"

    def __init__(self, profile, mp_context):
        super().__init__(profile, mp_context)
        # dbt-motherduck connects to MotherDuck on single attach mode
        # https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/attach-modes/
        # this callback is set by the MotherDuckAdapter (which owns the connection) and
        # returns which databases are needed to run the dbt project.
        self.databases_needed_callback = self._uninitialized_databases_needed

    @staticmethod
    def _uninitialized_databases_needed():
        raise DbtInternalError(
            "The adapter must initialize databases_needed_callback before opening a connection. "
            "This code should never be reached. If you encounter this error, "
            "contact MotherDuck support at support@motherduck.com."
        )

    @staticmethod
    def _attach_databases(handle, databases):
        requested_databases = {name.casefold(): name for name in databases}
        if not requested_databases:
            return
        with handle.cursor() as cursor:
            attached_databases = cursor.execute(
                "select alias from md_attached_databases()"
            ).fetchall()
            attached_database_names = set(
                row[0].casefold() for row in attached_databases
            )
            pending_databases = requested_databases.keys() - attached_database_names
            if not pending_databases:
                return
            available_databases = cursor.execute(
                "select alias from md_all_databases()"
            ).fetchall()
            database_aliases = {
                row[0].casefold(): row[0] for row in available_databases
            }
            for key in sorted(pending_databases):
                if key not in database_aliases:
                    raise psycopg.OperationalError(
                        f"MotherDuck database '{requested_databases[key]}' does not exist or is not accessible"
                    )
                cursor.execute(
                    pg_sql.SQL("attach {}").format(
                        pg_sql.Literal(f"md:{database_aliases[key]}")
                    )
                )

    def open(self, connection):
        if connection.state == ConnectionState.OPEN:
            return connection
        credentials = connection.credentials
        databases = self.databases_needed_callback()
        handle = None
        try:
            handle = psycopg.connect(
                host=credentials.host,
                port=credentials.port,
                user=credentials.user,
                password=credentials.token,
                dbname=credentials.database,
                sslmode="verify-full",
                sslrootcert=credentials.sslrootcert,
                connect_timeout=credentials.connect_timeout,
                options="-c attach_mode=single",
                # dbt's connection manager owns explicit BEGIN/COMMIT boundaries.
                autocommit=True,
            )
            self._attach_databases(handle, databases)
        except psycopg.Error as exc:
            if handle is not None:
                handle.close()
            connection.handle = None
            connection.state = ConnectionState.FAIL
            message = str(exc).replace(credentials.token, "[REDACTED]")
            raise FailedToConnectError(message) from None
        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except psycopg.Error as exc:
            connection = self.get_thread_connection()
            message = str(exc).replace(connection.credentials.token, "[REDACTED]")
            # Discard failed sessions rather than replaying SQL or reusing uncertain state.
            connection.handle.close()
            connection.transaction_open = False
            connection.state = ConnectionState.CLOSED
            raise DbtDatabaseError(message) from None

    @classmethod
    def get_response(cls, cursor):
        status = cursor.statusmessage or "OK"
        # DuckDB DML returns a Count row; cursor.rowcount counts that result row.
        affected = cursor.pgresult.command_tuples
        code = status.split()[0]
        # The endpoint reports UPDATE/DELETE 1 even when zero or many rows change.
        if code in {"UPDATE", "DELETE"}:
            status, affected = code, None
        return AdapterResponse(
            _message=status,
            code=code,
            rows_affected=affected,
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
        # we cannot send multiple queries as a single string because the endpoint tries to
        # prepare and bind all statements before executing any.
        # e.g. if we send "create table t; insert into table t;", the insert statement
        # fails to prepare because the table doesn't exist yet.
        # we send the statements one-by-one

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

    def cancel(self, connection):
        connection.handle.cancel_safe()
