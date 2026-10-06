from contextlib import contextmanager

import psycopg
import sqlparse
from dbt.adapters.contracts.connection import AdapterResponse, ConnectionState
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.clients.agate_helper import empty_table
from dbt_common.exceptions import DbtDatabaseError, DbtInternalError
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
            attached_database_names = {row[0].casefold() for row in attached_databases}
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
        # The endpoint binds a whole request before executing CREATE statements in it.
        statements = sqlparse.split(sql) if bindings is None else [sql]
        statements = [
            statement
            for statement in statements
            if any(
                token not in sqlparse.tokens.Whitespace
                and token not in sqlparse.tokens.Comment
                and value != ";"
                for token, value in lexer.tokenize(statement)
            )
        ]
        cursor = None
        for statement in statements or [sql]:
            if cursor is not None:
                cursor.close()
            connection, cursor = super().add_query(
                statement,
                auto_begin,
                bindings,
                abridge_sql_log,
                retryable_exceptions,
                retry_limit,
            )
        return connection, cursor

    def cancel(self, connection):
        connection.handle.cancel_safe()
