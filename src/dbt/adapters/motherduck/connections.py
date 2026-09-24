from contextlib import contextmanager

import psycopg
from dbt.adapters.contracts.connection import AdapterResponse, ConnectionState
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.clients.agate_helper import empty_table
from dbt_common.exceptions import DbtDatabaseError


class MotherDuckConnectionManager(SQLConnectionManager):
    TYPE = "motherduck"

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            return connection
        credentials = connection.credentials
        try:
            connection.handle = psycopg.connect(
                host=credentials.host,
                port=credentials.port,
                user=credentials.user,
                password=credentials.token,
                dbname=credentials.database,
                sslmode="verify-full",
                sslrootcert=credentials.sslrootcert,
                connect_timeout=credentials.connect_timeout,
                # dbt's connection manager owns explicit BEGIN/COMMIT boundaries.
                autocommit=True,
            )
        except psycopg.Error as exc:
            connection.handle = None
            connection.state = ConnectionState.FAIL
            message = str(exc).replace(credentials.token, "[REDACTED]")
            raise FailedToConnectError(message) from None
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
        return AdapterResponse(
            _message=status,
            code=status.split()[0],
            rows_affected=cursor.rowcount if cursor.rowcount >= 0 else None,
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

    def cancel(self, connection):
        connection.handle.cancel_safe()
