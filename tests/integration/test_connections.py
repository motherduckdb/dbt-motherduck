import psycopg
import pytest
from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.exceptions import FailedToConnectError
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.motherduck.impl import MotherDuckAdapter


def test_profile_and_connection(connection_adapter, test_database):
    assert isinstance(connection_adapter, MotherDuckAdapter)
    assert connection_adapter.config.threads == 2
    credentials = connection_adapter.config.credentials
    assert credentials.database == test_database
    assert credentials.type == "motherduck"
    assert credentials.token not in repr(credentials)
    assert credentials.token not in str(list(credentials.connection_info()))
    with connection_adapter.connection_named("connection"):
        connection = connection_adapter.connections.get_thread_connection()
        handle = connection.handle
        assert isinstance(handle, psycopg.Connection)
        assert handle.autocommit
        parameters = handle.info.get_parameters()
        assert handle.info.host == credentials.host
        assert handle.info.port == credentials.port
        assert handle.info.user == credentials.user
        assert handle.info.dbname == credentials.database
        assert parameters["sslmode"] == "verify-full"
        assert parameters["sslrootcert"] == credentials.sslrootcert
        assert parameters["connect_timeout"] == str(credentials.connect_timeout)
        connection_adapter.connections.open(connection)
        assert connection.handle is handle
        _, table = connection_adapter.execute(
            "SELECT current_database() AS db", fetch=True
        )
        assert table.rows[0]["db"] == credentials.database
    assert handle.closed
    assert connection.state == ConnectionState.CLOSED


def test_execute_converts_results(connection_adapter):
    with connection_adapter.connection_named("select"):
        response, table = connection_adapter.execute("SELECT 1 AS answer", fetch=True)
        assert table.column_names == ("answer",)
        assert table.rows[0]["answer"] == 1
        assert response.code == "SELECT"
        assert response.rows_affected == 1


def test_query_failure_discards_connection(connection_adapter):
    with connection_adapter.connection_named("query_failure"):
        connection_adapter.execute("SELECT 1", auto_begin=True)
        connection = connection_adapter.connections.get_thread_connection()
        handle = connection.handle
        assert connection.transaction_open
        with pytest.raises(DbtDatabaseError) as error:
            connection_adapter.execute(
                "SELECT * FROM dbt_motherduck_intentionally_missing_function()"
            )
        assert connection_adapter.config.credentials.token not in str(error.value)
        assert error.value.__suppress_context__
        assert handle.closed
        assert connection.state == ConnectionState.CLOSED
        assert not connection.transaction_open
    with connection_adapter.connection_named("after_failure"):
        _, table = connection_adapter.execute(
            "SELECT 2 AS answer", auto_begin=True, fetch=True
        )
        replacement = connection_adapter.connections.get_thread_connection().handle
        assert connection_adapter.connections.get_thread_connection().transaction_open
        assert replacement is not handle
        assert table.rows[0]["answer"] == 2
    assert replacement.closed


def test_rejected_authentication(connection_adapter):
    connection_adapter.config.credentials.token = (
        "dbt-motherduck-intentionally-invalid-token"
    )
    with connection_adapter.connection_named("invalid_token"):
        with pytest.raises(FailedToConnectError) as error:
            connection_adapter.execute("SELECT 1")
        connection = connection_adapter.connections.get_thread_connection()
        assert connection.state == ConnectionState.FAIL
        assert connection.handle is None
        assert connection_adapter.config.credentials.token not in str(error.value)
        assert error.value.__suppress_context__
