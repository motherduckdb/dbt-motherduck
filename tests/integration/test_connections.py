from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
from queue import Queue
from socket import AF_INET, SHUT_RDWR, SOCK_STREAM, fromfd
from time import monotonic, sleep

import psycopg
import pytest
from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.exceptions import FailedToConnectError
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.motherduck.impl import MotherDuckAdapter


def _wait_for_active(handle, future):
    deadline = monotonic() + 10
    while handle.info.transaction_status != psycopg.pq.TransactionStatus.ACTIVE:
        if future.done():
            future.result()
            pytest.fail("Query finished before interruption")
        assert monotonic() < deadline, "Query did not become active"
        sleep(0.01)


def _close_query_socket(query_socket, future):
    try:
        if not future.done():
            # Unblock libpq before executor shutdown if cancellation failed.
            try:
                query_socket.shutdown(SHUT_RDWR)
            except OSError:
                pass
    finally:
        query_socket.close()


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
        assert handle.execute(
            "select current_setting('motherduck_attach_mode')"
        ).fetchone() == ("single",)
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


def test_response_metadata_and_fetch_limit(connection_adapter):
    with connection_adapter.connection_named("responses"):
        response, table = connection_adapter.execute(
            "CREATE TABLE main.response_probe (id INTEGER)"
        )
        assert response.code == "CREATE"
        assert len(table.rows) == 0
        try:
            response, _ = connection_adapter.execute(
                "INSERT INTO main.response_probe VALUES (1), (2), (3)"
            )
            assert response.code == "INSERT"
            assert response.rows_affected == 3
            response, table = connection_adapter.execute(
                "SELECT id FROM main.response_probe ORDER BY id", fetch=True, limit=2
            )
            assert response.rows_affected == 3
            assert [row[0] for row in table.rows] == [1, 2]
            _, table = connection_adapter.execute(
                "SELECT id FROM main.response_probe WHERE false", fetch=True
            )
            assert table.column_names == ("id",)
            assert len(table.rows) == 0
            response, _ = connection_adapter.execute(
                "UPDATE main.response_probe SET id = id + 10 WHERE id < 3"
            )
            assert response.code == "UPDATE"
            assert response.rows_affected is None
            response, _ = connection_adapter.execute(
                "DELETE FROM main.response_probe WHERE id > 10"
            )
            assert response.code == "DELETE"
            assert response.rows_affected is None
            response, _ = connection_adapter.execute(
                "DELETE FROM main.response_probe WHERE id > 10"
            )
            assert response.rows_affected is None
            _, table = connection_adapter.execute(
                "SELECT id FROM main.response_probe", fetch=True
            )
            assert [row[0] for row in table.rows] == [3]
        finally:
            with connection_adapter.connection_named("responses_cleanup"):
                connection_adapter.execute("DROP TABLE main.response_probe")


def test_dependent_statements_and_rollback(connection_adapter):
    with connection_adapter.connection_named("dependent_statements"):
        _, table = connection_adapter.execute(
            "CREATE TABLE main.statement_probe (value VARCHAR); "
            "INSERT INTO main.statement_probe VALUES ('a;b'), ($literal$c;d$literal$); "
            "-- semicolon in a comment;\nSELECT value FROM main.statement_probe ORDER BY value; ; /* tail; */ -- last comment",
            auto_begin=True,
            fetch=True,
        )
        assert [row[0] for row in table.rows] == ["a;b", "c;d"]
        connection_adapter.commit_if_has_connection()
        with pytest.raises(DbtDatabaseError):
            connection_adapter.execute(
                "INSERT INTO main.statement_probe VALUES ('rollback'); "
                "SELECT error('intentional statement failure')",
                auto_begin=True,
            )
    with connection_adapter.connection_named("verify_statement_rollback"):
        _, table = connection_adapter.execute(
            "SELECT count(*) FROM main.statement_probe", fetch=True
        )
        assert table.rows[0][0] == 2
        connection_adapter.execute("DROP TABLE main.statement_probe")


@pytest.mark.parametrize("finish", ["commit", "rollback", "release"])
def test_pipeline_transaction_lifecycle(connection_adapter, finish):
    with connection_adapter.connection_named("pipeline_setup"):
        connection_adapter.execute("CREATE TABLE main.pipeline_probe (id INTEGER)")
    try:
        with connection_adapter.connection_named("pipeline"):
            response, table = connection_adapter.execute(
                "INSERT INTO main.pipeline_probe VALUES (1), (2), (3); "
                "SELECT id FROM main.pipeline_probe ORDER BY id; /* trailing; */",
                auto_begin=True,
                fetch=True,
                limit=2,
            )
            connection = connection_adapter.connections.get_thread_connection()
            handle = connection.handle
            assert response.code == "SELECT"
            assert response.rows_affected == 3
            assert [row[0] for row in table.rows] == [1, 2]
            assert connection.transaction_open
            assert (
                handle.info.transaction_status == psycopg.pq.TransactionStatus.INTRANS
            )
            assert handle.pgconn.pipeline_status == psycopg.pq.PipelineStatus.OFF
            if finish == "commit":
                connection_adapter.commit_if_has_connection()
            elif finish == "rollback":
                connection_adapter.connections.rollback_if_open()
            if finish != "release":
                assert not connection.transaction_open
                assert (
                    handle.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
                )
        assert handle.closed
        with connection_adapter.connection_named("pipeline_verify"):
            _, table = connection_adapter.execute(
                "SELECT count(*) FROM main.pipeline_probe", fetch=True
            )
            assert table.rows[0][0] == (3 if finish == "commit" else 0)
    finally:
        with connection_adapter.connection_named("pipeline_cleanup"):
            connection_adapter.execute("DROP TABLE main.pipeline_probe")


@pytest.mark.parametrize("position", [0, 1, 2], ids=["first", "middle", "last"])
@pytest.mark.parametrize(
    "failing_sql, message",
    [
        ("SELECT FROM", "(?i)syntax"),
        (
            "SELECT * FROM intentionally_missing_pipeline_table",
            "intentionally_missing_pipeline_table",
        ),
        (
            "SELECT error(cast(i AS VARCHAR) || ':intentional pipeline failure') FROM range(1) AS t(i)",
            "0:intentional pipeline failure",
        ),
    ],
    ids=["syntax", "binding", "runtime"],
)
def test_pipeline_errors_preserve_cause_and_rollback(
    connection_adapter, position, failing_sql, message
):
    with connection_adapter.connection_named("pipeline_error_setup"):
        connection_adapter.execute(
            "CREATE TABLE main.pipeline_error_probe (id INTEGER)"
        )
    try:
        with connection_adapter.connection_named("pipeline_error"):
            connection_adapter.execute("SELECT 1", auto_begin=True)
            connection = connection_adapter.connections.get_thread_connection()
            handle = connection.handle
            statements = [
                "INSERT INTO main.pipeline_error_probe VALUES (1)",
                "INSERT INTO main.pipeline_error_probe VALUES (2)",
            ]
            statements.insert(position, failing_sql)
            with pytest.raises(DbtDatabaseError, match=message) as error:
                connection_adapter.execute("; ".join(statements))
            assert connection_adapter.config.credentials.token not in str(error.value)
            assert error.value.__suppress_context__
            assert handle.closed
            assert not connection.transaction_open
            assert connection.state == ConnectionState.CLOSED
        with connection_adapter.connection_named("pipeline_error_verify"):
            _, table = connection_adapter.execute(
                "SELECT count(*) FROM main.pipeline_error_probe", fetch=True
            )
            assert table.rows[0][0] == 0
            assert (
                connection_adapter.connections.get_thread_connection().handle
                is not handle
            )
    finally:
        with connection_adapter.connection_named("pipeline_error_cleanup"):
            connection_adapter.execute("DROP TABLE main.pipeline_error_probe")


def test_nontransactional_batch_failure_preserves_committed_prefix(connection_adapter):
    with connection_adapter.connection_named("nontransactional_setup"):
        connection_adapter.execute(
            "CREATE TABLE main.nontransactional_probe (id INTEGER)"
        )
    try:
        with connection_adapter.connection_named("nontransactional_batch"):
            with pytest.raises(
                DbtDatabaseError, match="intentional nontransactional failure"
            ):
                connection_adapter.execute(
                    "INSERT INTO main.nontransactional_probe VALUES (1); "
                    "SELECT error('intentional nontransactional failure'); "
                    "INSERT INTO main.nontransactional_probe VALUES (2)",
                    auto_begin=False,
                )
        with connection_adapter.connection_named("nontransactional_verify"):
            _, table = connection_adapter.execute(
                "SELECT id FROM main.nontransactional_probe", fetch=True
            )
            assert [row[0] for row in table.rows] == [1]
    finally:
        with connection_adapter.connection_named("nontransactional_cleanup"):
            connection_adapter.execute("DROP TABLE main.nontransactional_probe")


def test_pipeline_many_results_and_final_dml_response(connection_adapter):
    with connection_adapter.connection_named("many_results"):
        connection_adapter.execute("CREATE TABLE main.many_results_probe (id INTEGER)")
        try:
            statements = [
                f"SELECT id: i + {number * 1000} FROM range(128) AS t(i)"
                for number in range(50)
            ]
            statements.append(
                "INSERT INTO main.many_results_probe SELECT * FROM range(17)"
            )
            response, table = connection_adapter.execute(
                "; ".join(statements), auto_begin=True
            )
            assert response.code == "INSERT"
            assert response.rows_affected == 17
            assert len(table.rows) == 0
            response, table = connection_adapter.execute(
                "SELECT 42; SELECT id FROM main.many_results_probe ORDER BY id",
                fetch=True,
                limit=3,
            )
            assert response.code == "SELECT"
            assert response.rows_affected == 17
            assert [row[0] for row in table.rows] == [0, 1, 2]
            connection_adapter.commit_if_has_connection()
        finally:
            with connection_adapter.connection_named("many_results_cleanup"):
                connection_adapter.connections.rollback_if_open()
                connection_adapter.execute("DROP TABLE main.many_results_probe")


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


def test_bound_types_and_result_conversion(connection_adapter):
    values = (
        None,
        Decimal("123456789.123456789"),
        datetime(2025, 1, 2, 3, 4, 5, 123456),
        datetime(2025, 1, 2, 1, 4, 5, tzinfo=timezone.utc),
        b"\x00\xffA",
        [1, None, 3],
        "quotes ' ; and non-ASCII: café",
    )
    with connection_adapter.connection_named("types"):
        connection_adapter.execute(
            "CREATE TABLE main.type_probe (missing INTEGER, amount DECIMAL(20,9), "
            "ts TIMESTAMP, tz TIMESTAMPTZ, payload BLOB, items INTEGER[], label VARCHAR)"
        )
        try:
            _, cursor = connection_adapter.connections.add_query(
                "INSERT INTO main.type_probe VALUES (%s, %s, %s, %s, %s, [1, NULL, 3], %s)",
                auto_begin=False,
                bindings=(*values[:5], values[6]),
            )
            cursor.close()
            _, cursor = connection_adapter.connections.add_query(
                "SELECT * FROM main.type_probe", auto_begin=False
            )
            try:
                assert cursor.fetchone() == values
                assert [column.type_code for column in cursor.description] == [
                    23,
                    1700,
                    1114,
                    1184,
                    17,
                    1007,
                    25,
                ]
            finally:
                cursor.close()
            _, table = connection_adapter.execute(
                "SELECT * FROM main.type_probe", fetch=True
            )
            assert tuple(table.rows[0]) == (
                *values[:4],
                str(values[4]),
                "[1, null, 3]",
                values[6],
            )
            _, columns = connection_adapter.execute(
                "DESCRIBE main.type_probe", fetch=True
            )
            assert [row[1] for row in columns.rows] == [
                "INTEGER",
                "DECIMAL(20,9)",
                "TIMESTAMP",
                "TIMESTAMP WITH TIME ZONE",
                "BLOB",
                "INTEGER[]",
                "VARCHAR",
            ]
            _, nested = connection_adapter.execute(
                "SELECT {'id': 1, 'name': 'a;b'} AS nested", fetch=True
            )
            assert nested.rows[0][0] == "{'id': 1, 'name': a;b}"
        finally:
            with connection_adapter.connection_named("types_cleanup"):
                connection_adapter.execute("DROP TABLE main.type_probe")


def test_concurrent_session_isolation(connection_adapter):
    with connection_adapter.connection_named("reader"):
        connection_adapter.execute("CREATE TABLE main.isolation_probe (id INTEGER)")
        reader = connection_adapter.connections.get_thread_connection().handle
        messages = Queue()
        proceed = Queue()

        def write():
            with connection_adapter.connection_named("writer"):
                connection_adapter.execute(
                    "INSERT INTO main.isolation_probe VALUES (1)", auto_begin=True
                )
                writer = connection_adapter.connections.get_thread_connection().handle
                messages.put(writer)
                assert proceed.get(timeout=30) == "commit"
                connection_adapter.commit_if_has_connection()
            return writer.closed

        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(write)
                try:
                    writer = messages.get(timeout=30)
                    assert writer is not reader
                    _, table = connection_adapter.execute(
                        "SELECT count(*) FROM main.isolation_probe", fetch=True
                    )
                    assert table.rows[0][0] == 0
                finally:
                    proceed.put("commit")
                assert future.result(timeout=30)
            assert not reader.closed
            _, table = connection_adapter.execute(
                "SELECT count(*) FROM main.isolation_probe", fetch=True
            )
            assert table.rows[0][0] == 1
        finally:
            connection_adapter.execute("DROP TABLE main.isolation_probe")


@pytest.mark.parametrize("operation", ["cancel", "cancel_open", "disconnect"])
@pytest.mark.parametrize("position", ["single", "first", "middle", "last"])
def test_interrupted_queries_rollback_and_reconnect(
    connection_adapter, operation, position
):
    connections = Queue()
    slow_query = "SELECT sum(sin(i)) FROM range(1000000000) AS t(i)"
    statements = [
        "INSERT INTO main.interrupted_probe VALUES (1)",
        "INSERT INTO main.interrupted_probe VALUES (2)",
    ]
    if position == "single":
        statements = [slow_query]
    else:
        statements.insert(["first", "middle", "last"].index(position), slow_query)

    def run_query():
        with connection_adapter.connection_named("interrupt_target"):
            connection_adapter.execute(
                "INSERT INTO main.interrupted_probe VALUES (-1)", auto_begin=True
            )
            connection = connection_adapter.connections.get_thread_connection()
            handle = connection.handle
            connections.put((connection, fromfd(handle.fileno(), AF_INET, SOCK_STREAM)))
            message = (
                "(?i)closed|connection|EOF|SSL|receive"
                if operation == "disconnect"
                else "(?i)interrupt|cancel"
            )
            with pytest.raises(DbtDatabaseError, match=message):
                connection_adapter.execute("; ".join(statements), fetch=True)
            assert handle.closed
            assert not connection.transaction_open
            assert connection.state == ConnectionState.CLOSED
        with connection_adapter.connection_named("interrupt_recovered"):
            _, table = connection_adapter.execute(
                "SELECT 42; SELECT 43", auto_begin=True, fetch=True
            )
            assert table.rows[0][0] == 43
            replacement = connection_adapter.connections.get_thread_connection().handle
            assert replacement is not handle
            connection_adapter.commit_if_has_connection()
        assert replacement.closed
        return handle

    with connection_adapter.connection_named("interrupt_observer"):
        connection_adapter.execute("CREATE TABLE main.interrupted_probe (id INTEGER)")
        observer = connection_adapter.connections.get_thread_connection().handle
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                handles = []
                for _ in range(2):
                    future = executor.submit(run_query)
                    connection, query_socket = connections.get(timeout=30)
                    handle = connection.handle
                    try:
                        _wait_for_active(handle, future)
                        if operation == "cancel":
                            connection_adapter.connections.cancel(connection)
                        elif operation == "cancel_open":
                            assert (
                                "interrupt_target"
                                in connection_adapter.connections.cancel_open()
                            )
                        else:
                            query_socket.shutdown(SHUT_RDWR)
                        handles.append(future.result(timeout=30))
                    finally:
                        _close_query_socket(query_socket, future)
                    _, table = connection_adapter.execute(
                        "SELECT count(*) FROM main.interrupted_probe", fetch=True
                    )
                    assert table.rows[0][0] == 0
                    assert (
                        connection_adapter.connections.get_thread_connection().handle
                        is observer
                    )
                    assert not observer.closed
                assert handles[0] is not handles[1]
        finally:
            connection_adapter.execute("DROP TABLE main.interrupted_probe")


def test_cancel_open_interrupts_multiple_workers(connection_adapter):
    connections = Queue()

    def run_query(name):
        with connection_adapter.connection_named(name):
            connection_adapter.execute("SELECT 1", auto_begin=True)
            connection = connection_adapter.connections.get_thread_connection()
            handle = connection.handle
            connections.put(
                (name, handle, fromfd(handle.fileno(), AF_INET, SOCK_STREAM))
            )
            with pytest.raises(DbtDatabaseError, match="(?i)interrupt|cancel"):
                connection_adapter.execute(
                    "SELECT 42; SELECT sum(sin(i)) FROM range(1000000000) AS t(i); SELECT 43"
                )
            assert handle.closed
            assert not connection.transaction_open
            assert connection.state == ConnectionState.CLOSED

    with connection_adapter.connection_named("multi_cancel_observer"):
        connection_adapter.execute("SELECT 1")
        observer = connection_adapter.connections.get_thread_connection().handle
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                name: executor.submit(run_query, name)
                for name in ("worker_0", "worker_1")
            }
            sockets = []
            try:
                handles = []
                for _ in futures:
                    name, handle, query_socket = connections.get(timeout=30)
                    sockets.append((query_socket, futures[name]))
                    handles.append(handle)
                for handle, (_, future) in zip(handles, sockets):
                    _wait_for_active(handle, future)
                assert handles[0] is not handles[1]
                assert set(connection_adapter.connections.cancel_open()) == set(futures)
                for future in futures.values():
                    future.result(timeout=30)
            finally:
                for query_socket, future in sockets:
                    _close_query_socket(query_socket, future)
        _, table = connection_adapter.execute("SELECT 42", fetch=True)
        assert table.rows[0][0] == 42
        assert connection_adapter.connections.get_thread_connection().handle is observer
        assert not observer.closed


def test_cancel_idle_connection_remains_usable(connection_adapter):
    with connection_adapter.connection_named("idle_cancel"):
        connection_adapter.execute("SELECT 1")
        connection = connection_adapter.connections.get_thread_connection()
        handle = connection.handle
        for _ in range(3):
            connection_adapter.connections.cancel(connection)
            _, table = connection_adapter.execute("SELECT 42", fetch=True)
            assert table.rows[0][0] == 42
            assert connection.handle is handle
            assert not handle.closed
