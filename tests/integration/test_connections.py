import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime, timezone
from decimal import Decimal
from queue import Queue
from socket import AF_INET, SHUT_RDWR, SOCK_STREAM, fromfd
from threading import Event
from time import monotonic, sleep
from traceback import format_exception

import psycopg
import pytest
from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.events.types import AdapterEventInfo
from dbt.adapters.exceptions import FailedToConnectError
from dbt_common.events import get_event_manager
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError

from dbt.adapters.motherduck.impl import MotherDuckAdapter


def test_profile_and_connection(connection_adapter, test_database, admin_connection):
    assert admin_connection.execute(
        # sanity check
        "select current_setting('motherduck_attach_mode')"
    ).fetchone() == ("single",)
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
    assert not handle.closed
    assert connection.state == ConnectionState.OPEN
    with connection_adapter.connection_named("reused_connection"):
        assert connection_adapter.connections.get_thread_connection() is connection
        assert connection.handle is handle
        _, table = connection_adapter.execute("SELECT 42", fetch=True)
        assert table.rows[0][0] == 42
    connection_adapter.cleanup_connections()
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
            assert response.rows_affected in (None, 2)
            response, _ = connection_adapter.execute(
                "DELETE FROM main.response_probe WHERE id > 10"
            )
            assert response.code == "DELETE"
            assert response.rows_affected in (None, 2)
            response, _ = connection_adapter.execute(
                "DELETE FROM main.response_probe WHERE id > 10"
            )
            assert response.rows_affected in (None, 0)
            _, table = connection_adapter.execute(
                "SELECT id FROM main.response_probe", fetch=True
            )
            assert [row[0] for row in table.rows] == [3]
        finally:
            with connection_adapter.connection_named("responses_cleanup"):
                connection_adapter.execute("DROP TABLE main.response_probe")


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


def test_bound_statement_with_trailing_comments(connection_adapter):
    with connection_adapter.connection_named("bound_comments"):
        _, cursor = connection_adapter.connections.add_query(
            "select value: %s::varchar; /* trailing; comment */ -- last comment",
            auto_begin=False,
            bindings=("a;b",),
        )
        try:
            assert cursor.fetchone() == ("a;b",)
        finally:
            cursor.close()


@pytest.mark.parametrize("auto_begin", [False, True])
def test_user_comments_reach_driver(connection_adapter, monkeypatch, auto_begin):
    original_execute = psycopg.Cursor.execute
    sent_sql = []
    comments = ["/* leading; */", "-- between;", "/* trailing; */", "-- final comment"]
    with connection_adapter.connection_named("user_comments"):
        connection_adapter.execute("select 1")
        handle = connection_adapter.connections.get_thread_connection().handle

        def record_sql(cursor, query, *args, **kwargs):
            if cursor.connection is handle and isinstance(query, str):
                sent_sql.append(query)
            return original_execute(cursor, query, *args, **kwargs)

        monkeypatch.setattr(psycopg.Cursor, "execute", record_sql)
        _, table = connection_adapter.execute(
            "/* leading; */; select value: 'a;b'; -- between;\n; select answer: 42; /* trailing; */; -- final comment",
            auto_begin=auto_begin,
            fetch=True,
        )
        assert table.rows[0][0] == 42
        assert all(comment in "\n".join(sent_sql) for comment in comments)


@pytest.mark.parametrize("bindings", [(1, 2), {"first": 1, "second": 2}])
def test_multiple_bound_statements_have_actionable_error(connection_adapter, bindings):
    sql = (
        "select %s; select %s"
        if isinstance(bindings, tuple)
        else "select %(first)s; select %(second)s"
    )
    with connection_adapter.connection_named("unsupported_bound_batch"):
        with pytest.raises(
            DbtRuntimeError,
            match="Execute each statement separately with its own bindings",
        ):
            connection_adapter.connections.add_query(sql, bindings=bindings)
        _, table = connection_adapter.execute("select answer: 42", fetch=True)
        assert table.rows[0][0] == 42


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
        assert not handle.closed
        assert not connection.transaction_open
        assert handle.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        with connection_adapter.connection_named("pipeline_verify"):
            assert (
                connection_adapter.connections.get_thread_connection().handle is handle
            )
            _, table = connection_adapter.execute(
                "SELECT count(*) FROM main.pipeline_probe", fetch=True
            )
            assert table.rows[0][0] == (3 if finish == "commit" else 0)
    finally:
        with connection_adapter.connection_named("pipeline_cleanup"):
            connection_adapter.execute("DROP TABLE main.pipeline_probe")


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
            assert connection_adapter.config.credentials.token not in "".join(
                format_exception(error.value)
            )
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
            return writer

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
                assert future.result(timeout=30) is writer
                assert not writer.closed
            assert not reader.closed
            _, table = connection_adapter.execute(
                "SELECT count(*) FROM main.isolation_probe", fetch=True
            )
            assert table.rows[0][0] == 1
        finally:
            connection_adapter.execute("DROP TABLE main.isolation_probe")


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
        assert connection_adapter.config.credentials.token not in "".join(
            format_exception(error.value)
        )


@pytest.mark.parametrize("operation", ["connect", "query"])
def test_driver_errors_redact_credentials(connection_adapter, monkeypatch, operation):
    token = "synthetic-sensitive-token"
    failure = psycopg.OperationalError(f"intentional driver failure containing {token}")
    credentials = connection_adapter.config.credentials
    original_token = credentials.token
    try:
        with connection_adapter.connection_named("redaction"):
            if operation == "connect":

                def fail_connect(*args, **kwargs):
                    raise failure

                monkeypatch.setattr(psycopg, "connect", fail_connect)
            else:
                connection_adapter.execute("select 1")
                handle = connection_adapter.connections.get_thread_connection().handle
                original_execute = psycopg.Cursor.execute

                def fail_query(cursor, query, *args, **kwargs):
                    if cursor.connection is handle:
                        raise failure
                    return original_execute(cursor, query, *args, **kwargs)

                monkeypatch.setattr(psycopg.Cursor, "execute", fail_query)
            # Do not put a real credential into a deliberately raised exception.
            credentials.token = token
            expected_error = (
                FailedToConnectError if operation == "connect" else DbtDatabaseError
            )
            with pytest.raises(expected_error) as error:
                connection_adapter.execute("select 42")
            assert token not in str(error.value)
            assert "[REDACTED]" in str(error.value)
            assert token not in "".join(format_exception(error.value))
    finally:
        credentials.token = original_token


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
        assert connection_adapter.config.credentials.token not in "".join(
            format_exception(error.value)
        )
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
    assert not replacement.closed
    assert not connection_adapter.connections.get_thread_connection().transaction_open


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


@pytest.mark.parametrize(
    "error_type", [psycopg.errors.CancellationTimeout, psycopg.OperationalError]
)
def test_cancel_open_continues_after_cancellation_failure(
    connection_adapter, monkeypatch, error_type
):
    connections = Queue()
    proceed = Event()
    attempted_handles = []
    token = "synthetic-sensitive-token"
    original_cancel = psycopg.Connection.cancel_safe
    cancellation_events = EventCatcher(AdapterEventInfo)
    event_manager = get_event_manager()

    def run_worker(name):
        with connection_adapter.connection_named(name):
            connection_adapter.execute("SELECT 1")
            connection = connection_adapter.connections.get_thread_connection()
            handle = connection.handle
            connections.put(connection)
            assert proceed.wait(timeout=90), "Cancellation attempts did not finish"
            _, table = connection_adapter.execute("SELECT 42", fetch=True)
            assert table.rows[0][0] == 42
            assert connection.handle is handle
            assert not handle.closed

    def cancel(handle, *args, **kwargs):
        attempted_handles.append(handle)
        if handle is failed_connection.handle:
            raise error_type(f"intentional cancellation failure containing {token}")
        return original_cancel(handle, *args, **kwargs)

    with connection_adapter.connection_named("multi_cancel_observer"):
        connection_adapter.execute("SELECT 1")
        observer = connection_adapter.connections.get_thread_connection().handle
        with ThreadPoolExecutor(max_workers=2) as executor:
            try:
                futures = [executor.submit(run_worker, "worker_0")]
                failed_connection = connections.get(timeout=30)
                futures.append(executor.submit(run_worker, "worker_1"))
                other_connection = connections.get(timeout=30)
                credentials = failed_connection.credentials
                original_token = credentials.token
                event_manager.add_callback(cancellation_events.catch)
                try:
                    # Both sessions are already open; never put a real token in the injected error.
                    credentials.token = token
                    with monkeypatch.context() as patch:
                        patch.setattr(psycopg.Connection, "cancel_safe", cancel)
                        patch.setattr(
                            event_manager, "require_warn_or_error_handling", True
                        )
                        patch.setattr(event_manager, "warn_error", True)
                        assert set(connection_adapter.connections.cancel_open()) == {
                            "worker_0",
                            "worker_1",
                        }
                finally:
                    credentials.token = original_token
                    event_manager.callbacks.remove(cancellation_events.catch)
                assert set(attempted_handles) == {
                    failed_connection.handle,
                    other_connection.handle,
                }
                failure_event = next(
                    event
                    for event in cancellation_events.caught_events
                    if "intentional cancellation failure" in event.info.msg
                )
                assert "worker_0" in failure_event.info.msg
                assert "[REDACTED]" in failure_event.info.msg
                assert token not in str(failure_event)
                proceed.set()
                for future in futures:
                    future.result(timeout=30)
            finally:
                proceed.set()
        _, table = connection_adapter.execute("SELECT 42", fetch=True)
        assert table.rows[0][0] == 42
        assert connection_adapter.connections.get_thread_connection().handle is observer
        assert not observer.closed


@pytest.mark.parametrize(
    "operation, position",
    [
        ("cancel", "single"),
        ("cancel", "middle"),
        ("disconnect", "single"),
        ("disconnect", "middle"),
    ],
)
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
        statements.insert(1, slow_query)

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
            try:
                connection_adapter.execute("; ".join(statements), fetch=True)
            except DbtDatabaseError as exc:
                assert re.search(message, str(exc)), str(exc)
            else:
                if operation == "cancel":
                    pytest.skip("Endpoint query completed without being cancelled")
                pytest.fail("Disconnected query unexpectedly succeeded")
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
        assert not replacement.closed
        return handle

    with connection_adapter.connection_named("interrupt_observer"):
        connection_adapter.execute("CREATE TABLE main.interrupted_probe (id INTEGER)")
        observer = connection_adapter.connections.get_thread_connection().handle
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(run_query)
                connection, query_socket = connections.get(timeout=30)
                handle = connection.handle
                try:
                    _wait_for_active(handle, future)
                    if operation == "cancel":
                        connection_adapter.connections.cancel(connection)
                    else:
                        query_socket.shutdown(SHUT_RDWR)
                    try:
                        future.result(timeout=30)
                    except TimeoutError:
                        if (
                            operation == "cancel"
                            and not future.done()
                            and handle.info.transaction_status
                            == psycopg.pq.TransactionStatus.ACTIVE
                        ):
                            pytest.skip(
                                "Endpoint did not interrupt the query after cancellation"
                            )
                        raise
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
        finally:
            connection_adapter.execute("DROP TABLE main.interrupted_probe")


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
