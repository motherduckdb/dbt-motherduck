from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier, RLock
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest
from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.events.types import SQLQuery, SQLQueryStatus
from dbt.adapters.exceptions import FailedToConnectError
from dbt_common.exceptions import DbtDatabaseError, DbtInternalError, DbtRuntimeError

from dbt.adapters.motherduck.connections import MotherDuckConnectionManager
from dbt.adapters.motherduck.credentials import MotherDuckCredentials


@pytest.fixture
def manager(monkeypatch):
    handle = Mock()
    handle.in_pipeline = False
    handle.enter_error = None
    handle.pipeline_error = None
    handle.execute_error = None
    handle.cursors = []
    handle.queries = []
    events = []

    def complete(cursor):
        cursor.pgresult = SimpleNamespace(command_tuples=1)
        cursor.statusmessage = "SELECT 1"

    def new_cursor():
        cursor = Mock(pgresult=None, statusmessage=None)

        def execute(statement, bindings=None, *, prepare=None):
            handle.queries.append((statement, bindings, prepare, handle.in_pipeline))
            if handle.execute_error and len(handle.queries) == 2:
                raise handle.execute_error
            if not handle.in_pipeline:
                complete(cursor)

        cursor.execute.side_effect = execute
        handle.cursors.append(cursor)
        return cursor

    @contextmanager
    def pipeline():
        if handle.enter_error:
            raise handle.enter_error
        handle.in_pipeline = True
        try:
            yield
            if handle.pipeline_error:
                raise handle.pipeline_error
            for cursor in handle.cursors:
                complete(cursor)
        finally:
            handle.in_pipeline = False

    handle.cursor.side_effect = new_cursor
    handle.pipeline.side_effect = pipeline
    connection = SimpleNamespace(
        name="test",
        handle=handle,
        transaction_open=True,
        state=ConnectionState.OPEN,
        credentials=SimpleNamespace(token="secret-token"),
    )
    manager = object.__new__(MotherDuckConnectionManager)
    manager.get_thread_connection = lambda: connection
    manager.events = events
    original_response = manager.get_response

    def get_response(cursor):
        assert not handle.in_pipeline
        return original_response(cursor)

    manager.get_response = get_response
    monkeypatch.setattr("dbt.adapters.motherduck.connections.fire_event", events.append)
    monkeypatch.setattr("dbt.adapters.sql.connections.fire_event", events.append)
    return manager


@pytest.mark.parametrize(
    "sql, expected_statements",
    [
        (
            "select 'a;b'; -- comment;\nselect $literal$c;d$literal$; /* tail; */ -- end",
            [
                "select 'a;b' -- comment;",
                "select $literal$c;d$literal$\n/* tail; */ -- end",
            ],
        ),
        ("select 1; ; select 2; ;; /* tail */", ["select 1", "select 2\n/* tail */"]),
        (
            "/* lead */ select 1; /* between */ select 2; /* tail */",
            ["/* lead */ select 1", "/* between */ select 2\n/* tail */"],
        ),
        (
            "-- lead\nselect ';';\nselect $tag$;/* text */$tag$; ; /* tail */",
            ["-- lead\nselect ';'", "select $tag$;/* text */$tag$\n/* tail */"],
        ),
        pytest.param(
            "select " + ", ".join(map(str, range(6000))) + "; select 2",
            ["select " + ", ".join(map(str, range(6000))), "select 2"],
            id="large_column_list",
        ),
        pytest.param(
            "select " + "(" * 110 + "1" + ")" * 110 + "; select 2",
            ["select " + "(" * 110 + "1" + ")" * 110, "select 2"],
            id="deep_expression",
        ),
    ],
)
def test_pipeline_synchronizes_before_reading_responses(
    manager, monkeypatch, sql, expected_statements
):
    clock = Mock(side_effect=[10.0, 12.5])
    monkeypatch.setattr("dbt.adapters.motherduck.connections.perf_counter", clock)
    connection, cursor = manager.add_query(sql, auto_begin=False)
    handle = connection.handle
    handle.pipeline.assert_called_once()
    assert [query[0] for query in handle.queries] == expected_statements
    assert all(query[2:] == (False, True) for query in handle.queries)
    assert cursor is handle.cursors[-1]
    handle.cursors[0].close.assert_called_once()
    cursor.close.assert_not_called()
    assert [
        type(event)
        for event in manager.events
        if isinstance(event, (SQLQuery, SQLQueryStatus))
    ] == [
        SQLQuery,
        SQLQuery,
        SQLQueryStatus,
        SQLQueryStatus,
    ]
    assert clock.call_count == 2
    assert [
        event.elapsed for event in manager.events if isinstance(event, SQLQueryStatus)
    ] == [2.5, 2.5]
    assert connection.transaction_open
    cursor.close()


@pytest.mark.parametrize("transaction_open", [False, True])
def test_batches_preserve_standalone_comments(manager, transaction_open):
    connection = manager.get_thread_connection()
    connection.transaction_open = transaction_open
    _, cursor = manager.add_query(
        "/* lead */; ; SELECT 1; -- first;\n; /* between */; "
        "/* next */ SELECT 2; -- last;\n; /* tail */; -- end\n",
        auto_begin=False,
    )
    assert connection.handle.pipeline.called is transaction_open
    assert [query[0] for query in connection.handle.queries] == [
        "/* lead */\nSELECT 1 -- first;\n/* between */",
        "/* next */ SELECT 2 -- last;\n/* tail */ -- end",
    ]
    cursor.close()


@pytest.mark.parametrize(
    "auto_begin, transaction_open, pipelined",
    [(True, False, True), (False, True, True), (False, False, False)],
)
def test_pipeline_respects_transaction_boundaries(
    manager, auto_begin, transaction_open, pipelined
):
    connection = manager.get_thread_connection()
    connection.transaction_open = transaction_open
    _, cursor = manager.add_query("select 1; select 2", auto_begin=auto_begin)
    assert connection.handle.pipeline.called is pipelined
    if auto_begin:
        assert connection.handle.queries[0] == ("BEGIN", None, None, False)
    assert connection.transaction_open is (auto_begin or transaction_open)
    cursor.close()


@pytest.mark.parametrize(
    "statement, bindings",
    [
        ("select 1", None),
        ("select %s", ("a;b",)),
        ("select ';', %s /* comment; */", (10,)),
        ("select $tag$a;b$tag$, %s", (20,)),
        ("select %(value)s", {"value": 10}),
        ("", None),
        ("   ", None),
        ("; ;", None),
        ("-- comment", None),
        ("/* comment */", None),
        ("/* lead */; -- middle;\n; /* tail */", None),
        ("/* lead */; -- middle;\n; /* tail */", ()),
    ],
)
def test_single_statements_keep_standard_execution(manager, statement, bindings):
    connection, cursor = manager.add_query(
        statement, auto_begin=False, bindings=bindings
    )
    connection.handle.pipeline.assert_not_called()
    assert connection.handle.queries == [(statement, bindings, None, False)]
    cursor.close()


@pytest.mark.parametrize(
    "statement, bindings, expected_statement",
    [
        ("select 1;", None, "select 1"),
        ("select ';'; -- comment;", None, "select ';' -- comment;"),
        ("select %s; -- comment;", ("a;b",), "select %s -- comment;"),
        (
            "select %(value)s; /* comment; */",
            {"value": 10},
            "select %(value)s\n/* comment; */",
        ),
        (
            "select $tag$a;b$tag$; -- comment;",
            None,
            "select $tag$a;b$tag$ -- comment;",
        ),
        (
            "/* leading */ select 1; /* trailing */",
            None,
            "/* leading */ select 1\n/* trailing */",
        ),
        (
            "/* leading */; select %s; /* trailing; */; -- end",
            (10,),
            "/* leading */\nselect %s\n/* trailing; */ -- end",
        ),
    ],
)
def test_single_statements_remove_terminator(
    manager, statement, bindings, expected_statement
):
    connection, cursor = manager.add_query(
        statement, auto_begin=False, bindings=bindings
    )
    connection.handle.pipeline.assert_not_called()
    assert connection.handle.queries == [(expected_statement, bindings, None, False)]
    cursor.close()


@pytest.mark.parametrize(
    "sql, bindings",
    [
        ("select %s; select %s", (10, 20)),
        ("select 1; select %s", (20,)),
        ("select %(value)s; select %(value)s", {"value": 10}),
        ("/* lead */ select %s; /* between */ select 2; /* tail */", (10,)),
        ("select 1; select 2", ()),
        ("select 1; select 2", {}),
    ],
)
def test_multiple_statements_with_bindings_fail_before_execution(
    manager, sql, bindings
):
    connection = manager.get_thread_connection()
    connection.transaction_open = False
    with pytest.raises(
        DbtRuntimeError, match="multiple SQL statements with bindings"
    ) as error:
        manager.add_query(sql, bindings=bindings)
    assert "Execute each statement separately with its own bindings." in str(
        error.value
    )
    connection.handle.cursor.assert_not_called()
    connection.handle.pipeline.assert_not_called()
    connection.handle.close.assert_not_called()
    assert connection.handle.queries == []
    assert not connection.transaction_open
    assert connection.state == ConnectionState.OPEN


def test_retryable_queries_keep_standard_execution(manager):
    connection, cursor = manager.add_query(
        "select 1; -- first;\nselect 2; -- last;",
        auto_begin=False,
        retryable_exceptions=(psycopg.OperationalError,),
        retry_limit=2,
    )
    connection.handle.pipeline.assert_not_called()
    assert connection.handle.queries == [
        ("select 1 -- first;", None, None, False),
        ("select 2 -- last;", None, None, False),
    ]
    cursor.close()


@pytest.mark.parametrize("phase", ["enter", "execute", "pipeline"])
def test_pipeline_failure_discards_session_and_closes_cursors(manager, phase):
    connection = manager.get_thread_connection()
    setattr(
        connection.handle,
        f"{phase}_error",
        psycopg.OperationalError("failure secret-token"),
    )
    with pytest.raises(DbtDatabaseError, match="failure \\[REDACTED\\]") as error:
        manager.add_query("select 1; select 2; select 3", auto_begin=False)
    assert error.value.__suppress_context__
    assert not connection.transaction_open
    assert connection.state == ConnectionState.CLOSED
    connection.handle.close.assert_called_once()
    for cursor in connection.handle.cursors:
        cursor.close.assert_called_once()


@pytest.fixture
def session_manager(monkeypatch):
    credentials = MotherDuckCredentials(
        host="example.com", token="secret-token", database="db", schema="main"
    )
    connections = MotherDuckConnectionManager(
        SimpleNamespace(credentials=credentials), SimpleNamespace(RLock=RLock)
    )
    connections.databases_needed_callback = Mock(return_value=("db", "other"))
    monkeypatch.setattr(connections, "_attach_databases", Mock())

    def connect(**kwargs):
        handle = Mock(closed=False)
        handle.info = SimpleNamespace(
            transaction_status=psycopg.pq.TransactionStatus.IDLE
        )
        handle.pgconn = SimpleNamespace(pipeline_status=psycopg.pq.PipelineStatus.OFF)

        def rollback():
            handle.info.transaction_status = psycopg.pq.TransactionStatus.IDLE

        def close():
            handle.closed = True

        handle.rollback.side_effect = rollback
        handle.close.side_effect = close
        return handle

    monkeypatch.setattr(psycopg, "connect", Mock(side_effect=connect))
    try:
        yield connections
    finally:
        connections.cleanup_all()


def test_release_does_not_open_unused_connections(session_manager):
    session_manager.release()
    connection = session_manager.set_connection_name("unused")
    session_manager.release()
    session_manager.release()
    assert connection.state == ConnectionState.INIT
    session_manager.cleanup_all()
    assert session_manager.thread_connections == {}
    psycopg.connect.assert_not_called()
    session_manager.databases_needed_callback.assert_not_called()


def test_release_reuses_session_and_attachments(session_manager):
    first = session_manager.set_connection_name("first_model")
    handle = first.handle
    for name in ("second_model", "third_model"):
        session_manager.release()
        session_manager.release()
        connection = session_manager.set_connection_name(name)
        assert connection is first
        assert connection.handle is handle
        assert connection.name == name
        assert connection.state == ConnectionState.OPEN
    psycopg.connect.assert_called_once()
    session_manager.databases_needed_callback.assert_called_once()
    session_manager._attach_databases.assert_called_once_with(handle, ("db", "other"))
    handle.rollback.assert_not_called()
    handle.cursor.assert_not_called()
    handle.close.assert_not_called()
    session_manager.cleanup_all()
    handle.close.assert_called_once()
    assert connection.state == ConnectionState.CLOSED
    assert session_manager.thread_connections == {}


@pytest.mark.parametrize(
    "status",
    [
        psycopg.pq.TransactionStatus.INTRANS,
        psycopg.pq.TransactionStatus.INERROR,
    ],
)
def test_release_rolls_back_unfinished_transactions(session_manager, status):
    connection = session_manager.set_connection_name("unfinished_model")
    handle = connection.handle
    connection.transaction_open = True
    handle.info.transaction_status = status
    session_manager.release()
    session_manager.release()
    handle.rollback.assert_called_once()
    handle.commit.assert_not_called()
    handle.close.assert_not_called()
    assert connection.state == ConnectionState.OPEN
    assert not connection.transaction_open
    assert handle.info.transaction_status == psycopg.pq.TransactionStatus.IDLE


@pytest.mark.parametrize(
    "transaction_open, status, advice",
    [
        (True, psycopg.pq.TransactionStatus.IDLE, "adapter.commit()"),
        (False, psycopg.pq.TransactionStatus.INTRANS, "auto_begin=True"),
        (False, psycopg.pq.TransactionStatus.INERROR, "auto_begin=True"),
    ],
)
def test_release_rejects_mismatched_transaction_state(
    session_manager, transaction_open, status, advice
):
    connection = session_manager.set_connection_name("transaction_mismatch")
    handle = connection.handle
    connection.transaction_open = transaction_open
    handle.info.transaction_status = status
    with pytest.raises(DbtInternalError) as raised:
        session_manager.release()
    assert advice in str(raised.value)
    assert "support@motherduck.com" in str(raised.value)
    assert handle.closed
    assert connection.state == ConnectionState.CLOSED
    assert not connection.transaction_open
    handle.close.assert_called_once()
    handle.rollback.assert_not_called()
    handle.commit.assert_not_called()
    assert session_manager.get_if_exists() is None
    session_manager.release()
    psycopg.connect.assert_called_once()
    assert session_manager.set_connection_name("replacement").handle is not handle


@pytest.mark.parametrize(
    "error, expected_error",
    [
        (psycopg.OperationalError("rollback failed secret-token"), DbtDatabaseError),
        (RuntimeError("rollback failed"), RuntimeError),
    ],
)
def test_release_discards_session_when_rollback_fails(
    session_manager, error, expected_error
):
    connection = session_manager.set_connection_name("failed_cleanup")
    handle = connection.handle
    connection.transaction_open = True
    handle.info.transaction_status = psycopg.pq.TransactionStatus.INTRANS
    handle.rollback.side_effect = error
    with pytest.raises(expected_error, match="rollback failed") as raised:
        session_manager.release()
    assert "secret-token" not in str(raised.value)
    assert handle.closed
    assert not connection.transaction_open
    assert connection.state == ConnectionState.CLOSED
    assert session_manager.get_if_exists() is None
    replacement = session_manager.set_connection_name("next_model").handle
    assert replacement is not handle
    assert psycopg.connect.call_count == 2


@pytest.mark.parametrize(
    "closed, status, pipeline_status",
    [
        (True, psycopg.pq.TransactionStatus.UNKNOWN, psycopg.pq.PipelineStatus.OFF),
        (False, psycopg.pq.TransactionStatus.UNKNOWN, psycopg.pq.PipelineStatus.OFF),
        (True, psycopg.pq.TransactionStatus.ACTIVE, psycopg.pq.PipelineStatus.ON),
        (
            False,
            psycopg.pq.TransactionStatus.UNKNOWN,
            psycopg.pq.PipelineStatus.ABORTED,
        ),
    ],
)
def test_release_discards_broken_sessions(
    session_manager, closed, status, pipeline_status
):
    connection = session_manager.set_connection_name("unusable")
    handle = connection.handle
    connection.transaction_open = True
    handle.closed = closed
    handle.info.transaction_status = status
    handle.pgconn.pipeline_status = pipeline_status
    session_manager.release()
    assert handle.closed
    assert connection.state == ConnectionState.CLOSED
    assert not connection.transaction_open
    handle.rollback.assert_not_called()
    assert session_manager.set_connection_name("replacement").handle is not handle


def test_release_discards_session_if_rollback_does_not_clear_transaction(
    session_manager,
):
    connection = session_manager.set_connection_name("uncleared_transaction")
    handle = connection.handle
    connection.transaction_open = True
    handle.info.transaction_status = psycopg.pq.TransactionStatus.INTRANS
    handle.rollback.side_effect = None
    session_manager.release()
    assert handle.closed
    assert connection.state == ConnectionState.CLOSED
    assert not connection.transaction_open


def test_release_does_not_reopen_failed_sessions(session_manager):
    connection = session_manager.set_connection_name("query_failure")
    handle = connection.handle
    with pytest.raises(DbtDatabaseError, match="query failed"):
        with session_manager.exception_handler("select 1"):
            raise psycopg.OperationalError("query failed")
    session_manager.release()
    psycopg.connect.assert_called_once()
    assert handle.closed
    assert session_manager.set_connection_name("recovered").handle is not handle


def test_release_does_not_reopen_failed_lazy_connections(session_manager):
    psycopg.connect.side_effect = psycopg.OperationalError("connection failed")
    connection = session_manager.set_connection_name("connection_failure")
    with pytest.raises(FailedToConnectError, match="connection failed"):
        connection.handle
    assert connection.state == ConnectionState.FAIL
    # dbt can install a new lazy handle before a subsequent model fails to compile.
    connection = session_manager.set_connection_name("compilation_failure")
    session_manager.release()
    session_manager.cleanup_all()
    assert connection.state == ConnectionState.FAIL
    assert session_manager.thread_connections == {}
    psycopg.connect.assert_called_once()


def test_worker_threads_reuse_distinct_sessions_until_cleanup(session_manager):
    ready = Barrier(2)

    def run_models(worker):
        handles = []
        for model in range(3):
            connection = session_manager.set_connection_name(
                f"worker_{worker}_model_{model}"
            )
            handles.append(connection.handle)
            if model == 0:
                ready.wait(timeout=10)
            session_manager.release()
        assert all(handle is handles[0] for handle in handles)
        return handles[0]

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run_models, worker) for worker in range(2)]
        handles = [future.result(timeout=15) for future in futures]
    assert handles[0] is not handles[1]
    assert all(not handle.closed for handle in handles)
    assert psycopg.connect.call_count == 2
    assert session_manager._attach_databases.call_count == 2
    session_manager.cleanup_all()
    assert all(handle.closed for handle in handles)
    assert session_manager.thread_connections == {}
