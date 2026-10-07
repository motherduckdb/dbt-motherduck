from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest
from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.events.types import SQLQuery, SQLQueryStatus
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError

from dbt.adapters.motherduck.connections import MotherDuckConnectionManager


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
