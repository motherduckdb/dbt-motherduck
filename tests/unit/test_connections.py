from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dbt.adapters.contracts.connection import ConnectionState
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.motherduck.connections import MotherDuckConnectionManager


@pytest.fixture
def manager():
    handle = Mock()
    handle.cursors = []
    handle.queries = []

    def new_cursor():
        cursor = Mock(
            pgresult=SimpleNamespace(command_tuples=1), statusmessage="SELECT 1"
        )

        def execute(statement, bindings=None, *, prepare=None):
            handle.queries.append((statement, bindings, prepare, False))

        cursor.execute.side_effect = execute
        handle.cursors.append(cursor)
        return cursor

    handle.cursor.side_effect = new_cursor
    connection = SimpleNamespace(
        name="test",
        handle=handle,
        transaction_open=True,
        state=ConnectionState.OPEN,
        credentials=SimpleNamespace(token="secret-token"),
    )
    manager = object.__new__(MotherDuckConnectionManager)
    manager.get_thread_connection = lambda: connection
    return manager


@pytest.mark.parametrize("transaction_open", [False])
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
