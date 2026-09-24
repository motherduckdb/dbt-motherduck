import pytest
from psycopg import sql

from dbt.adapters.motherduck.impl import MotherDuckAdapter


def test_registered_adapter_and_database(project, database_type):
    assert isinstance(project.adapter, MotherDuckAdapter)
    with project.adapter.connection_named("setup"):
        relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        assert project.adapter.is_ducklake(relation) == (database_type == "ducklake")
        _, table = project.adapter.execute(
            "SELECT current_database() AS name, is_ducklake FROM md_attached_databases() "
            "WHERE alias = current_database()",
            fetch=True,
        )
        assert table.rows[0]["name"] == project.database
        assert table.rows[0]["is_ducklake"] == (database_type == "ducklake")


@pytest.mark.parametrize("fail", [False, True])
def test_database_cleanup(temporary_database, admin_connection, fail):
    try:
        with temporary_database() as name:
            admin_connection.execute(
                sql.SQL("CREATE TABLE {}.main.cleanup_probe AS SELECT 1 AS id").format(
                    sql.Identifier(name)
                )
            )
            if fail:
                raise RuntimeError("intentional test-body failure")
    except RuntimeError as exc:
        assert fail and str(exc) == "intentional test-body failure"
    assert (
        admin_connection.execute(
            "SELECT count(*) FROM md_all_databases() WHERE alias = %s", (name,)
        ).fetchone()[0]
        == 0
    )


def test_missing_relation_columns(project):
    relation = project.adapter.Relation.create(
        database=project.database,
        schema=project.test_schema,
        identifier="does_not_exist",
    )
    with project.adapter.connection_named("missing_columns"):
        project.adapter.execute("SELECT 1", auto_begin=True)
        handle = project.adapter.connections.get_thread_connection().handle
        assert project.adapter.get_columns_in_relation(relation) == []
        assert not handle.closed
        assert project.adapter.connections.get_thread_connection().transaction_open
        _, table = project.adapter.execute("SELECT 2 AS answer", fetch=True)
        assert table.rows[0][0] == 2
