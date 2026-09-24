import pytest
from psycopg import sql

from dbt.adapters.motherduck.impl import MotherDuckAdapter


def test_registered_adapter_and_database(project, database_type):
    assert isinstance(project.adapter, MotherDuckAdapter)
    with project.adapter.connection_named("setup"):
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
