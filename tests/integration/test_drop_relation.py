# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_drop_relation.py.
import pytest
from dbt.tests.util import get_connection, relation_from_name, run_dbt
from psycopg import Error


class TestDropRelation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"table_model.sql": "{{ config(materialized='table') }} select 1 as id"}

    @pytest.mark.parametrize(
        "dependency",
        [
            None,
            pytest.param(
                "view",
                marks=pytest.mark.skip_database_type(
                    "ducklake",
                    reason="DuckLake does not support relation-level cascade drops",
                ),
            ),
            pytest.param(
                "macro",
                marks=pytest.mark.skip_database_type(
                    "ducklake",
                    reason="DuckLake does not support relation-level cascade drops",
                ),
            ),
        ],
    )
    def test_drop_table(self, project, dependency):
        run_dbt(["run"])
        table = relation_from_name(project.adapter, "table_model")
        dependent = relation_from_name(project.adapter, "dependent")
        with get_connection(project.adapter):
            if dependency:
                connection = project.adapter.connections.get_thread_connection()
                connection.handle.execute(
                    f"set enable_{dependency}_dependencies = true"
                )
                if dependency == "view":
                    connection.handle.execute(
                        f"create view {dependent} as select * from {table}"
                    )
                else:
                    connection.handle.execute(
                        f"create macro {dependent}() as table select * from {table}"
                    )
                with pytest.raises(Error, match="depends on"):
                    connection.handle.execute(f"drop table {table}")
            relation = project.adapter.get_relation(
                database=table.database,
                schema=table.schema,
                identifier=table.identifier,
            )
            assert relation is not None
            project.adapter.drop_relation(relation)
            assert (
                project.adapter.get_relation(
                    database=table.database,
                    schema=table.schema,
                    identifier=table.identifier,
                )
                is None
            )
        assert project.run_sql(
            "select count(*) from duckdb_tables() "
            f"where database_name = '{project.database}' "
            f"and schema_name = '{project.test_schema}' and table_name = 'table_model'",
            fetch="one",
        ) == (0,)
        if dependency == "view":
            assert project.run_sql(
                "select count(*) from duckdb_views() "
                f"where database_name = '{project.database}' "
                f"and schema_name = '{project.test_schema}' and view_name = 'dependent'",
                fetch="one",
            ) == (0,)
        elif dependency == "macro":
            assert project.run_sql(
                "select count(*) from duckdb_functions() "
                f"where database_name = '{project.database}' "
                f"and schema_name = '{project.test_schema}' and function_name = 'dependent'",
                fetch="one",
            ) == (0,)
