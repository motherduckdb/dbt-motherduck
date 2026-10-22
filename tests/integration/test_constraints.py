# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_constraints.py; includes the MotherDuck-enabled cases.
import json

import pytest
from dbt.tests.adapter.constraints.test_constraints import (
    BaseIncrementalConstraintsColumnsEqual,
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
)
from dbt.tests.util import get_connection, run_dbt
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)
from dbt_common.exceptions import DbtDatabaseError
from psycopg import sql

pytestmark = pytest.mark.skip_database_type(
    "ducklake", reason="DuckLake does not support primary or unique constraints"
)


class DuckDBColumnEqualSetup:
    @pytest.fixture
    def int_type(self):
        return "INT"

    @pytest.fixture
    def string_type(self):
        return "VARCHAR"

    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["true", "bool", "BOOL"],
            ["'2013-11-03 00:00:00-07'::timestamp", "TIMESTAMP", "TIMESTAMP"],
            [
                "'2013-11-03 00:00:00-07'::timestamptz",
                "TIMESTAMPTZ",
                "TIMESTAMP WITH TIME ZONE",
            ],
            ["ARRAY['a','b','c']", "VARCHAR[]", "VARCHAR[]"],
            ["ARRAY[1,2,3]", "INTEGER[]", "INTEGER[]"],
            ["'1'::numeric", "numeric", "DECIMAL"],
            [
                """'{"bar": "baz", "balance": 7.77, "active": false}'::json""",
                "json",
                "JSON",
            ],
        ]


class TestTableConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseTableConstraintsColumnsEqual
):
    pass


class TestViewConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseViewConstraintsColumnsEqual
):
    pass


class TestIncrementalConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual
):
    pass


class TestQuotedForeignKeys:
    @pytest.fixture(scope="class")
    def models(self):
        parent = {
            "name": "parent",
            "columns": [
                {
                    "name": "id",
                    "data_type": "integer",
                    "constraints": [{"type": "primary_key"}],
                }
            ],
        }
        child_column = {
            "name": "child_column",
            "columns": [
                {
                    "name": "parent_id",
                    "data_type": "integer",
                    "constraints": [
                        {
                            "type": "foreign_key",
                            "to": "ref('parent')",
                            "to_columns": ["id"],
                        }
                    ],
                }
            ],
        }
        child_model = {
            "name": "child_model",
            "columns": [{"name": "parent_id", "data_type": "integer"}],
            "constraints": [
                {
                    "type": "foreign_key",
                    "columns": ["parent_id"],
                    "to": "ref('parent')",
                    "to_columns": ["id"],
                }
            ],
        }
        config = "{{ config(materialized='table', schema='schema.with.dot', contract={'enforced': true}) }}"
        return {
            "parent.sql": config + "{{ config(alias='parent.with.dot') }} select id: 1",
            "child_column.sql": config
            + "\n-- depends_on: {{ ref('parent') }}\nselect parent_id: 1",
            "child_model.sql": config
            + "\n-- depends_on: {{ ref('parent') }}\nselect parent_id: 1",
            "schema.yml": json.dumps(
                {"version": 2, "models": [parent, child_column, child_model]}
            ),
        }

    def test_quoted_parent_is_resolved_and_enforced(self, project):
        # two models being built concurrently, each creating a foreign key
        # referencing the same parent table can run into write-write conflicts
        results = run_dbt(["run", "--threads", "1"])
        assert len(results) == 3
        for result in results:
            if result.node.name == "parent":
                continue
            node = result.node
            relation = project.adapter.Relation.create(
                database=node.database, schema=node.schema, identifier=node.alias
            )
            assert project.run_sql(f"select * from {relation}", fetch="all") == [(1,)]
            with (
                get_connection(project.adapter),
                pytest.raises(DbtDatabaseError, match=r"(?i)foreign key|constraint"),
            ):
                project.adapter.execute(f"insert into {relation} values (999)")
            assert project.run_sql(f"select * from {relation}", fetch="all") == [(1,)]

    @pytest.mark.parametrize("level", ["column", "model"])
    def test_rendered_foreign_key_accepts_escaped_identifiers(self, project, level):
        # Exercise the public constraint renderer against real SQL. dbt's ref
        # renderer cannot currently escape embedded quotes in relation names.
        schema = sql.Identifier(project.database, 'sche"ma.with.dot').as_string()
        parent = sql.Identifier(
            project.database, 'sche"ma.with.dot', 'par"ent.with.dot'
        ).as_string()
        project.run_sql(f"create schema if not exists {schema}")
        project.run_sql(f"create table if not exists {parent} (id integer primary key)")
        project.run_sql(f"insert into {parent} values (1) on conflict do nothing")
        kwargs = {
            "type": ConstraintType.foreign_key,
            "to": parent,
            "to_columns": ["id"],
        }
        if level == "column":
            constraint = project.adapter.render_column_constraint(
                ColumnLevelConstraint(**kwargs)
            )
            columns = f"parent_id integer {constraint}"
        else:
            constraint = project.adapter.render_model_constraint(
                ModelLevelConstraint(columns=["parent_id"], **kwargs)
            )
            columns = f"parent_id integer, {constraint}"
        child = sql.Identifier(
            project.database, 'sche"ma.with.dot', f"escaped_child_{level}"
        ).as_string()
        project.run_sql(f"create table {child} ({columns})")
        project.run_sql(f"insert into {child} values (1)")
        with (
            get_connection(project.adapter),
            pytest.raises(DbtDatabaseError, match=r"(?i)foreign key|constraint"),
        ):
            project.adapter.execute(f"insert into {child} values (999)")
        assert project.run_sql(f"select * from {child}", fetch="all") == [(1,)]
