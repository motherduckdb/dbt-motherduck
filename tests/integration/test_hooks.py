# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_hooks.py.
import uuid

import pytest
from dbt.tests.util import relation_from_name, run_dbt

basic_model_sql = """
select range from range(3)
"""

test_table = f"test_table_{str(uuid.uuid1()).replace('-', '_')}"

post_hook_sql = f"create table {test_table} as select 1;"


class TestPostHook:
    """
    Post hook should run inside txn
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
            "models": {"post-hook": [{"sql": post_hook_sql}]},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_model.sql": basic_model_sql,
        }

    def test_run(self, project):
        run_dbt(["run"])

        # check that the model was run
        relation = relation_from_name(project.adapter, "basic_model")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one"
        )
        assert result[0] == 3

        # check that the post hook was run
        result = project.run_sql(
            f"select count(*) as num_rows from {test_table}", fetch="one"
        )
        assert result[0] == 1

        # reset
        project.run_sql(f"drop table {test_table}")


class TestPostHookTransactionFalse(TestPostHook):
    """
    Post hook should run outside txn
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
            "models": {"post-hook": [{"sql": post_hook_sql, "transaction": False}]},
        }


class TestIncrementalHookRollback:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hooked.sql": """
{{ config(materialized='incremental',
    pre_hook=[{'sql': 'insert into ' ~ this.schema ~ '.hook_log values (1)', 'transaction': false},
              'insert into ' ~ this.schema ~ '.hook_log values (2)'],
    post_hook="select error('intentional hook failure')" if var('fail', false) else []) }}
select 1 as id
"""
        }

    def test_outside_hook_survives_model_failure_without_cache(self, project):
        log = relation_from_name(project.adapter, "hook_log")
        project.run_sql(f"create table {log} (value integer)")
        run_dbt(["run"])
        project.run_sql(f"delete from {log}")
        results = run_dbt(
            ["run", "--no-populate-cache", "--vars", "{fail: true}"],
            expect_pass=False,
        )
        assert "intentional hook failure" in results.results[0].message
        assert project.run_sql(f"select value from {log}", fetch="all") == [(1,)]
        target = relation_from_name(project.adapter, "hooked")
        assert project.run_sql(f"select * from {target}", fetch="all") == [(1,)]


class TestHooks:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hooked.sql": """
{{ config(materialized='table',
    persist_docs={'relation': true, 'columns': true},
    pre_hook=[{'sql': 'create table if not exists ' ~ this.schema ~ '.hook_log (value integer)', 'transaction': false},
              'insert into ' ~ this.schema ~ '.hook_log values (1)'],
    post_hook=['insert into ' ~ this.schema ~ '.hook_log values (2)',
               {'sql': 'insert into ' ~ this.schema ~ '.hook_log values (3)', 'transaction': false}]) }}
select 1 as id
""",
            "schema.yml": """
version: 2
models:
  - name: hooked
    description: "Table's comment; with punctuation"
    columns:
      - name: id
        description: "Column's comment; with punctuation"
""",
        }

    def test_hook_transactions(self, project):
        run_dbt(["run"])
        target = relation_from_name(project.adapter, "hook_log")
        assert project.run_sql(
            f"SELECT value FROM {target} ORDER BY value", fetch="all"
        ) == [(1,), (2,), (3,)]
        assert project.run_sql(
            "SELECT comment FROM duckdb_tables() "
            f"WHERE database_name = '{project.database}' AND schema_name = '{project.test_schema}' "
            "AND table_name = 'hooked'",
            fetch="one",
        ) == ("Table's comment; with punctuation",)
        assert project.run_sql(
            "SELECT comment FROM duckdb_columns() "
            f"WHERE database_name = '{project.database}' AND schema_name = '{project.test_schema}' "
            "AND table_name = 'hooked' AND column_name = 'id'",
            fetch="one",
        ) == ("Column's comment; with punctuation",)
