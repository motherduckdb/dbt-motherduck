# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_incremental.py.
import pytest
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)
from dbt.tests.util import relation_from_name, run_dbt


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    pass


class TestSchemaChangeRollback:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "changing.sql": """
{{ config(materialized='incremental', unique_key='id', on_schema_change='sync_all_columns',
          post_hook="select error('intentional schema failure')" if var('fail', false) else []) }}
select 1 as id
{% if not var('fail', false) %}, 'preserved' as original{% else %}, 'new' as replacement{% endif %}
"""
        }

    def test_schema_and_data_roll_back(self, project):
        run_dbt(["run"])
        result = run_dbt(["run", "--vars", "{fail: true}"], expect_pass=False)
        assert "intentional schema failure" in result.results[0].message
        target = relation_from_name(project.adapter, "changing")
        assert project.run_sql(f"SELECT * FROM {target}", fetch="all") == [
            (1, "preserved")
        ]
        assert [r[0] for r in project.run_sql(f"DESCRIBE {target}", fetch="all")] == [
            "id",
            "original",
        ]
