# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_simple_snapshot.py.
import pytest
from dbt.tests.adapter.simple_snapshot.test_snapshot import (
    BaseSimpleSnapshot,
    BaseSnapshotCheck,
)
from dbt.tests.util import relation_from_name, run_dbt


class TestSimpleSnapshotDuckDB(BaseSimpleSnapshot):
    pass


class TestSnapshotCheckDuckDB(BaseSnapshotCheck):
    pass


class TestSnapshotRollback:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "tracked.sql": """
{% snapshot tracked %}
{{ config(unique_key='id', strategy='check', check_cols=['value'],
          pre_hook='insert into ' ~ this.schema ~ '.hook_log values (1)',
          post_hook="select error('intentional snapshot failure')" if var('fail', false) else []) }}
select 1 as id, {{ var('value', 1) }} as value
{% endsnapshot %}
"""
        }

    def test_snapshot_failure_rolls_back_hooks_and_cleans_staging(self, project):
        log = relation_from_name(project.adapter, "hook_log")
        project.run_sql(f"CREATE TABLE {log} (value INTEGER)")
        run_dbt(["snapshot"])
        result = run_dbt(
            ["snapshot", "--vars", "{value: 2, fail: true}"], expect_pass=False
        )
        assert "intentional snapshot failure" in result.results[0].message
        assert project.run_sql(f"SELECT count(*) FROM {log}", fetch="one") == (1,)
        target = relation_from_name(project.adapter, "tracked")
        assert project.run_sql(f"SELECT value FROM {target}", fetch="all") == [(1,)]
        assert (
            project.run_sql(
                "SELECT table_name FROM duckdb_tables() "
                f"WHERE database_name = '{project.database}' AND schema_name = 'dbt_temp' "
                "AND table_name LIKE 'tracked__%'",
                fetch="all",
            )
            == []
        )
        run_dbt(["snapshot", "--vars", "{value: 2}"])
        assert project.run_sql(
            f"SELECT value FROM {target} WHERE dbt_valid_to IS NULL", fetch="all"
        ) == [(2,)]
