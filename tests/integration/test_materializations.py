from pathlib import Path

import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture


class TestSmokeProject:
    @pytest.fixture(scope="class")
    def models(self):
        directory = Path(__file__).parent / "projects" / "smoke" / "models"
        return {path.name: path.read_text() for path in directory.iterdir()}

    @pytest.fixture(scope="class")
    def seeds(self):
        directory = Path(__file__).parent / "projects" / "smoke" / "seeds"
        return {path.name: path.read_text() for path in directory.iterdir()}

    def test_build_replace_and_full_refresh(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        relation = f'"{project.database}"."{project.test_schema}"'
        assert project.run_sql(
            f"SELECT id FROM {relation}.as_incremental ORDER BY id", fetch="all"
        ) == [(1,), (2,)]
        run_dbt(["run", "--vars", "{max_id: 3}"])
        for name in ("input", "as_view", "as_table", "as_incremental"):
            assert project.run_sql(
                f"SELECT id FROM {relation}.{name} ORDER BY id", fetch="all"
            ) == [(1,), (2,), (3,)]
        run_dbt(["test"])
        run_dbt(["run", "--full-refresh"])
        assert project.run_sql(
            f"SELECT id FROM {relation}.as_incremental ORDER BY id", fetch="all"
        ) == [(1,), (2,)]
        with project.adapter.connection_named("relation_types"):
            for name, kind in (
                ("as_view", "view"),
                ("as_table", "table"),
                ("as_incremental", "table"),
            ):
                found = project.adapter.get_relation(
                    project.database, project.test_schema, name
                )
                assert found.type == kind


class TestSeedBindings:
    @pytest.fixture(scope="class")
    def seeds(self):
        rows = ["id,label"]
        for i in range(1, 502):
            value = {1: "O'Reilly", 500: "日本語 50%", 501: ""}.get(i, "ordinary")
            rows.append(f"{i},{value}")
        return {"bound.csv": "\n".join(rows) + "\n"}

    def test_batch_boundary_and_values(self, project):
        relation = f'"{project.database}"."{project.test_schema}".bound'
        for args in (["seed"], ["seed"], ["seed", "--full-refresh"]):
            run_dbt(args)
            assert project.run_sql(f"SELECT count(*) FROM {relation}", fetch="one") == (
                501,
            )
            assert project.run_sql(
                f"SELECT id, label FROM {relation} WHERE id IN (1, 500, 501) ORDER BY id",
                fetch="all",
            ) == [(1, "O'Reilly"), (500, "日本語 50%"), (501, None)]


class TestIncrementalStrategies:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "changes.sql": """
{{ config(materialized='incremental', unique_key='id',
          incremental_strategy=var('strategy'),
          on_schema_change='fail',
          meta={'temp_schema_name': var('staging_schema', 'dbt_temp')},
          post_hook="select error('intentional model failure')" if var('fail', false) else []) }}
select 1 as id, {{ var('value', 1) }} as value
{% if var('extra_column', false) %}, 1 as extra{% endif %}
"""
        }

    @pytest.mark.parametrize(
        "strategy, staging_schema",
        [
            ("append", "dbt_temp"),
            ("delete+insert", "custom_staging"),
            ("merge", "merge_staging"),
        ],
    )
    def test_updates_and_failure_cleanup(self, project, strategy, staging_schema):
        relation = f'"{project.database}"."{project.test_schema}".changes'
        variables = f"strategy: '{strategy}', staging_schema: {staging_schema}"
        staging_sql = (
            "SELECT table_name FROM duckdb_tables() "
            f"WHERE database_name = '{project.database}' "
            f"AND schema_name = '{staging_schema}' "
            "AND table_name LIKE 'changes__%'"
        )
        run_dbt(["run", "--full-refresh", "--vars", f"{{{variables}}}"])
        run_dbt(["run", "--vars", f"{{{variables}, value: 2}}"])
        assert project.run_sql(staging_sql, fetch="all") == []
        assert project.run_sql(
            "SELECT schema_name FROM duckdb_schemas() "
            f"WHERE database_name = '{project.database}' AND schema_name = '{staging_schema}'",
            fetch="all",
        ) == [(staging_schema,)]
        expected = [(1, 1), (1, 2)] if strategy == "append" else [(1, 2)]
        assert (
            project.run_sql(f"SELECT * FROM {relation} ORDER BY value", fetch="all")
            == expected
        )
        result = run_dbt(
            ["run", "--vars", f"{{{variables}, value: 3, fail: true}}"],
            expect_pass=False,
        )
        assert "intentional model failure" in result.results[0].message
        assert (
            project.run_sql(f"SELECT * FROM {relation} ORDER BY value", fetch="all")
            == expected
        )
        assert project.run_sql(staging_sql, fetch="all") == []
        run_dbt(["run", "--full-refresh", "--vars", f"{{{variables}, value: 4}}"])
        assert project.run_sql(f"SELECT * FROM {relation}", fetch="all") == [(1, 4)]

    def test_cleanup_after_schema_change_error(self, project):
        run_dbt(["run", "--full-refresh", "--vars", "{strategy: append}"])
        result = run_dbt(
            ["run", "--vars", "{strategy: append, extra_column: true}"],
            expect_pass=False,
        )
        assert (
            "schemas on this incremental model are out of sync"
            in result.results[0].message
        )
        assert (
            project.run_sql(
                "SELECT table_name FROM duckdb_tables() "
                f"WHERE database_name = '{project.database}' "
                "AND schema_name = 'dbt_temp' AND table_name LIKE 'changes__%'",
                fetch="all",
            )
            == []
        )
        assert project.run_sql(
            f'SELECT * FROM "{project.database}"."{project.test_schema}".changes',
            fetch="all",
        ) == [(1, 1)]


class TestStagingCleanupFailures:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cleanup_probe.sql": """
{{ config(materialized='incremental', unique_key='id', incremental_strategy='merge',
          post_hook=[{'sql': '{{ create_staging_view() }}', 'transaction': false}]
                    if var('break_cleanup', false) else []) }}
select id: 1, value: {{ var('value', 1) }}
"""
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "create_staging_view.sql": """
{% macro create_staging_view() %}
  {% set staging = this.incorporate(path=adapter.get_temp_relation_path(this)) %}
  {# Run after commit and normal cleanup: DROP TABLE must fail on this view. #}
  create view {{ staging }} as select id: 1;
  {% if var('fail', false) %}
    select error('original post-commit hook failure');
  {% endif %}
{% endmacro %}
"""
        }

    @pytest.mark.parametrize("model_fails", [False, True])
    def test_cleanup_preserves_model_outcome(self, project, model_fails):
        run_dbt(["run", "--full-refresh"])
        results, logs = run_dbt_and_capture(
            [
                "run",
                "--threads",
                "1",
                "--vars",
                f"{{value: 2, break_cleanup: true, fail: {str(model_fails).lower()}}}",
            ],
            expect_pass=not model_fails,
        )

        assert "Could not clean up temporary table" in logs
        assert project.database in logs
        if model_fails:
            assert "original post-commit hook failure" in results.results[0].message
            assert "Could not clean up temporary table" not in results.results[0].message
        target = f'"{project.database}"."{project.test_schema}".cleanup_probe'
        # Both cases commit before the hook that creates the cleanup failure.
        assert project.run_sql(f"select value from {target}", fetch="all") == [(2,)]
        run_dbt(["run", "--vars", "{value: 3}"])
        assert project.run_sql(f"select value from {target}", fetch="all") == [(3,)]
