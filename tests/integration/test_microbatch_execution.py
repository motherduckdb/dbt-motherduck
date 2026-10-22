import pytest
from dbt.tests.util import relation_from_name, run_dbt


class TestMicrobatchExecution:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "events.sql": """
{{ config(materialized='table', event_time='event_time') }}
select i as id, timestamp '2025-01-01' + i * interval '1 day' as event_time
from range(5) t(i)
""",
            "batched.sql": """
{{ config(materialized='incremental', incremental_strategy='microbatch',
          event_time='event_time', batch_size='day', begin='2025-01-01',
          concurrent_batches=var('concurrent', false),
          post_hook="select error('intentional batch failure')"
          if var('fail', false)
          else []) }}
select id, event_time from {{ ref('events') }}
""",
        }

    @pytest.mark.parametrize(
        "concurrent",
        [
            False,
            pytest.param(
                True,
                marks=pytest.mark.skip_database_type(
                    "ducklake",
                    reason="Concurrent batch writes conflict on DuckLake inlined data",
                ),
            ),
        ],
    )
    def test_bounded_batches_and_replacement(self, project, concurrent):
        variables = f"{{concurrent: {str(concurrent).lower()}}}"
        bounds = ["--event-time-start", "2025-01-01", "--event-time-end", "2025-01-05"]
        result = run_dbt(["run", "--full-refresh", "--vars", variables, *bounds])
        batch = next(r for r in result.results if r.node.name == "batched")
        assert len(batch.batch_results.successful) == 4
        target = relation_from_name(project.adapter, "batched")
        source = relation_from_name(project.adapter, "events")
        assert project.run_sql(f"SELECT id FROM {target} ORDER BY id", fetch="all") == [
            (0,),
            (1,),
            (2,),
            (3,),
        ]
        project.run_sql(f"UPDATE {source} SET id = 10 WHERE id = 1")
        events = []
        result = run_dbt(
            [
                "run",
                "--select",
                "batched",
                "--threads",
                "4",
                "--vars",
                variables,
                *bounds,
            ],
            callbacks=[events.append],
        )
        active = peak = 0
        for event in events:
            if event.info.name == "LogStartBatch":
                active += 1
                peak = max(peak, active)
            elif event.info.name == "LogBatchResult":
                active -= 1
        assert active == 0
        assert peak > 1 if concurrent else peak == 1
        assert len(result.results[0].batch_results.successful) == 4
        assert project.run_sql(f"SELECT id FROM {target} ORDER BY id", fetch="all") == [
            (0,),
            (2,),
            (3,),
            (10,),
        ]
        assert _staging_tables(project) == []

    def test_failed_batch_and_rerun(self, project):
        bounds = ["--event-time-start", "2025-01-01", "--event-time-end", "2025-01-05"]
        run_dbt(["run", "--full-refresh", *bounds])
        result = run_dbt(
            ["run", "--select", "batched", "--vars", "{fail: true}", *bounds],
            expect_pass=False,
        )
        assert len(result.results[0].batch_results.failed) == 1
        assert _staging_tables(project) == []
        result = run_dbt(["run", "--select", "batched", *bounds])
        assert len(result.results[0].batch_results.successful) == 4
        target = relation_from_name(project.adapter, "batched")
        assert project.run_sql(f"SELECT id FROM {target} ORDER BY id", fetch="all") == [
            (0,),
            (1,),
            (2,),
            (3,),
        ]


def _staging_tables(project):
    # dbt_temp is shared by every test in the database; other tests (e.g. cancellation)
    # can leave their own staging tables behind, so only look at this model's.
    prefix = relation_from_name(project.adapter, "batched").identifier + "__"
    return project.run_sql(
        "SELECT table_name FROM duckdb_tables() "
        f"WHERE database_name = '{project.database}' AND schema_name = 'dbt_temp' "
        f"AND starts_with(table_name, '{prefix}')",
        fetch="all",
    )
