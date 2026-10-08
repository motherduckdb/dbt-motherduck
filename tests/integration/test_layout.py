# Adapted from dbt-duckdb's DuckLake partitioning and sorting tests.
import json

import pytest
from dbt.tests.util import relation_from_name, run_dbt


class TestLayout:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "layout.sql": """
{{ config(materialized=var('kind', 'table'), meta=var('layout', {}),
          unique_key='id', contract={'enforced': var('contract', false)}) }}
select id: 1, ds: 2, region: 3, ts: timestamp '2025-01-01', "field name": 4
""",
            "schema.yml": """
version: 2
models:
  - name: layout
    columns:
      - {name: id, data_type: integer}
      - {name: ds, data_type: integer}
      - {name: region, data_type: integer}
      - {name: ts, data_type: timestamp}
      - {name: field name, data_type: integer, quote: true}
""",
        }

    @pytest.mark.parametrize(
        "kind, options, contract",
        [
            ("table", {"partitioned_by": "ds", "sorted_by": "region"}, False),
            ("incremental", {"partitioned_by": "ds", "sorted_by": "region"}, False),
            (
                "table",
                {"partition_by": ["ds", "region"], "sort_by": ["ds", "region"]},
                False,
            ),
            (
                "table",
                {"partitioned_by": ["day(ts)", "bucket(16, id)", '"field name"']},
                False,
            ),
            ("table", {"partitioned_by": "ds", "sorted_by": "region"}, True),
        ],
    )
    def test_build_and_replace(self, project, database_type, kind, options, contract):
        relation = relation_from_name(project.adapter, "layout")
        project.run_sql(f"drop table if exists {relation}")
        flags_by_run = [[], []] if kind == "table" else [[], [], ["--full-refresh"]]
        for iteration, flags in enumerate(flags_by_run):
            events = []
            run_dbt(
                [
                    "run",
                    "--vars",
                    json.dumps({"kind": kind, "layout": options, "contract": contract}),
                    *flags,
                ],
                callbacks=[events.append],
            )
            # Managed DuckLake does not expose layout metadata. Check only layout
            # clauses sent during a real build, not the whole materialization SQL.
            statements = [
                event.data.sql.lower()
                for event in events
                if event.info.name == "SQLQuery"
            ]
            sql = " ".join(statements)
            if database_type == "native":
                assert "set partitioned by" not in sql
                assert "set sorted by" not in sql
            elif kind == "table" or iteration == 0 or flags:
                partition = options.get("partitioned_by", options.get("partition_by"))
                if partition:
                    expressions = (
                        [partition] if isinstance(partition, str) else partition
                    )
                    expected = "set partitioned by (" + ", ".join(expressions) + ")"
                    assert any(
                        "".join(expected.split()) in "".join(statement.split())
                        for statement in statements
                    )
                sorting = options.get("sorted_by", options.get("sort_by"))
                if sorting:
                    columns = [sorting] if isinstance(sorting, str) else sorting
                    expected = "set sorted by (" + ", ".join(columns) + ")"
                    assert any(
                        "".join(expected.split())
                        in "".join(statement.replace('"', "").split())
                        for statement in statements
                    )
            relation = relation_from_name(project.adapter, "layout")
            assert project.run_sql(
                f'select id, ds, region, "field name" from {relation}', fetch="all"
            ) == [(1, 2, 3, 4)]

    @pytest.mark.parametrize(
        "key, value, message",
        [
            (key, value, message)
            for key in ("partitioned_by", "sorted_by")
            for value, message in [
                (["ds", 1], "list values must be non-empty strings"),
                ([], "must contain at least one column"),
            ]
        ],
    )
    def test_invalid_layout(self, project, key, value, message):
        result = run_dbt(
            ["run", "--vars", json.dumps({"layout": {key: value}})], expect_pass=False
        )
        assert message in result.results[0].message

    @pytest.mark.skip_database_type(
        "native", reason="Sorting is ignored for native databases"
    )
    def test_sort_column_is_not_executed_as_sql(self, project):
        sentinel = relation_from_name(project.adapter, "sort_sentinel")
        project.run_sql(f"create table {sentinel} as select id: 42")
        name = f"ds); drop table {sentinel}; --"
        result = run_dbt(
            ["run", "--vars", json.dumps({"layout": {"sorted_by": name}})],
            expect_pass=False,
        )
        assert name in result.results[0].message
        assert project.run_sql(f"select * from {sentinel}", fetch="all") == [(42,)]


@pytest.mark.skip_database_type(
    "native", reason="Partitioning/sorting requires DuckLake"
)
class TestLayoutRollback:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "layout.sql": """
{{ config(materialized=var('kind'), unique_key='id',
          meta={'partitioned_by': 'day(ts)', 'sorted_by': ['id']}) }}
select id: case when {{ 'true' if var('fail', false) else 'false' }} and i = 1
            then error('intentional layout failure') else i end,
       ts: timestamp '2025-01-01'
from range(1, 2) t(i)
"""
        }

    @pytest.mark.parametrize("kind", ["table", "incremental"])
    def test_failed_build_rolls_back_and_recovers(self, project, kind):
        target = relation_from_name(project.adapter, "layout")
        project.run_sql(f"drop table if exists {target}")
        result = run_dbt(
            ["run", "--vars", f"{{kind: {kind}, fail: true}}"], expect_pass=False
        )
        assert "intentional layout failure" in result.results[0].message
        with project.adapter.connection_named("inspect_failed_layout"):
            assert (
                project.adapter.get_relation(
                    database=project.database,
                    schema=project.test_schema,
                    identifier="layout",
                )
                is None
            )
        run_dbt(["run", "--vars", f"{{kind: {kind}}}"])
        target = relation_from_name(project.adapter, "layout")
        assert project.run_sql(f"select id from {target}", fetch="all") == [(1,)]
