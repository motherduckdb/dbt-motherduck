# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_ducklake_{partitioned_by,sorted_by}{,_integration}.py.
import json

import pytest
from dbt.tests.util import relation_from_name, run_dbt


class TestLayout:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "layout.sql": """
{{ config(materialized=var('kind', 'view'), meta=var('layout', {}),
          unique_key='id', contract={'enforced': var('contract', false)}) }}
{% if execute and var('render', false) %}
  {% set partitioned = motherduck__get_partitioned_by(this, false) %}
  {% set sorted = motherduck__get_sorted_by(this, false) %}
  {{ motherduck__create_table_as(false, this, 'select 1 as id, 2 as ds, 3 as region',
       partitioned_by=partitioned, sorted_by=sorted) }}
{% else %}
select 1 as id, 2 as ds, 3 as region
{% endif %}
""",
            "schema.yml": """
version: 2
models:
  - name: layout
    columns:
      - {name: id, data_type: integer}
      - {name: ds, data_type: integer}
      - {name: region, data_type: integer}
""",
        }

    @pytest.mark.parametrize(
        "options, expected",
        [
            ({"partitioned_by": "ds"}, "set partitioned by (ds)"),
            ({"partition_by": ["ds", "region"]}, "set partitioned by (ds, region)"),
            (
                {
                    "partitioned_by": [
                        "event_day",
                        "event_month",
                        "event_year",
                        "event_hour",
                    ]
                },
                "set partitioned by (event_day, event_month, event_year, event_hour)",
            ),
            ({"partitioned_by": '"field name"'}, 'set partitioned by ("field name")'),
            (
                {"partitioned_by": ["day(ts)", '"region"']},
                'set partitioned by (day(ts), "region")',
            ),
            (
                {"partitioned_by": ["bucket(16, user_id)"]},
                "set partitioned by (bucket(16, user_id))",
            ),
            ({"sorted_by": "ds"}, 'set sorted by ("ds")'),
            ({"sort_by": ["ds", "region"]}, 'set sorted by ("ds", "region")'),
        ],
    )
    def test_generated_layout(self, project, database_type, options, expected):
        results = run_dbt(
            ["compile", "--vars", json.dumps({"layout": options, "render": True})]
        )
        sql = " ".join(results.results[0].node.compiled_code.lower().split())
        if database_type == "ducklake":
            assert expected in sql
        else:
            assert "set partitioned by" not in sql
            assert "set sorted by" not in sql

    @pytest.mark.parametrize("key", ["partitioned_by", "sorted_by"])
    def test_layout_with_contract(self, project, database_type, key):
        results = run_dbt(
            [
                "compile",
                "--vars",
                json.dumps({"layout": {key: "ds"}, "render": True, "contract": True}),
            ]
        )
        sql = " ".join(results.results[0].node.compiled_code.lower().split())
        expected = (
            "set partitioned by (ds)"
            if key == "partitioned_by"
            else 'set sorted by ("ds")'
        )
        assert (expected in sql) == (database_type == "ducklake")

    @pytest.mark.parametrize("kind", ["table", "incremental"])
    @pytest.mark.parametrize(
        "options",
        [
            {"partitioned_by": "ds"},
            {"partition_by": ["ds", "region"]},
            {"sorted_by": "ds"},
            {"sort_by": ["ds", "region"]},
            {"partitioned_by": "ds", "sorted_by": "region"},
        ],
    )
    def test_build_replace_and_full_refresh(
        self, project, database_type, kind, options
    ):
        for flags in (["--full-refresh"], [], ["--full-refresh"]):
            events = []
            run_dbt(
                [
                    "run",
                    "--vars",
                    json.dumps({"kind": kind, "layout": options}),
                    *flags,
                ],
                callbacks=[events.append],
            )
            sql = "\n".join(
                event.data.sql.lower()
                for event in events
                if event.info.name == "SQLQuery"
            )
            if database_type == "native":
                assert "set partitioned by" not in sql
                assert "set sorted by" not in sql
            elif kind == "table" or flags:
                if "partitioned_by" in options or "partition_by" in options:
                    assert "set partitioned by" in sql
                if "sorted_by" in options or "sort_by" in options:
                    assert "set sorted by" in sql
                if "partitioned_by" in options and "sorted_by" in options:
                    assert sql.index("set partitioned by") < sql.index("set sorted by")
            relation = relation_from_name(project.adapter, "layout")
            assert project.run_sql(f"select * from {relation}", fetch="all") == [
                (1, 2, 3)
            ]

    @pytest.mark.parametrize(
        "key", ["partitioned_by", "partition_by", "sorted_by", "sort_by"]
    )
    @pytest.mark.parametrize(
        "value, message",
        [
            (["ds", 1], "list values must be non-empty strings"),
            ([], "must contain at least one column"),
        ],
    )
    def test_invalid_layout(self, project, key, value, message):
        result = run_dbt(
            ["run", "--vars", json.dumps({"kind": "table", "layout": {key: value}})],
            expect_pass=False,
        )
        assert message in result.results[0].message

    @pytest.mark.skip_database_type(
        "native", reason="Sorting is ignored for native databases"
    )
    def test_unknown_sort_column(self, project):
        result = run_dbt(
            [
                "run",
                "--vars",
                json.dumps(
                    {"kind": "table", "layout": {"sorted_by": "ds); drop table x; --"}}
                ),
            ],
            expect_pass=False,
        )
        assert "ds); drop table x; --" in result.results[0].message
        assert any(
            text in result.results[0].message.lower()
            for text in ("not found", "does not exist")
        )
