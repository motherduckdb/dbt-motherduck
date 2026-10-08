# Third-party package compatibility: godatadriven/dbt_date.
#
# dbt_date ships `duckdb__` implementations for eleven of its macros, and
# dispatch cannot reach them: the adapter type is `motherduck`, so the search
# prefixes are `motherduck__` then `default__` (dbt/context/providers.py,
# _get_adapter_macro_prefixes). The adapter ships no shims, so every call lands
# on `default__`, which is written for Snowflake.
#
# The xfails below are therefore not flakes. Each one names a macro where
# dbt_date would work on DuckDB today if it carried a `motherduck__` variant,
# and the expected value in each case is what the package's own `duckdb__`
# implementation produces. That makes this module the checklist for upstream
# contributions. `strict=True` is deliberate: if a release starts passing, the
# gap is closed and the entry should be deleted.
import pytest
from dbt.tests.util import run_dbt

# A Thursday, so weekday-sensitive macros have a distinguishable answer. The
# dbt_date macros take SQL as a string, so this has to reach Jinja quoted.
DATE = "\"cast('2026-10-08' as date)\""

MODEL = """
{{{{ config(materialized="table") }}}}
select cast({expression} as varchar) as value
"""

# name -> (jinja expression, expected value, xfail reason or None)
CASES = {
    "date_week_of_year": (
        f"{{{{ dbt_date.week_of_year({DATE}) }}}}",
        "41",
        None,
    ),
    "date_day_of_week": (
        f"{{{{ dbt_date.day_of_week({DATE}, isoweek=False) }}}}",
        "5",
        None,
    ),
    "date_day_of_week_iso": (
        f"{{{{ dbt_date.day_of_week({DATE}, isoweek=True) }}}}",
        "4",
        None,
    ),
    "date_day_of_year": (
        f"{{{{ dbt_date.day_of_year({DATE}) }}}}",
        "281",
        None,
    ),
    "date_from_unixtimestamp": (
        "cast(extract(epoch from {{ dbt_date.from_unixtimestamp(1700000000) }})"
        " as bigint)",
        "1700000000",
        None,
    ),
    "date_convert_timezone": (
        "{{ dbt_date.convert_timezone("
        "\"cast('2026-10-08 12:00:00' as timestamp)\","
        " 'America/New_York', 'UTC') }}",
        "2026-10-08 08:00:00",
        "default__convert_timezone emits convert_timezone(), which DuckDB does "
        "not have; duckdb__ delegates to the `at time zone` form",
    ),
    "date_day_name": (
        f"{{{{ dbt_date.day_name({DATE}, short=True) }}}}",
        "Thu",
        "default__day_name emits to_char(), which DuckDB does not have; "
        "duckdb__ uses dayname()",
    ),
    "date_month_name": (
        f"{{{{ dbt_date.month_name({DATE}, short=True) }}}}",
        "Oct",
        "default__month_name emits to_char(), which DuckDB does not have; "
        "duckdb__ uses monthname()",
    ),
    "date_iso_week_start": (
        f"{{{{ dbt_date.iso_week_start({DATE}) }}}}",
        "2026-10-05",
        "default__iso_week_start emits date_trunc('isoweek', ...); DuckDB does "
        "not recognise the isoweek specifier",
    ),
    "date_iso_week_of_year": (
        f"{{{{ dbt_date.iso_week_of_year({DATE}) }}}}",
        "41",
        "default__iso_week_of_year emits date_part('isoweek', ...); DuckDB does "
        "not recognise the isoweek specifier",
    ),
    "date_week_start": (
        f"{{{{ dbt_date.week_start({DATE}) }}}}",
        "2026-10-04",
        "silent wrong answer: default__week_start is Monday-based, while "
        "dbt_date's contract (and duckdb__) is Sunday-based",
    ),
    "date_week_end": (
        f"{{{{ dbt_date.week_end({DATE}) }}}}",
        "2026-10-10",
        "silent wrong answer: default__week_end follows the Monday-based "
        "week_start, so it lands a day late",
    ),
}


def case_params():
    params = []
    for name, (_, expected, reason) in CASES.items():
        marks = [pytest.mark.xfail(reason=reason, strict=True)] if reason else []
        params.append(pytest.param(name, expected, marks=marks, id=name))
    return params


class TestDbtDate:
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "godatadriven/dbt_date",
                    "version": [">=0.21.0", "<1.0.0"],
                }
            ]
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # dbt_date defaults this in its own dbt_project.yml; pinning it here
        # keeps convert_timezone's expected value independent of that default.
        return {"vars": {"dbt_date:time_zone": "America/New_York"}}

    @pytest.fixture(scope="class")
    def models(self):
        models = {
            f"{name}.sql": MODEL.format(expression=expression)
            for name, (expression, _, _) in CASES.items()
        }
        models["date_today.sql"] = MODEL.format(expression="{{ dbt_date.today() }}")
        return models

    @pytest.fixture(scope="class")
    def deps(self, project):
        run_dbt(["deps"])
        return project

    @pytest.mark.parametrize(("name", "expected"), case_params())
    def test_macro(self, deps, project, name, expected):
        run_dbt(["run", "--select", name])
        value = project.run_sql(
            f"select value from {project.test_schema}.{name}", fetch="one"
        )[0]
        assert value == expected

    @pytest.mark.xfail(
        reason="dbt_date.today() routes through now() -> convert_timezone(), so "
        "the convert_timezone gap also takes out today/tomorrow/yesterday/now "
        "and every macro called without an explicit date",
        strict=True,
    )
    def test_today_reaches_convert_timezone(self, deps, project):
        run_dbt(["run", "--select", "date_today"])
        value = project.run_sql(
            f"select value from {project.test_schema}.date_today", fetch="one"
        )[0]
        expected = project.run_sql("select cast(current_date as varchar)", fetch="one")[
            0
        ]
        assert value == expected
