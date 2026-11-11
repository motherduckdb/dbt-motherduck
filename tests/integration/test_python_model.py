"""Python models execute on MotherDuck Flights.

The base dbt cases that use a DuckDB relation (ref, limit, filter, empty, sample)
run here. Spark-only helpers such as session.createDataFrame do not. Accounts
without Flights skip these tests.
"""

import pytest
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonEmptyTests,
    BasePythonIncrementalTests,
    BasePythonModelTests,
    BasePythonSampleTests,
    m_1,
)
from dbt.tests.util import run_dbt


@pytest.fixture(scope="class", autouse=True)
def _require_flights(project):
    try:
        project.run_sql('select 1 from MD_LIST_FLIGHTS("limit" := 1)', fetch="all")
    except Exception as err:
        pytest.skip(f"MotherDuck Flights are not available on this account: {err}")


class TestPythonModel(BasePythonModelTests):
    pass


class TestPythonEmpty(BasePythonEmptyTests):
    pass


class TestPythonSample(BasePythonSampleTests):
    pass


incremental_python = """
def model(dbt, session):
    dbt.config(materialized="incremental", unique_key="id")
    df = dbt.ref("m_1")
    if dbt.is_incremental:
        df = df.filter("id > 5")
    return df
"""


class TestPythonIncremental(BasePythonIncrementalTests):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "delete+insert"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"m_1.sql": m_1, "incremental.py": incremental_python}


upstream_sql = """
{{ config(materialized='table') }}
select i as id, i % 3 as grp from generate_series(1, 100) g(i)
"""

flight_python_model = """
def model(dbt, session):
    dbt.config(materialized="table")
    df = dbt.ref("upstream_model")
    return df.filter("id <= 3")
"""

failing_python_model = """
def model(dbt, session):
    dbt.config(materialized="table")
    raise ValueError("python model blew up")
"""


class TestMotherDuckFlightBehavior:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream_model.sql": upstream_sql,
            "flight_model.py": flight_python_model,
        }

    def test_flight_result_is_visible_to_the_worker(self, project):
        # The Flight commits from another session. This worker must see that table.
        run_dbt(["run"])
        model = f"{project.database}.{project.test_schema}.flight_model"
        rows = project.run_sql(f"select id from {model} order by id", fetch="all")
        assert [row[0] for row in rows] == [1, 2, 3]

    def test_rerun_reuses_the_flight(self, project):
        run_dbt(["run"])
        (flight_id,) = project.run_sql(
            'select flight_id from MD_LIST_FLIGHTS("limit" := 200, owner_only := true) '
            "where flight_name like '%-flight_model'",
            fetch="one",
        )
        version_sql = (
            f"select current_version from MD_GET_FLIGHT(flight_id := '{flight_id}')"
        )
        (before,) = project.run_sql(version_sql, fetch="one")
        run_dbt(["run"])
        (after,) = project.run_sql(version_sql, fetch="one")
        assert after == before


class TestMotherDuckFlightError:
    @pytest.fixture(scope="class")
    def models(self):
        return {"broken.py": failing_python_model}

    def test_flight_failure_is_raised(self, project):
        result = run_dbt(["run"], expect_pass=False)
        message = result.results[0].message
        assert "status FAILED" in message
        assert "Logs:" in message
