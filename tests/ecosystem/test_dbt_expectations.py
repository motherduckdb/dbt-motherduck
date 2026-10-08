# Third-party package compatibility: metaplane/dbt_expectations.
#
# dbt_expectations is the widest generic-test surface anyone is likely to point
# at the adapter. It also pulls godatadriven/dbt_date as a dependency, so some
# of its tests inherit dbt_date's dispatch gaps (see test_dbt_date.py).
#
# Rather than one assertion per test, this runs the whole suite once and
# compares the full name -> status map. A regression and an upstream fix both
# show up as a diff, which is what makes the module useful as a gap list.
import pytest
from dbt.tests.util import run_dbt

SUBJECT_CSV = """id,team,amount,email,measured_at
1,alpha,10,ana@example.com,2020-01-01
2,alpha,20,bo@example.com,2020-02-01
3,beta,30,cy@example.com,2020-03-01
4,beta,40,di@example.com,2020-04-01
"""

SCHEMA_YML = """
version: 2

seeds:
  - name: expectations_subject
    data_tests:
      - dbt_expectations.expect_table_row_count_to_equal:
          name: expectations_row_count
          arguments:
            value: 4
      - dbt_expectations.expect_table_column_count_to_equal:
          name: expectations_column_count
          arguments:
            value: 5
      - dbt_expectations.expect_compound_columns_to_be_unique:
          name: expectations_compound_unique
          arguments:
            column_list: ["id", "team"]
    columns:
      - name: id
        data_tests:
          - dbt_expectations.expect_column_values_to_be_unique:
              name: expectations_id_unique
          - dbt_expectations.expect_column_values_to_not_be_null:
              name: expectations_id_not_null
          - dbt_expectations.expect_column_values_to_be_between:
              name: expectations_id_between
              arguments:
                min_value: 1
                max_value: 4
      - name: team
        data_tests:
          - dbt_expectations.expect_column_values_to_be_in_set:
              name: expectations_team_in_set
              arguments:
                value_set: ["alpha", "beta"]
          - dbt_expectations.expect_column_distinct_count_to_equal:
              name: expectations_team_distinct_count
              arguments:
                value: 2
      - name: amount
        data_tests:
          - dbt_expectations.expect_column_mean_to_be_between:
              name: expectations_amount_mean
              arguments:
                min_value: 10
                max_value: 40
      - name: email
        data_tests:
          - dbt_expectations.expect_column_values_to_match_regex:
              name: expectations_email_regex
              arguments:
                regex: "^[a-z]+@example"
      - name: measured_at
        data_tests:
          - dbt_expectations.expect_row_values_to_have_recent_data:
              name: expectations_recent_data
              arguments:
                datepart: year
                interval: 1000
"""

# Tests that resolve to a `default__` implementation DuckDB accepts.
EXPECTED_PASS = {
    "expectations_row_count",
    "expectations_column_count",
    "expectations_compound_unique",
    "expectations_id_unique",
    "expectations_id_not_null",
    "expectations_id_between",
    "expectations_team_in_set",
    "expectations_team_distinct_count",
    "expectations_amount_mean",
}

# Tests blocked by a macro dbt_expectations (or dbt_date) only implements under
# a `duckdb__` prefix, which dispatch cannot reach for adapter type
# `motherduck`. Both are upstream gaps, not adapter bugs:
#
#   expectations_email_regex   default__regexp_instr emits regexp_instr(),
#                              which DuckDB does not have. duckdb__ uses
#                              regexp_matches(). Also affects the three other
#                              regex-matching tests in the package.
#   expectations_recent_data   reaches dbt_date.now() -> convert_timezone(),
#                              which DuckDB does not have. Fixing
#                              dbt_date.convert_timezone unblocks this one plus
#                              expect_grouped_row_values_to_have_recent_data and
#                              expect_column_values_to_be_within_n_moving_stdevs.
EXPECTED_FAIL = {
    "expectations_email_regex",
    "expectations_recent_data",
}


class TestDbtExpectations:
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "metaplane/dbt_expectations",
                    "version": [">=0.10.0", "<0.11.0"],
                }
            ]
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expectations_subject.csv": SUBJECT_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": SCHEMA_YML}

    def test_generic_tests(self, project):
        run_dbt(["deps"])
        assert len(run_dbt(["seed"])) == 1

        results = run_dbt(["test"], expect_pass=False)
        statuses = {result.node.name: result.status for result in results}

        assert set(statuses) == EXPECTED_PASS | EXPECTED_FAIL
        passed = {name for name, status in statuses.items() if status == "pass"}
        assert passed == EXPECTED_PASS
