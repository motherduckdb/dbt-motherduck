# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_unit_testing.py.
import pytest
from dbt.tests.adapter.unit_testing.test_case_insensitivity import (
    BaseUnitTestCaseInsensivity,
)
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput
from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from dbt.tests.util import run_dbt


class TestUnitTestingTypesDuckDB(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["2.0", "2.0"],
            ["'12345'", "12345"],
            ["'string'", "string"],
            ["true", "true"],
            ["DATE '2020-01-02'", "2020-01-02"],
            ["TIMESTAMP '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            ["'2013-11-03 00:00:00-0'::TIMESTAMPTZ", "2013-11-03 00:00:00-0"],
            [
                "{'Alberta':'Edmonton','Manitoba':'Winnipeg'}",
                "{'Alberta':'Edmonton','Manitoba':'Winnipeg'}",
            ],
            ["ARRAY['a','b','c']", "['a','b','c']"],
            ["ARRAY[1,2,3]", "[1, 2, 3]"],
        ]


class TestUnitTestCaseInsensitivityDuckDB(BaseUnitTestCaseInsensivity):
    pass


class TestUnitTestInvalidInputDuckDB(BaseUnitTestInvalidInput):
    pass


class TestUnitTestMetadata:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": "select 1 as id",
            "empty.sql": "select id from {{ ref('upstream') }} where false",
            "sql_fixture.sql": "select id from {{ ref('upstream') }}",
            "duplicate.sql": "select id as value, id + 1 as value from {{ ref('upstream') }}",
            "schema.yml": """
unit_tests:
  - name: empty_expected
    model: empty
    given:
      - input: ref('upstream')
        rows: [{id: 1}]
    expect:
      rows: []
  - name: sql_expected
    model: sql_fixture
    given:
      - input: ref('upstream')
        format: sql
        rows: select 1 as id
    expect:
      format: sql
      rows: select 1 as id
  - name: duplicate_aliases
    model: duplicate
    given:
      - input: ref('upstream')
        rows: [{id: 1}]
    expect:
      rows: [{value: 1, value_1: 2}]
""",
        }

    def test_metadata_without_temporary_tables(self, project):
        run_dbt(["run", "--select", "upstream"])
        results = run_dbt(["test", "--select", "test_type:unit"])
        assert len(results) == 3
