# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_constraints.py; includes the MotherDuck-enabled cases.
import pytest
from dbt.tests.adapter.constraints.test_constraints import (
    BaseIncrementalConstraintsColumnsEqual,
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
)

pytestmark = pytest.mark.skip_database_type(
    "ducklake", reason="DuckLake does not support primary or unique constraints"
)


class DuckDBColumnEqualSetup:
    @pytest.fixture
    def int_type(self):
        return "INT"

    @pytest.fixture
    def string_type(self):
        return "VARCHAR"

    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["true", "bool", "BOOL"],
            ["'2013-11-03 00:00:00-07'::timestamp", "TIMESTAMP", "TIMESTAMP"],
            [
                "'2013-11-03 00:00:00-07'::timestamptz",
                "TIMESTAMPTZ",
                "TIMESTAMP WITH TIME ZONE",
            ],
            ["ARRAY['a','b','c']", "VARCHAR[]", "VARCHAR[]"],
            ["ARRAY[1,2,3]", "INTEGER[]", "INTEGER[]"],
            ["'1'::numeric", "numeric", "DECIMAL"],
            [
                """'{"bar": "baz", "balance": 7.77, "active": false}'::json""",
                "json",
                "JSON",
            ],
        ]


class TestTableConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseTableConstraintsColumnsEqual
):
    pass


class TestViewConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseViewConstraintsColumnsEqual
):
    pass


class TestIncrementalConstraintsColumnsEqual(
    DuckDBColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual
):
    pass
