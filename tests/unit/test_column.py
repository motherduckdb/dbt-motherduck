# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/unit/test_column.py.
import pytest

from dbt.adapters.motherduck.column import MotherDuckColumn


@pytest.mark.parametrize(
    "dtype, expected",
    [
        ("real", True),
        ("float", True),
        ("float4", True),
        ("float8", True),
        ("double", True),
        ("integer", False),
        ("string", False),
        ("bigint", False),
    ],
)
def test_is_float(dtype, expected):
    for spelling in (dtype, dtype.upper()):
        column = MotherDuckColumn(column="float_test", dtype=spelling)
        assert column.is_float() == expected


@pytest.mark.parametrize(
    "dtype, expected",
    [
        ("tinyint", True),
        ("smallint", True),
        ("integer", True),
        ("bigint", True),
        ("hugeint", True),
        ("utinyint", True),
        ("usmallint", True),
        ("uinteger", True),
        ("ubigint", True),
        ("int1", True),
        ("int2", True),
        ("int4", True),
        ("int8", True),
        ("short", True),
        ("int", True),
        ("signed", True),
        ("long", True),
        ("float", False),
        ("string", False),
        ("double", False),
    ],
)
def test_is_integer(dtype, expected):
    for spelling in (dtype, dtype.upper()):
        column = MotherDuckColumn(column="integer_test", dtype=spelling)
        assert column.is_integer() == expected
