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


@pytest.mark.parametrize(
    "dtype, expected",
    [
        ("struct(a integer, b varchar)", True),
        ("struct(a integer)", True),
        ("STRUCT(a integer, b varchar)", True),
        ("integer", False),
        ("varchar", False),
    ],
)
def test_is_struct(dtype, expected):
    column = MotherDuckColumn(column="struct_test", dtype=dtype)
    assert column.is_struct() == expected


def test_flatten_simple_struct():
    column = MotherDuckColumn(
        column="struct_test", dtype="struct(a integer, b varchar)"
    )
    flattened = column.flatten()
    assert len(flattened) == 2
    assert flattened[0].column == "struct_test.a"
    assert flattened[0].dtype == "integer"
    assert flattened[1].column == "struct_test.b"
    assert flattened[1].dtype == "varchar"


def test_flatten_nested_struct():
    column = MotherDuckColumn(
        column="struct_test", dtype="struct(a integer, b struct(c integer, d varchar))"
    )
    flattened = column.flatten()
    assert len(flattened) == 3
    assert flattened[0].column == "struct_test.a"
    assert flattened[0].dtype == "integer"
    assert flattened[1].column == "struct_test.b.c"
    assert flattened[1].dtype == "integer"
    assert flattened[2].column == "struct_test.b.d"
    assert flattened[2].dtype == "varchar"


def test_flatten_non_struct():
    column = MotherDuckColumn(column="integer_test", dtype="integer")
    flattened = column.flatten()
    assert len(flattened) == 1
    assert flattened[0].column == "integer_test"
    assert flattened[0].dtype == "integer"


def test_flatten_struct_with_decimal_precision():
    column = MotherDuckColumn(
        "payload",
        "STRUCT(id INTEGER, nested STRUCT(value DECIMAL(10,2), label VARCHAR))",
    )
    assert [(field.name, field.dtype) for field in column.flatten()] == [
        ("payload.id", "INTEGER"),
        ("payload.nested.value", "DECIMAL(10,2)"),
        ("payload.nested.label", "VARCHAR"),
    ]


def test_flatten_struct_with_quoted_field_names():
    column = MotherDuckColumn(
        "payload",
        'STRUCT("a b" INTEGER, "a,b" VARCHAR, "a(b)" BOOLEAN, "a""b" BIGINT, '
        'nested STRUCT("c,d" DECIMAL(10,2)))',
    )
    assert [(field.name, field.dtype) for field in column.flatten()] == [
        ('payload."a b"', "INTEGER"),
        ('payload."a,b"', "VARCHAR"),
        ('payload."a(b)"', "BOOLEAN"),
        ('payload."a""b"', "BIGINT"),
        ('payload.nested."c,d"', "DECIMAL(10,2)"),
    ]
