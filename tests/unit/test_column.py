import pytest

from dbt.adapters.motherduck.column import MotherDuckColumn


@pytest.mark.parametrize("dtype", ["TINYINT", "UINTEGER", "HUGEINT", "int8"])
def test_integer_types(dtype):
    column = MotherDuckColumn("value", dtype)
    assert column.is_integer()
    assert not column.is_float()


@pytest.mark.parametrize("dtype", ["DOUBLE", "REAL", "float8"])
def test_float_types(dtype):
    column = MotherDuckColumn("value", dtype)
    assert column.is_float()
    assert not column.is_integer()


def test_nested_struct_columns():
    column = MotherDuckColumn(
        "payload",
        "STRUCT(id INTEGER, nested STRUCT(value DECIMAL(10,2), label VARCHAR))",
    )
    assert [(field.name, field.dtype) for field in column.flatten()] == [
        ("payload.id", "INTEGER"),
        ("payload.nested.value", "DECIMAL(10,2)"),
        ("payload.nested.label", "VARCHAR"),
    ]
