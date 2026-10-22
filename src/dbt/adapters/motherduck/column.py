# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# dbt/adapters/duckdb/column.py.
from dbt.adapters.base.column import Column


class MotherDuckColumn(Column):
    def is_float(self):
        return self.dtype.lower() in {
            # floats
            "real",
            "float",
            "float4",
            "float8",
            "double",
        }

    def is_integer(self) -> bool:
        return self.dtype.lower() in {
            # signed types
            "tinyint",
            "smallint",
            "integer",
            "bigint",
            "hugeint",
            # unsigned types
            "utinyint",
            "usmallint",
            "uinteger",
            "ubigint",
            # aliases
            "int1",
            "int2",
            "int4",
            "int8",
            "short",
            "int",
            "signed",
            "long",
        }
