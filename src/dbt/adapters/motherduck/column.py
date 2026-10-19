# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# dbt/adapters/duckdb/column.py.
import re
from dataclasses import dataclass, field

from dbt.adapters.base.column import Column


@dataclass
class MotherDuckColumn(Column):
    fields: list["MotherDuckColumn"] = field(default_factory=list)

    def __post_init__(self):
        if self.is_struct():
            self._parse_struct_fields()

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

    def is_struct(self) -> bool:
        return self.dtype.lower().startswith("struct")

    def flatten(self) -> list["MotherDuckColumn"]:
        if not self.is_struct():
            return [self]

        flat_columns: list[MotherDuckColumn] = []
        for column_field in self.fields:
            if column_field.is_struct():
                # Recursively flatten nested structs
                for nested_field in column_field.flatten():
                    flat_columns.append(
                        MotherDuckColumn(
                            column=f"{self.column}.{nested_field.column}",
                            dtype=nested_field.dtype,
                        )
                    )
            else:
                flat_columns.append(
                    MotherDuckColumn(
                        column=f"{self.column}.{column_field.column}",
                        dtype=column_field.dtype,
                    )
                )
        return flat_columns

    def _parse_struct_fields(self):
        # In DuckDB, structs are defined as STRUCT(key1 type1, key2 type2, ...)
        # We need to extract the key-type pairs from the struct definition
        # e.g., STRUCT(a VARCHAR, b INTEGER) -> ["a VARCHAR", "b INTEGER"]
        # We can't just split by comma, because types can contain commas
        # e.g. DECIMAL(10, 2)
        # The following logic will handle nested structs and complex types
        match = re.match(r"STRUCT\((.*)\)", self.dtype, re.IGNORECASE)
        if not match:
            return

        content = match.group(1)

        fields = []
        paren_level = 0
        quote = None
        current_field = ""
        index = 0
        while index < len(content):
            char = content[index]
            if quote is not None:
                if char == quote:
                    if content[index : index + 2] == quote * 2:
                        current_field += quote * 2
                        index += 2
                        continue
                    quote = None
            elif char in ('"', "'"):
                # Identifiers use double quotes; ENUM labels use single quotes.
                quote = char
            elif char == "(":
                paren_level += 1
            elif char == ")":
                paren_level -= 1

            if char == "," and paren_level == 0 and quote is None:
                fields.append(current_field.strip())
                current_field = ""
            else:
                current_field += char
            index += 1
        fields.append(current_field.strip())

        for f in fields:
            in_quotes = False
            index = 0
            while index < len(f):
                char = f[index]
                if char == '"':
                    in_quotes = not in_quotes
                elif char.isspace() and not in_quotes:
                    break
                index += 1
            col_name = f[:index]
            col_type = f[index:].strip()
            self.fields.append(MotherDuckColumn(column=col_name, dtype=col_type))
