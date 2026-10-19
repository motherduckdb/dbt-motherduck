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
        return _struct_body(_tokenize(self.dtype)) is not None

    def flatten(self) -> list["MotherDuckColumn"]:
        # Only structs with named fields are flattened. Lists/arrays of structs,
        # unnamed structs (from row(...)) and anything we can't parse are kept whole.
        if not self.fields:
            return [self]
        return [
            MotherDuckColumn(
                column=f"{self.column}.{nested_field.column}",
                dtype=nested_field.dtype,
            )
            for column_field in self.fields
            for nested_field in column_field.flatten()
        ]

    def _parse_struct_fields(self):
        for name, dtype in _parse_struct_fields(self.dtype) or []:
            self.fields.append(MotherDuckColumn(column=name, dtype=dtype))


# DuckDB prints struct types as STRUCT(name type, ...). Names are bare words or
# double-quoted identifiers ("" escapes a quote); ENUM labels are single-quoted
# literals ('' escapes a quote). Either may contain spaces, commas, parentheses or
# newlines, so the type string has to be tokenized rather than scanned by character.
_TOKEN_RE = re.compile(
    r"""
      (?P<ident>"(?:[^"]|"")*")
    | (?P<literal>'(?:[^']|'')*')
    | (?P<punct>[()\[\],])
    | (?P<word>[^\s"'()\[\],]+)
    | (?P<space>\s+)
    """,
    re.VERBOSE,
)


@dataclass(frozen=True)
class _Token:
    kind: str
    text: str
    start: int
    end: int


def _tokenize(dtype: str) -> list[_Token] | None:
    tokens = []
    position = 0
    while position < len(dtype):
        match = _TOKEN_RE.match(dtype, position)
        if match is None:
            # Unterminated quote: not a type string DuckDB would print.
            return None
        if match.lastgroup != "space":
            tokens.append(_Token(match.lastgroup, match.group(), *match.span()))
        position = match.end()
    return tokens


def _struct_body(tokens: list[_Token] | None) -> list[_Token] | None:
    """Return the tokens between STRUCT( and its closing paren, if the whole type
    is a bare struct. STRUCT(...)[] and STRUCT(...)[3] are lists/arrays."""
    if (
        not tokens
        or len(tokens) < 3
        or tokens[0].kind != "word"
        or tokens[0].text.upper() != "STRUCT"
        or tokens[1].text != "("
    ):
        return None
    depth = 0
    for index, token in enumerate(tokens[1:], start=1):
        if token.text in ("(", "["):
            depth += 1
        elif token.text in (")", "]"):
            depth -= 1
            if depth == 0:
                return tokens[2:index] if index == len(tokens) - 1 else None
    return None


def _parse_struct_fields(dtype: str) -> list[tuple[str, str]] | None:
    body = _struct_body(_tokenize(dtype))
    if not body:
        return None

    fields: list[list[_Token]] = [[]]
    depth = 0
    for token in body:
        if token.text in ("(", "["):
            depth += 1
        elif token.text in (")", "]"):
            depth -= 1
        if token.text == "," and depth == 0:
            fields.append([])
        else:
            fields[-1].append(token)

    parsed = []
    for name, *type_tokens in fields:
        # Unnamed structs print as STRUCT(INTEGER, TIMESTAMP WITH TIME ZONE). In named
        # structs the type always starts with a bare word, and DuckDB quotes field
        # names that are keywords, so "TIMESTAMP" followed by WITH can't be a name.
        if (
            name.kind not in ("ident", "word")
            or not type_tokens
            or type_tokens[0].kind != "word"
            or type_tokens[0].text.upper() == "WITH"
        ):
            return None
        parsed.append((name.text, dtype[type_tokens[0].start : type_tokens[-1].end]))
    return parsed
