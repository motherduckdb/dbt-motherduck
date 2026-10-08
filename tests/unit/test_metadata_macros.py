from pathlib import Path
from types import SimpleNamespace

import pytest
from dbt_common.clients.jinja import get_template

from dbt.adapters.motherduck.relation import MotherDuckRelation
from dbt.include.motherduck import PACKAGE_PATH


@pytest.fixture
def metadata_macros():
    queries = []

    def run_query(sql):
        queries.append(" ".join(sql.split()))
        return []

    def statement(name, caller, **kwargs):
        queries.append(" ".join(caller().split()))
        return ""

    context = {
        "run_query": run_query,
        "statement": statement,
        "load_result": lambda name: SimpleNamespace(table=[]),
    }
    source = (Path(PACKAGE_PATH) / "macros" / "adapters.sql").read_text()
    module = get_template(source, context).make_module(context)
    return module, queries


@pytest.mark.parametrize(
    "database, expected_database",
    [("db", "db"), ("DB'Name", "db''name")],
)
def test_create_schema_escapes_database_literal(
    metadata_macros, database, expected_database
):
    module, queries = metadata_macros
    relation = MotherDuckRelation.create(database=database, schema="schema")

    module.dbt_macro__motherduck__create_schema(relation)

    assert queries == [
        "select type from duckdb_databases() "
        f"where lower(database_name)='{expected_database}' and type != 'duckdb'",
        f"create schema if not exists {relation.without_identifier()}",
    ]


@pytest.mark.parametrize(
    "database, schema, identifier, expected_literals",
    [
        ("db", "schema", "model", ("db", "schema", "model")),
        (
            "DB'Name",
            "Schema'Name",
            "Model'Name",
            ("DB''Name", "Schema''Name", "Model''Name"),
        ),
    ],
)
def test_drop_indexes_escapes_relation_literals(
    metadata_macros, database, schema, identifier, expected_literals
):
    module, queries = metadata_macros
    relation = MotherDuckRelation.create(
        database=database, schema=schema, identifier=identifier
    )
    expected_database, expected_schema, expected_identifier = expected_literals

    module.dbt_macro__drop_indexes_on_relation(relation)

    assert queries == [
        "SELECT index_name FROM duckdb_indexes() "
        f"WHERE database_name = '{expected_database}' "
        f"AND schema_name = '{expected_schema}' "
        f"AND table_name = '{expected_identifier}'"
    ]
