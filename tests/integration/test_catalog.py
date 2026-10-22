# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_basic.py and test_get_columns_in_relation.py.
import pytest
from dbt.tests.adapter.basic.expected_catalog import (
    base_expected_catalog,
    expected_references_catalog,
    no_stats,
)
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
)
from dbt.tests.util import get_connection, relation_from_name, run_dbt


class TestDocsGenerate(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return base_expected_catalog(
            project,
            role=None,
            id_type="INTEGER",
            text_type="VARCHAR",
            time_type="TIMESTAMP",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
        )


class TestDocsGenReferences(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return expected_references_catalog(
            project,
            role=None,
            id_type="INTEGER",
            text_type="VARCHAR",
            time_type="TIMESTAMP",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
            bigint_type="BIGINT",
        )


class TestGetColumnsInRelation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "types_model.sql": """
{{ config(materialized='table') }}
select 1::integer as id, 1.5::decimal(18, 2) as amount, 'x'::varchar as label,
       cast(null as struct(fname varchar, age integer)) as nested
"""
        }

    def test_describe_preserves_type_strings(self, project):
        run_dbt(["run"])
        relation = relation_from_name(project.adapter, "types_model")
        with get_connection(project.adapter):
            columns = project.adapter.get_columns_in_relation(relation)
        assert {column.name: column.dtype for column in columns} == {
            "id": "INTEGER",
            "amount": "DECIMAL(18,2)",
            "label": "VARCHAR",
            "nested": "STRUCT(fname VARCHAR, age INTEGER)",
        }

    def test_schema_inspection_preserves_nested_type_strings(self, project):
        sql = """
select
    payload: cast(null as struct(
        "a b" integer,
        "a,b" varchar,
        "a""b" bigint,
        e enum('a''"(,)'),
        nested struct("c,d" decimal(10,2))
    )),
    items: [{'a': 1}],
    pair: row(1, 'x')
"""
        with get_connection(project.adapter):
            columns = project.adapter.get_column_schema_from_query(sql)
        assert [(column.name, column.dtype) for column in columns] == [
            (
                "payload",
                """STRUCT("a b" INTEGER, "a,b" VARCHAR, "a""b" BIGINT, """
                """e ENUM('a''"(,)'), nested STRUCT("c,d" DECIMAL(10,2)))""",
            ),
            ("items", "STRUCT(a INTEGER)[]"),
            ("pair", "STRUCT(INTEGER, VARCHAR)"),
        ]


class TestCatalogRelations:
    @pytest.fixture(scope="class")
    def other_database(self, temporary_database):
        with temporary_database() as name:
            yield name

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target, other_database):
        return {**dbt_profile_target, "attach_databases": [other_database]}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "alpha_model.sql": """
{{ config(materialized='table') }}
select id: 1, note: 'main'
""",
            "beta_model.sql": "select id: 2, score: 3.14",
        }

    def test_get_catalog_relations(self, project, other_database):
        assert len(run_dbt(["run"])) == 2
        adapter = project.adapter
        shadow = adapter.Relation.create(
            database=other_database,
            schema=project.test_schema,
            identifier="alpha_model",
            type="table",
        )
        with get_connection(adapter):
            adapter.execute(f"create schema {shadow.without_identifier()}")
            adapter.execute(
                f"create table {shadow} as "
                "select shadow_id: 999, shadow_note: 'other database', shadow_flag: true"
            )
            _, databases = adapter.execute(
                "select database_name from duckdb_databases()", fetch=True
            )
            assert {project.database, other_database} <= {
                row[0] for row in databases.rows
            }

            information_schema = adapter.Relation.create(
                database=project.database.upper(), schema="information_schema"
            )
            specific = adapter.Relation.create(
                database=project.database.upper(),
                schema=project.test_schema.upper(),
                identifier="ALPHA_MODEL",
            )
            schema = specific.incorporate(path={"identifier": None})
            alpha_columns = [
                (project.database, project.test_schema, "alpha_model", "id"),
                (project.database, project.test_schema, "alpha_model", "note"),
            ]
            beta_columns = [
                (project.database, project.test_schema, "beta_model", "id"),
                (project.database, project.test_schema, "beta_model", "score"),
            ]
            for relation, expected in (
                (specific, alpha_columns),
                (schema, alpha_columns + beta_columns),
            ):
                catalog = adapter.execute_macro(
                    "get_catalog_relations",
                    kwargs={
                        "information_schema": information_schema,
                        "relations": [relation],
                    },
                )
                assert [
                    (
                        row["table_database"],
                        row["table_schema"],
                        row["table_name"],
                        row["column_name"],
                    )
                    for row in catalog.rows
                ] == expected


def test_external_source_columns(project):
    relation = project.adapter.Relation.create(
        database=project.database,
        schema=project.test_schema,
        identifier="external_without_catalog_table",
        external="range(3)",
    )
    with project.adapter.connection_named("external_columns"):
        columns = project.adapter.get_columns_in_relation(relation)
    assert [(column.name, column.dtype) for column in columns] == [("range", "BIGINT")]


def test_missing_relation_columns(project):
    relation = project.adapter.Relation.create(
        database=project.database,
        schema=project.test_schema,
        identifier="does_not_exist",
    )
    with project.adapter.connection_named("missing_columns"):
        assert project.adapter.get_columns_in_relation(relation) == []


class TestQuotedMetadata:
    @pytest.fixture(scope="class")
    def other_database(self, temporary_database):
        with temporary_database(prefix="dbt_motherduck_test_quote'") as name:
            yield name

    @pytest.fixture(scope="class")
    def project_config_update(self, other_database):
        return {"models": {"+database": other_database, "+schema": "sche'ma.with.dot"}}

    @pytest.fixture(scope="class")
    def models(self, database_type):
        indexes = "[]" if database_type == "ducklake" else "[{'columns': ['id']}]"
        return {
            "quoted.sql": "{{ config(materialized='table', alias=\"ta'ble.with.dot\", indexes="
            + indexes
            + ") }} select id: 1"
        }

    def test_build_and_replace_quoted_names(
        self, project, other_database, database_type
    ):
        for _ in range(2):
            result = run_dbt(["run"])
            node = result.results[0].node
            relation = project.adapter.Relation.create(
                database=node.database,
                schema=node.schema,
                identifier=node.alias,
                type="table",
            )
            assert project.run_sql(f"select * from {relation}", fetch="all") == [(1,)]
            with get_connection(project.adapter):
                assert relation.schema in project.adapter.list_schemas(other_database)
                assert (
                    project.adapter.get_relation(
                        database=relation.database,
                        schema=relation.schema,
                        identifier=relation.identifier,
                    )
                    is not None
                )
                columns = project.adapter.get_columns_in_relation(relation)
                assert [(column.name, column.dtype) for column in columns] == [
                    ("id", "INTEGER")
                ]
                if database_type == "native":
                    handle = project.adapter.connections.get_thread_connection().handle
                    assert handle.execute(
                        "select count(*) from duckdb_indexes() where database_name = %s and schema_name = %s and table_name = %s",
                        (relation.database, relation.schema, relation.identifier),
                    ).fetchone() == (1,)
