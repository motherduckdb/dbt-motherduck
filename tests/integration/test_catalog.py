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
