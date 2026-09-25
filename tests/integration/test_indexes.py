# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/indexes/test_indexes.py and fixtures.py.
import re

import pytest
from dbt.tests.util import (
    run_dbt,
)

models__incremental_sql = """
{{
  config(
    materialized = "incremental",
    indexes=[
      {'columns': ['column_a']},
      {'columns': ['column_a', 'column_b'], 'unique': True},
    ]
  )
}}

select *
from (
  select 1 as column_a, 2 as column_b
) t

{% if is_incremental() %}
    where column_a > (select max(column_a) from {{this}})
{% endif %}

"""

models__table_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_a']},
      {'columns': ['column_b']},
      {'columns': ['column_a', 'column_b']},
      {'columns': ['column_b', 'column_a'], 'unique': True},
      {'columns': ['column_a']}
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

snapshots__colors_sql = """
{% snapshot colors %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['color'],
            indexes=[
              {'columns': ['id']},
              {'columns': ['id', 'color'], 'unique': True},
            ]
        )
    }}

    {% if var('version') == 1 %}

        select 1 as id, 'red' as color union all
        select 2 as id, 'green' as color

    {% else %}

        select 1 as id, 'blue' as color union all
        select 2 as id, 'green' as color

    {% endif %}

{% endsnapshot %}

"""

seeds__seed_csv = """country_code,country_name
US,United States
CA,Canada
GB,United Kingdom
"""


INDEX_DEFINITION_PATTERN = re.compile(r"\((.*?)\)")


@pytest.mark.skip_database_type("ducklake", reason="DuckLake does not support indexes")
class TestIndex:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table.sql": models__table_sql,
            "incremental.sql": models__incremental_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"colors.sql": snapshots__colors_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seeds": {
                "quote_columns": False,
                "indexes": [
                    {"columns": ["country_code"], "unique": False},
                    {"columns": ["country_code", "country_name"], "unique": True},
                ],
            },
            "vars": {
                "version": 1,
            },
        }

    def test_table(self, project, unique_schema):
        for _ in range(2):
            results = run_dbt(["run", "--models", "table"])
            assert len(results) == 1

            indexes = self.get_indexes("table", project, unique_schema)
            expected = [
                {"columns": "column_a", "unique": False},
                {"columns": "column_b", "unique": False},
                {"columns": "column_a, column_b", "unique": False},
                {"columns": "column_b, column_a", "unique": True},
                {"columns": "column_a", "unique": False},
            ]
            assert sorted((i["columns"], i["unique"]) for i in indexes) == sorted(
                (i["columns"], i["unique"]) for i in expected
            )

    def test_incremental(self, project, unique_schema):
        for additional_argument in [[], [], ["--full-refresh"]]:
            results = run_dbt(["run", "--models", "incremental"] + additional_argument)
            assert len(results) == 1

            indexes = self.get_indexes("incremental", project, unique_schema)
            expected = [
                {"columns": "column_a", "unique": False},
                {"columns": "column_a, column_b", "unique": True},
            ]
            assert sorted((i["columns"], i["unique"]) for i in indexes) == sorted(
                (i["columns"], i["unique"]) for i in expected
            )

            # Regression for #660: full-refresh must clean up the __dbt_backup table.
            backup_tables = project.run_sql(
                f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE lower(table_schema) = lower('{unique_schema}')
                  AND table_name LIKE '%__dbt_backup'
                """,
                fetch="all",
            )
            assert backup_tables == [], (
                f"__dbt_backup tables leaked after run: {backup_tables}"
            )

    def test_seed(self, project, unique_schema):
        for additional_argument in [[], [], ["--full-refresh"]]:
            results = run_dbt(["seed"] + additional_argument)
            assert len(results) == 1

            indexes = self.get_indexes("seed", project, unique_schema)
            expected = [
                {"columns": "country_code", "unique": False},
                {
                    "columns": "country_code, country_name",
                    "unique": True,
                },
            ]
            assert sorted((i["columns"], i["unique"]) for i in indexes) == sorted(
                (i["columns"], i["unique"]) for i in expected
            )

    def test_snapshot(self, project, unique_schema):
        for version in [1, 2]:
            results = run_dbt(["snapshot", "--vars", f"version: {version}"])
            assert len(results) == 1

            indexes = self.get_indexes("colors", project, unique_schema)
            expected = [
                {"columns": "id", "unique": False},
                {"columns": "id, color", "unique": True},
            ]
            assert sorted((i["columns"], i["unique"]) for i in indexes) == sorted(
                (i["columns"], i["unique"]) for i in expected
            )

    def get_indexes(self, table_name, project, unique_schema):
        sql = f"""
            SELECT
              sql as index_definition, is_unique
            FROM duckdb_indexes()
            WHERE
              schema_name = '{unique_schema}'
              AND 
              table_name = '{table_name}'
        """
        results = project.run_sql(sql, fetch="all")
        return [self.parse_index_definition(row[0], row[1]) for row in results]

    def parse_index_definition(self, index_definition, is_unique):
        index_definition = index_definition.lower()
        m = INDEX_DEFINITION_PATTERN.search(index_definition)
        return {
            "columns": m.group(1),
            "unique": is_unique,
        }
