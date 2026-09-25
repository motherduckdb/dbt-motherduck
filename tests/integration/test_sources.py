# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_sources.py; uses a remote fixture instead of /tmp.
import pytest
from dbt.tests.util import get_connection, relation_from_name, run_dbt


class TestExternalSources:
    @pytest.fixture(scope="class")
    def models(self):
        root = (
            "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/"
            "fd7bfacae4f497ff044a6a0275268676bf1b64c3/seeds"
        )
        return {
            "sources.yml": f"""
version: 2
sources:
  - name: remote
    config:
      meta:
        external_location: "{root}/{{name}}.csv"
    tables:
      - name: raw_customers
        columns:
          - name: id
            data_tests: [unique, not_null]
      - name: oldstyle
        identifier: raw_customers
        config:
          meta:
            external_location: "read_csv_auto('{root}/%(identifier)s.csv')"
            formatter: oldstyle
      - name: template
        config:
          meta:
            external_location: "read_csv_auto('{root}/${{file}}.csv')"
            formatter: template
            file: raw_customers
""",
            "customers.sql": """
{{ config(materialized='table') }}
select a.* from {{ source('remote', 'raw_customers') }} a
join {{ source('remote', 'oldstyle') }} b using (id)
join {{ source('remote', 'template') }} c using (id)
""",
        }

    def test_remote_sources(self, project):
        results = run_dbt(["build"])
        assert len(results) == 3
        relation = relation_from_name(project.adapter, "customers")
        assert project.run_sql(
            f"select count(*), min(id), max(id) from {relation}", fetch="one"
        ) == (100, 1, 100)
        assert project.run_sql(
            f"select first_name, last_name from {relation} where id = 1", fetch="one"
        ) == ("Michael", "P.")
        model = next(
            result.node for result in results if result.node.name == "customers"
        )
        sources = list(model.depends_on.nodes)
        assert len(sources) == 3
        with get_connection(project.adapter):
            external = project.adapter.Relation.create(
                external="'https://raw.githubusercontent.com/dbt-labs/jaffle_shop/"
                "fd7bfacae4f497ff044a6a0275268676bf1b64c3/seeds/raw_customers.csv'"
            )
            columns = project.adapter.get_columns_in_relation(external)
        assert [column.name for column in columns] == ["id", "first_name", "last_name"]


class TestRemoteParquet:
    @pytest.fixture(scope="class")
    def models(self):
        url = (
            "https://raw.githubusercontent.com/apache/parquet-testing/"
            "56653c437c8092f704a092d0d1d4e600124cd49f/data/alltypes_plain.parquet"
        )
        return {
            "sources.yml": f"""
version: 2
sources:
  - name: remote
    tables:
      - name: parquet_path
        config:
          meta:
            external_location: "'{url}'"
      - name: parquet_reader
        config:
          meta:
            external_location: "read_parquet(['{url}'], union_by_name=true)"
""",
            "parquet.sql": """
select a.id from {{ source('remote', 'parquet_path') }} a
join {{ source('remote', 'parquet_reader') }} b using (id)
""",
        }

    def test_remote_parquet(self, project):
        assert len(run_dbt(["run"])) == 1
        relation = relation_from_name(project.adapter, "parquet")
        assert project.run_sql(
            f"select id from {relation} order by id", fetch="all"
        ) == [(i,) for i in range(8)]
