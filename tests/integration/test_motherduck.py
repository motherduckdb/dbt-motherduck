# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/plugins/motherduck/test_macros.py, test_motherduck.py,
# and test_motherduck_write_conflict.py.
import pytest
from dbt.tests.util import relation_from_name, run_dbt


class TestConcurrentIncrementalStaging:
    @pytest.fixture
    def other_database(self, temporary_database):
        with temporary_database() as name:
            yield name

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"incremental_{limit}.sql": f"""
{{{{ config(materialized='incremental', database=var('output_database'),
            meta={{'temp_schema_name': var('staging_schema')}}) }}}}
select generate_series as id from generate_series(1, {limit})
{{% if is_incremental() %}}
where generate_series > (select coalesce(max(id), 0) from {{{{ this }}}})
{{% endif %}}
"""
            for limit in (50, 100)
        }

    @pytest.mark.parametrize("staging_schema", ["dbt_temp", "custom_staging"])
    def test_two_models_share_staging_schema(
        self, project, staging_schema, other_database
    ):
        for iteration in range(3):
            args = [
                "run",
                "--threads",
                "2",
                "--vars",
                f"{{output_database: {other_database}, staging_schema: {staging_schema}}}",
            ]
            if iteration == 0:
                args.append("--full-refresh")
            results = run_dbt(args)
            assert len(results) == 2
            for limit in (50, 100):
                node = next(
                    result.node
                    for result in results
                    if result.node.name == f"incremental_{limit}"
                )
                relation = project.adapter.Relation.create(
                    database=node.database,
                    schema=node.schema,
                    identifier=node.alias,
                )
                assert project.run_sql(
                    f"select count(*) from {relation}", fetch="one"
                ) == (limit,)
            assert (
                project.run_sql(
                    "select table_name from information_schema.tables "
                    f"where table_catalog = '{other_database}' "
                    f"and table_schema = '{staging_schema}'",
                    fetch="all",
                )
                == []
            )
            assert project.run_sql(
                "select count(*) from system.information_schema.schemata "
                f"where catalog_name = '{other_database}' "
                f"and schema_name = '{staging_schema}'",
                fetch="one",
            ) == (1,)


class TestStagingSchemaInitialization:
    @pytest.fixture(scope="class")
    def other_database(self, temporary_database):
        with temporary_database() as name:
            yield name

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ordinary.sql": """
{{ config(materialized='table', database=var('output_database')) }}
select id: 1
""",
            "incremental.sql": """
{{ config(materialized='incremental', database=var('output_database')) }}
select id: 1
""",
        }

    def test_schema_is_created_only_for_staged_materializations(
        self, project, other_database
    ):
        args = ["--vars", f"{{output_database: {other_database}}}"]
        assert len(run_dbt(["run", "--select", "ordinary", *args])) == 1
        assert project.run_sql(
            "select count(*) from system.information_schema.schemata "
            f"where catalog_name = '{other_database}' and schema_name = 'dbt_temp'",
            fetch="one",
        ) == (0,)

        assert len(run_dbt(["run", "--select", "incremental", *args])) == 1
        assert project.run_sql(
            "select count(*) from system.information_schema.schemata "
            f"where catalog_name = '{other_database}' and schema_name = 'dbt_temp'",
            fetch="one",
        ) == (1,)


@pytest.mark.parametrize(
    "other_database, uppercase",
    [
        ("dbt_motherduck_test_", False),
        ("dbt_motherduck_test_", True),
        ("DBT_MOTHERDUCK_TEST_", False),
        ("dbt_MotherDuck_Test_", True),
    ],
    indirect=["other_database"],
)
class TestCrossDatabaseNames:
    @pytest.fixture(scope="class")
    def other_database(self, temporary_database, request):
        with temporary_database(prefix=request.param) as name:
            yield name

    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": "select * from {{ ref('seed') }}"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": "a,b,c\n1,2,3\n4,5,6\n7,8,9\n"}

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "database.sql": """
{% macro generate_database_name(custom_database_name=none, node=none) %}
{{ return(var('output_database', target.database)) }}
{% endmacro %}
"""
        }

    def test_database_name_case(self, project, other_database, uppercase):
        name = other_database.upper() if uppercase else other_database.lower()
        args = ["--vars", f"{{output_database: {name}}}"]
        assert len(run_dbt(["seed", *args])) == 1
        for _ in range(3):
            results = run_dbt(["run", *args])
            assert len(results) == 1
            node = results.results[0].node
            assert node.database == name
            relation = project.adapter.Relation.create(
                database=node.database, schema=node.schema, identifier=node.alias
            )
            assert project.run_sql(
                f"select * from {relation} order by a", fetch="all"
            ) == [(1, 2, 3), (4, 5, 6), (7, 8, 9)]


class TestStagingNameCollision:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "summary.sql": """
{{ config(materialized='incremental', meta={'temp_schema_name': 'dbt_temp_test'}) }}
select i as id from range(10) t(i)
{% if is_incremental() %} where i >= 5 {% endif %}
"""
        }

    def test_existing_staging_name_is_preserved(self, project):
        staging = project.adapter.Relation.create(
            database=project.database, schema="dbt_temp_test", identifier="summary"
        )
        project.run_sql(f"create schema if not exists {staging.without_identifier()}")
        project.run_sql(f"create table {staging} as select 42 as sentinel")
        target = relation_from_name(project.adapter, "summary")
        for count in (10, 15):
            assert len(run_dbt(["run"])) == 1
            assert project.run_sql(f"select count(*) from {target}", fetch="one") == (
                count,
            )
            assert project.run_sql(f"select * from {staging}", fetch="all") == [(42,)]
