# Third-party package compatibility: dbt-labs/codegen.
#
# codegen is adapter-agnostic (ten macros, none of them dispatched per adapter
# beyond a single bigquery branch), so it is a cheap canary rather than a
# likely source of gaps: everything it does goes through
# get_columns_in_relation, the catalog, and dbt_utils' information_schema
# queries. If relation metadata regresses, this is where it shows first.
#
# The macros print their output and return it, so the assertions are made
# inside a model that inspects the returned string rather than by scraping logs.
import pytest
from dbt.tests.util import run_dbt

ORDERS_CSV = """id,status,amount
1,new,10
2,paid,20
"""

GENERATE_SOURCE_SQL = """
-- depends_on: {{ ref("codegen_orders") }}
{{ config(materialized="table") }}
{% set generated = codegen.generate_source(
    schema_name=target.schema,
    database_name=target.database,
    generate_columns=True,
    include_data_types=True,
) if execute else "" %}
select
    {{ "true" if "name: codegen_orders" in generated else "false" }} as lists_table,
    {{ "true" if "name: status" in generated else "false" }} as lists_column,
    {{ "true" if "data_type:" in generated else "false" }} as lists_data_type
"""

GENERATE_MODEL_YAML_SQL = """
-- depends_on: {{ ref("codegen_summary") }}
{{ config(materialized="table") }}
{% set generated = codegen.generate_model_yaml(
    model_names=["codegen_summary"],
) if execute else "" %}
select
    {{ "true" if "name: codegen_summary" in generated else "false" }} as names_model,
    {{ "true" if "name: order_count" in generated else "false" }} as lists_column,
    {{ "true" if "data_type:" in generated else "false" }} as lists_data_type
"""

SUMMARY_SQL = """
{{ config(materialized="table") }}
select status, count(*) as order_count
from {{ ref("codegen_orders") }}
group by status
"""


class TestDbtCodegen:
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"package": "dbt-labs/codegen", "version": [">=0.14.0", "<1.0.0"]}
            ]
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"codegen_orders.csv": ORDERS_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "codegen_summary.sql": SUMMARY_SQL,
            "codegen_generated_source.sql": GENERATE_SOURCE_SQL,
            "codegen_generated_model.sql": GENERATE_MODEL_YAML_SQL,
        }

    @pytest.fixture(scope="class")
    def built(self, project):
        run_dbt(["deps"])
        assert len(run_dbt(["seed"])) == 1
        assert len(run_dbt(["run"])) == 3
        return project

    def test_generate_source(self, built, project):
        row = project.run_sql(
            f"select lists_table, lists_column, lists_data_type "
            f"from {project.test_schema}.codegen_generated_source",
            fetch="one",
        )
        assert tuple(row) == (True, True, True)

    def test_generate_model_yaml(self, built, project):
        row = project.run_sql(
            f"select names_model, lists_column, lists_data_type "
            f"from {project.test_schema}.codegen_generated_model",
            fetch="one",
        )
        assert tuple(row) == (True, True, True)
