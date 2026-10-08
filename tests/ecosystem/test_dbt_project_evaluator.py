# Third-party package compatibility: dbt-labs/dbt_project_evaluator.
#
# This is the broadest "does a real project build" check available: the package
# ships around forty models plus seeds and generic tests, and leans on graph
# introspection, set operators, string aggregation and view-on-view chains.
#
# The package's own tests are assertions about the *host* project's modelling
# hygiene, and a two-model fixture deliberately violates several of them. So
# the assertion here is narrower and more useful: every model and seed the
# package ships must build. Test failures are expected and ignored.
import pytest
from dbt.tests.util import run_dbt

ORDERS_SQL = """
{{ config(materialized="table") }}
select 1 as id, 'new' as status
"""

SUMMARY_SQL = """
{{ config(materialized="view") }}
select status, count(*) as order_count from {{ ref("evaluator_orders") }}
group by status
"""


class TestDbtProjectEvaluator:
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_project_evaluator",
                    "version": [">=1.4.0", "<2.0.0"],
                }
            ]
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "evaluator_orders.sql": ORDERS_SQL,
            "evaluator_summary.sql": SUMMARY_SQL,
        }

    def test_package_builds(self, project):
        run_dbt(["deps"])
        run_dbt(["run"])

        results = run_dbt(
            ["build", "--select", "package:dbt_project_evaluator"], expect_pass=False
        )

        failed = [
            result.node.name
            for result in results
            if result.node.resource_type in ("model", "seed")
            and result.status != "success"
        ]
        assert not failed
        # The package ships far more than this; the bound only guards against
        # the selector silently matching nothing.
        assert len(results) > 30
