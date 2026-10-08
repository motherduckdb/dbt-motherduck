# Third-party package compatibility: dbt-labs/audit_helper.
#
# audit_helper is almost entirely `default__`, so most of it is expected to
# work. What it exercises that the adapter's own suite does not is relation and
# column comparison: set operators, full outer joins, and the column metadata
# query behind compare_relation_columns.
import pytest
from dbt.tests.util import run_dbt

LEFT_CSV = """id,team,amount
1,alpha,10
2,alpha,20
3,beta,30
"""

# Same shape and row count, but id 3 carries a different amount, so each
# comparison has one matching and one diverging row to report on.
RIGHT_CSV = """id,team,amount
1,alpha,10
2,alpha,20
3,beta,99
"""

COMPARE_RELATIONS_SQL = """
{{ config(materialized="table") }}
with comparison as (
    {{ audit_helper.compare_relations(
        a_relation=ref("audit_left"),
        b_relation=ref("audit_right"),
        primary_key="id",
    ) }}
)
select in_a, in_b, cast("count" as integer) as count_records
from comparison
"""

COMPARE_ROW_COUNTS_SQL = """
{{ config(materialized="table") }}
with comparison as (
    {{ audit_helper.compare_row_counts(
        a_relation=ref("audit_left"),
        b_relation=ref("audit_right"),
    ) }}
)
select cast(total_records as integer) as total_records from comparison
"""

COMPARE_RELATION_COLUMNS_SQL = """
{{ config(materialized="table") }}
with comparison as (
    {{ audit_helper.compare_relation_columns(
        a_relation=ref("audit_left"),
        b_relation=ref("audit_right"),
    ) }}
)
select column_name, has_ordinal_position_match, has_data_type_match, in_both
from comparison
"""

COMPARE_COLUMN_VALUES_SQL = """
{{ config(materialized="table") }}
with comparison as (
    {{ audit_helper.compare_column_values(
        a_query="select * from " ~ ref("audit_left"),
        b_query="select * from " ~ ref("audit_right"),
        primary_key="id",
        column_to_compare="amount",
        emojis=False,
    ) }}
)
select match_status, cast(count_records as integer) as count_records
from comparison
"""


class TestAuditHelper:
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"package": "dbt-labs/audit_helper", "version": [">=0.14.0", "<1.0.0"]}
            ]
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"audit_left.csv": LEFT_CSV, "audit_right.csv": RIGHT_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "audit_compare_relations.sql": COMPARE_RELATIONS_SQL,
            "audit_compare_row_counts.sql": COMPARE_ROW_COUNTS_SQL,
            "audit_compare_relation_columns.sql": COMPARE_RELATION_COLUMNS_SQL,
            "audit_compare_column_values.sql": COMPARE_COLUMN_VALUES_SQL,
        }

    @pytest.fixture(scope="class")
    def built(self, project):
        run_dbt(["deps"])
        assert len(run_dbt(["seed"])) == 2
        assert len(run_dbt(["run"])) == 4
        return project

    def test_compare_relations(self, built, project):
        rows = project.run_sql(
            f"select in_a, in_b, count_records "
            f"from {project.test_schema}.audit_compare_relations "
            f"order by in_a desc, in_b desc",
            fetch="all",
        )
        # Two rows match outright; id 3 appears once on each side only.
        assert [tuple(row) for row in rows] == [
            (True, True, 2),
            (True, False, 1),
            (False, True, 1),
        ]

    def test_compare_row_counts(self, built, project):
        rows = project.run_sql(
            f"select total_records from {project.test_schema}.audit_compare_row_counts",
            fetch="all",
        )
        assert [row[0] for row in rows] == [3, 3]

    def test_compare_relation_columns(self, built, project):
        rows = project.run_sql(
            f"select column_name, has_ordinal_position_match, has_data_type_match, "
            f"in_both from {project.test_schema}.audit_compare_relation_columns "
            f"order by column_name",
            fetch="all",
        )
        assert [row[0] for row in rows] == ["amount", "id", "team"]
        assert all(tuple(row[1:]) == (True, True, True) for row in rows)

    def test_compare_column_values(self, built, project):
        rows = project.run_sql(
            f"select match_status, count_records "
            f"from {project.test_schema}.audit_compare_column_values",
            fetch="all",
        )
        counts = {
            "match" if "perfect match" in status else "mismatch": count
            for status, count in rows
        }
        assert counts == {"match": 2, "mismatch": 1}


class TestAuditHelperUnsupported:
    """quick_are_queries_identical raises for any adapter it has no branch for.

    Kept in its own class because the error surfaces while the project is
    parsed, which would otherwise take the rest of the module's models with it.
    """

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"package": "dbt-labs/audit_helper", "version": [">=0.14.0", "<1.0.0"]}
            ]
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "audit_quick_identical.sql": """
{{ config(materialized="table") }}
{{ audit_helper.quick_are_queries_identical(
    query_a="select 1 as id",
    query_b="select 1 as id",
    columns=["id"],
) }}
"""
        }

    @pytest.mark.xfail(
        reason="default__quick_are_queries_identical raises a compiler error for "
        "any adapter without a dedicated branch; needs a motherduck__ "
        "implementation upstream",
        strict=True,
    )
    def test_quick_are_queries_identical(self, project):
        run_dbt(["deps"])
        run_dbt(["run"])
