# Third-party package compatibility: dbt-labs/dbt_utils.
#
# The cross-database macros dbt_utils re-exports (dateadd, datediff, split_part,
# ...) forward to the `dbt` namespace, so tests/integration/test_utils.py already
# covers them through dbt-tests-adapter. What is exercised here is the part that
# stays in the `dbt_utils` namespace: SQL generators, the introspection macros
# that run queries while a model is being rendered, and the generic tests.
#
# No `dispatch` config is set and the adapter ships no package shims, so
# dispatch resolves `motherduck__` and then `default__`. That is exactly what a
# user gets from `dbt deps` plus a run, which is the behaviour worth measuring.
import pytest
from dbt.tests.util import run_dbt

ORDERS_CSV = """id,status,amount,lower_bound,upper_bound,url
1,new,190000,0,10,https://example.com/orders/1?utm_source=email&utm_medium=campaign
2,new,290000,10,20,http://example.com/orders/2?utm_source=search
3,paid,400000,20,30,android-app://example.app/orders/3
"""

ARCHIVE_CSV = """id,status,amount,lower_bound,upper_bound,url
4,paid,500000,30,40,https://example.com/orders/4
"""

STATUSES_CSV = """status
new
new
paid
"""

STAR_SQL = """
{{ config(materialized="table") }}
select {{ dbt_utils.star(from=ref("utils_orders"), except=["amount", "url"]) }}
from {{ ref("utils_orders") }}
"""

SURROGATE_KEY_SQL = """
{{ config(materialized="table") }}
select
    id,
    {{ dbt_utils.generate_surrogate_key(["id", "status"]) }} as order_key
from {{ ref("utils_orders") }}
"""

DEDUPLICATE_SQL = """
{{ config(materialized="table") }}
with duplicated as (
    select id, status, 1 as row_priority from {{ ref("utils_orders") }}
    union all
    select id, status, 2 as row_priority from {{ ref("utils_orders") }}
)
{{ dbt_utils.deduplicate(
    relation="duplicated",
    partition_by="id",
    order_by="row_priority asc",
) }}
"""

UNION_SQL = """
{{ config(materialized="table") }}
{{ dbt_utils.union_relations(
    relations=[ref("utils_orders"), ref("utils_orders_archive")],
) }}
"""

UNPIVOT_SQL = """
{{ config(materialized="table") }}
{{ dbt_utils.unpivot(
    relation=ref("utils_orders"),
    cast_to="varchar",
    exclude=["id"],
    remove=["status", "url"],
    field_name="metric",
    value_name="metric_value",
) }}
"""

# width_bucket's default__ implementation returns a double, so pin the type here
# rather than asserting against whatever the driver maps it to.
WIDTH_BUCKET_SQL = """
{{ config(materialized="table") }}
select
    id,
    cast({{ dbt_utils.width_bucket("amount", 200000, 600000, 4) }} as integer) as amount_bucket
from {{ ref("utils_orders") }}
"""

URLS_SQL = """
{{ config(materialized="table") }}
select
    id,
    {{ dbt_utils.get_url_host("url") }} as url_host,
    {{ dbt_utils.get_url_path("url") }} as url_path,
    {{ dbt_utils.get_url_parameter("url", "utm_source") }} as utm_source,
    {{ dbt_utils.get_url_parameter("url", "utm_medium") }} as utm_medium
from {{ ref("utils_orders") }}
"""

# Both of these run a query against information_schema while the model renders.
RELATIONS_SQL = """
{{ config(materialized="table") }}
{% set by_pattern = dbt_utils.get_relations_by_pattern(
    schema_pattern=target.schema, table_pattern="utils_%"
) %}
{% set by_prefix = dbt_utils.get_relations_by_prefix(
    schema=target.schema, prefix="utils_"
) %}
select
    {{ by_pattern | length }} as pattern_count,
    {{ by_prefix | length }} as prefix_count
"""

INTROSPECTION_SQL = """
{{ config(materialized="table") }}
{% set statuses = dbt_utils.get_column_values(
    ref("utils_orders"), "status", order_by="status", default=[]
) %}
{% set total = dbt_utils.get_single_value(
    "select sum(amount) from " ~ ref("utils_orders"), default=0
) %}
select
    {{ statuses | length }} as status_count,
    '{{ statuses | join(",") }}' as statuses,
    cast({{ total }} as bigint) as total_amount
"""

SCHEMA_YML = """
version: 2

seeds:
  - name: utils_orders
    data_tests:
      - dbt_utils.equal_rowcount:
          arguments:
            compare_model: ref('utils_statuses')
      - dbt_utils.expression_is_true:
          arguments:
            expression: "upper_bound > lower_bound"
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: ['id', 'status']
      - dbt_utils.mutually_exclusive_ranges:
          arguments:
            lower_bound_column: lower_bound
            upper_bound_column: upper_bound
            gaps: not_allowed
    columns:
      - name: id
        data_tests:
          - dbt_utils.sequential_values:
              arguments:
                interval: 1
          - dbt_utils.accepted_range:
              arguments:
                min_value: 1
                max_value: 3
      - name: status
        data_tests:
          - dbt_utils.not_constant
          - dbt_utils.at_least_one
          - dbt_utils.not_empty_string
          - dbt_utils.not_null_proportion:
              arguments:
                at_least: 1.0
          - dbt_utils.not_accepted_values:
              arguments:
                values: ['cancelled']
          - dbt_utils.cardinality_equality:
              arguments:
                to: ref('utils_statuses')
                field: status
          - dbt_utils.relationships_where:
              arguments:
                to: ref('utils_statuses')
                field: status

  - name: utils_orders_archive
    data_tests:
      - dbt_utils.fewer_rows_than:
          arguments:
            compare_model: ref('utils_orders')
"""


class TestDbtUtils:
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"package": "dbt-labs/dbt_utils", "version": [">=1.4.0", "<2.0.0"]}
            ]
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "utils_orders.csv": ORDERS_CSV,
            "utils_orders_archive.csv": ARCHIVE_CSV,
            "utils_statuses.csv": STATUSES_CSV,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "utils_star.sql": STAR_SQL,
            "utils_surrogate_key.sql": SURROGATE_KEY_SQL,
            "utils_deduplicate.sql": DEDUPLICATE_SQL,
            "utils_union.sql": UNION_SQL,
            "utils_unpivot.sql": UNPIVOT_SQL,
            "utils_width_bucket.sql": WIDTH_BUCKET_SQL,
            "utils_urls.sql": URLS_SQL,
            "utils_relations.sql": RELATIONS_SQL,
            "utils_introspection.sql": INTROSPECTION_SQL,
            "schema.yml": SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def built(self, project):
        run_dbt(["deps"])
        assert len(run_dbt(["seed"])) == 3
        assert len(run_dbt(["run"])) == 9
        return project

    def query(self, project, sql):
        return project.run_sql(sql, fetch="all")

    def test_generic_tests(self, built):
        results = run_dbt(["test"])
        failed = [r.node.name for r in results if r.status != "pass"]
        assert not failed
        # Guards against a rename upstream silently dropping coverage here.
        assert len(results) == 14

    def test_star_excludes_columns(self, built, project):
        rows = self.query(
            project, f"select * from {project.test_schema}.utils_star order by id"
        )
        assert [tuple(row) for row in rows] == [
            (1, "new", 0, 10),
            (2, "new", 10, 20),
            (3, "paid", 20, 30),
        ]

    def test_generate_surrogate_key(self, built, project):
        rows = self.query(
            project,
            f"select order_key from {project.test_schema}.utils_surrogate_key",
        )
        keys = {row[0] for row in rows}
        assert len(keys) == 3
        assert all(len(key) == 32 for key in keys)

    def test_deduplicate(self, built, project):
        rows = self.query(
            project,
            f"select id, row_priority from {project.test_schema}.utils_deduplicate "
            f"order by id",
        )
        assert [tuple(row) for row in rows] == [(1, 1), (2, 1), (3, 1)]

    def test_union_relations(self, built, project):
        rows = self.query(
            project,
            f"select count(*), count(distinct _dbt_source_relation) "
            f"from {project.test_schema}.utils_union",
        )
        assert tuple(rows[0]) == (4, 2)

    def test_unpivot(self, built, project):
        rows = self.query(
            project,
            f"select metric, count(*) from {project.test_schema}.utils_unpivot "
            f"group by metric order by metric",
        )
        assert [tuple(row) for row in rows] == [
            ("amount", 3),
            ("lower_bound", 3),
            ("upper_bound", 3),
        ]

    def test_width_bucket(self, built, project):
        rows = self.query(
            project,
            f"select amount_bucket from {project.test_schema}.utils_width_bucket "
            f"order by id",
        )
        assert [row[0] for row in rows] == [0, 1, 3]

    def test_url_macros(self, built, project):
        rows = self.query(
            project,
            f"select url_host, url_path, utm_source, utm_medium "
            f"from {project.test_schema}.utils_urls order by id",
        )
        # default__get_url_path only strips http(s)://, so the android-app scheme
        # survives into the path. That is upstream behaviour, not an adapter bug.
        assert [tuple(row) for row in rows] == [
            ("example.com", "orders/1", "email", "campaign"),
            ("example.com", "orders/2", "search", None),
            ("example.app", "/example.app/orders/3", None, None),
        ]

    def test_relation_discovery(self, built, project):
        rows = self.query(
            project,
            f"select pattern_count, prefix_count "
            f"from {project.test_schema}.utils_relations",
        )
        pattern_count, prefix_count = rows[0]
        # The three seeds are built before any model runs; model build order is
        # not guaranteed, so only the lower bound is stable.
        assert pattern_count >= 3
        assert prefix_count >= 3

    def test_introspection_macros(self, built, project):
        rows = self.query(
            project,
            f"select status_count, statuses, total_amount "
            f"from {project.test_schema}.utils_introspection",
        )
        assert tuple(rows[0]) == (2, "new,paid", 880000)
