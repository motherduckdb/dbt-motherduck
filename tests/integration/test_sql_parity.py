import pytest
from dbt.tests.util import relation_from_name, run_dbt


class TestContracts:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "typed.sql": """
{{ config(materialized=var('kind'), unique_key='id', on_schema_change='fail', contract={'enforced': true}) }}
select 1 as id, {{ "42" if var('wrong_type', false) else "'value'" }} as label, 3 as "order id"
""",
            "schema.yml": """
version: 2
models:
  - name: typed
    columns:
      - name: id
        data_type: integer
      - name: label
        data_type: varchar
      - name: order id
        data_type: integer
        quote: true
""",
        }

    @pytest.mark.parametrize("kind", ["table", "view", "incremental"])
    def test_contract_and_mismatch(self, project, kind):
        args = ["run", "--vars", f"{{kind: {kind}}}"]
        run_dbt([*args, "--full-refresh"])
        run_dbt(args)
        target = relation_from_name(project.adapter, "typed")
        assert project.run_sql(f"SELECT * FROM {target}", fetch="all") == [
            (1, "value", 3)
        ]
        result = run_dbt(
            ["run", "--full-refresh", "--vars", f"{{kind: {kind}, wrong_type: true}}"],
            expect_pass=False,
        )
        assert "contract" in result.results[0].message.lower()
        assert project.run_sql(f"SELECT * FROM {target}", fetch="all") == [
            (1, "value", 3)
        ]


@pytest.mark.skip_database_type("ducklake", reason="DuckLake does not support indexes")
class TestIndexes:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "indexed.sql": """
{{ config(materialized=var('kind'), unique_key='id', indexes=[{'columns': ['id']}]) }}
select 1 as id
"""
        }

    @pytest.mark.parametrize("kind", ["table", "incremental"])
    def test_indexes_survive_replacement(self, project, kind):
        for flags in (["--full-refresh"], [], ["--full-refresh"]):
            run_dbt(["run", "--vars", f"{{kind: {kind}}}", *flags])
            assert project.run_sql(
                "SELECT count(*) FROM duckdb_indexes() "
                f"WHERE database_name = '{project.database}' "
                f"AND schema_name = '{project.test_schema}' AND table_name = 'indexed'",
                fetch="one",
            ) == (1,)


@pytest.mark.skip_database_type(
    "native", reason="Partitioning/sorting requires DuckLake"
)
class TestDuckLakeLayout:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "layout.sql": """
{{ config(materialized=var('kind'), unique_key='id',
          meta={'partitioned_by': 'day(ts)', 'sorted_by': ['id']}) }}
select case when {{ 'true' if var('fail', false) else 'false' }} and i = 1
            then error('intentional layout failure') else i end as id,
       timestamp '2025-01-01' as ts
from range(1, 2) t(i)
"""
        }

    @pytest.mark.parametrize("kind", ["table", "incremental"])
    def test_layout_build_and_replace(self, project, kind):
        target = relation_from_name(project.adapter, "layout")
        project.run_sql(f"DROP TABLE IF EXISTS {target}")
        result = run_dbt(
            ["run", "--vars", f"{{kind: {kind}, fail: true}}"], expect_pass=False
        )
        assert "intentional layout failure" in result.results[0].message
        assert project.run_sql(
            "SELECT count(*) FROM information_schema.tables "
            f"WHERE table_catalog = '{project.database}' AND table_schema = '{project.test_schema}' "
            "AND table_name IN ('layout', 'layout__dbt_tmp')",
            fetch="one",
        ) == (0,)
        for flags in (["--full-refresh"], [], ["--full-refresh"]):
            events = []
            run_dbt(
                ["run", "--vars", f"{{kind: {kind}}}", *flags],
                callbacks=[events.append],
            )
            if kind == "table" or flags:
                statements = "\n".join(
                    event.data.sql for event in events if event.info.name == "SQLQuery"
                ).lower()
                assert "set partitioned by (day(ts))" in statements
                assert 'set sorted by ("id")' in statements
            target = relation_from_name(project.adapter, "layout")
            assert project.run_sql(f"SELECT id FROM {target}", fetch="all") == [(1,)]


class TestHooks:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hooked.sql": """
{{ config(materialized='table',
    persist_docs={'relation': true, 'columns': true},
    pre_hook=[{'sql': 'create table if not exists ' ~ this.schema ~ '.hook_log (value integer)', 'transaction': false},
              'insert into ' ~ this.schema ~ '.hook_log values (1)'],
    post_hook=['insert into ' ~ this.schema ~ '.hook_log values (2)',
               {'sql': 'insert into ' ~ this.schema ~ '.hook_log values (3)', 'transaction': false}]) }}
select 1 as id
""",
            "schema.yml": """
version: 2
models:
  - name: hooked
    description: "Table's comment; with punctuation"
    columns:
      - name: id
        description: "Column's comment; with punctuation"
""",
        }

    def test_hook_transactions(self, project):
        run_dbt(["run"])
        target = relation_from_name(project.adapter, "hook_log")
        assert project.run_sql(
            f"SELECT value FROM {target} ORDER BY value", fetch="all"
        ) == [(1,), (2,), (3,)]
        assert project.run_sql(
            "SELECT comment FROM duckdb_tables() "
            f"WHERE database_name = '{project.database}' AND schema_name = '{project.test_schema}' "
            "AND table_name = 'hooked'",
            fetch="one",
        ) == ("Table's comment; with punctuation",)
        assert project.run_sql(
            "SELECT comment FROM duckdb_columns() "
            f"WHERE database_name = '{project.database}' AND schema_name = '{project.test_schema}' "
            "AND table_name = 'hooked' AND column_name = 'id'",
            fetch="one",
        ) == ("Column's comment; with punctuation",)


class TestSnapshotRollback:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "tracked.sql": """
{% snapshot tracked %}
{{ config(unique_key='id', strategy='check', check_cols=['value'],
          pre_hook='insert into ' ~ this.schema ~ '.hook_log values (1)',
          post_hook="select error('intentional snapshot failure')" if var('fail', false) else []) }}
select 1 as id, {{ var('value', 1) }} as value
{% endsnapshot %}
"""
        }

    def test_snapshot_failure_rolls_back_hooks_and_cleans_staging(self, project):
        log = relation_from_name(project.adapter, "hook_log")
        project.run_sql(f"CREATE TABLE {log} (value INTEGER)")
        run_dbt(["snapshot"])
        result = run_dbt(
            ["snapshot", "--vars", "{value: 2, fail: true}"], expect_pass=False
        )
        assert "intentional snapshot failure" in result.results[0].message
        assert project.run_sql(f"SELECT count(*) FROM {log}", fetch="one") == (1,)
        target = relation_from_name(project.adapter, "tracked")
        assert project.run_sql(f"SELECT value FROM {target}", fetch="all") == [(1,)]
        assert (
            project.run_sql(
                "SELECT table_name FROM information_schema.tables "
                f"WHERE table_catalog = '{project.database}' AND table_schema = 'dbt_temp' "
                "AND table_name LIKE 'tracked__%'",
                fetch="all",
            )
            == []
        )
        run_dbt(["snapshot", "--vars", "{value: 2}"])
        assert project.run_sql(
            f"SELECT value FROM {target} WHERE dbt_valid_to IS NULL", fetch="all"
        ) == [(2,)]
