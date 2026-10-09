# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/plugins/motherduck/test_motherduck_ducklake.py and
# tests/functional/adapter/test_database_type.py.
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from dbt.tests.util import get_connection, run_dbt
from dbt_common.events import get_event_manager


class TestNonDefaultDatabaseType:
    @pytest.fixture(scope="class")
    def other_database(self, temporary_database, database_type):
        kind = "native" if database_type == "ducklake" else "ducklake"
        with temporary_database(kind=kind) as name:
            yield name

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "layout.sql": """
{{ config(materialized='table', database=var('output_database'),
          meta={'partitioned_by': 'ds', 'sorted_by': 'id'}) }}
select id: 1, ds: 2
""",
        }

    def test_is_ducklake_for_non_default_database(
        self, connection_adapter, database_type, other_database
    ):
        adapter = connection_adapter
        adapter.config.credentials.attach_databases = [other_database]
        primary = adapter.Relation.create(
            database=adapter.config.credentials.database,
            schema="main",
            identifier="type_probe",
        )
        target = primary.incorporate(path={"database": other_database})
        with get_connection(adapter):
            adapter.execute(f"create table {target} as select id: 1")
            _, current = adapter.execute("select current_database()", fetch=True)
            assert current.rows[0][0] == primary.database

        ready = Barrier(2)

        def lookup(worker):
            with adapter.connection_named(f"database_type_{worker}"):
                _ = adapter.connections.get_thread_connection().handle
                ready.wait(timeout=30)
                return adapter.is_ducklake(primary), adapter.is_ducklake(target)

        events = []
        event_manager = get_event_manager()
        event_manager.add_callback(events.append)
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                assert (
                    list(executor.map(lookup, range(2)))
                    == [(database_type == "ducklake", database_type == "native")] * 2
                )
            assert sum(event.info.name == "SQLQuery" for event in events) <= 2

            events.clear()
            for relation, expected in (
                (primary, database_type == "ducklake"),
                (target, database_type == "native"),
            ):
                assert adapter.is_ducklake(relation) is expected
                differently_cased = relation.incorporate(
                    path={"database": relation.database.upper()}
                )
                assert adapter.is_ducklake(differently_cased) is expected
            assert not any(event.info.name == "SQLQuery" for event in events)
        finally:
            event_manager.callbacks.remove(events.append)

    def test_unattached_database_type_is_not_cached(
        self, connection_adapter, database_type, other_database
    ):
        adapter = connection_adapter
        relation = adapter.Relation.create(database=other_database)
        with get_connection(adapter):
            assert adapter.is_ducklake(relation) is False
            adapter.execute(f"attach 'md:{other_database}'")
            assert adapter.is_ducklake(relation) is (database_type == "native")

    def test_layout_and_drop_follow_target_database_type(
        self, project, database_type, other_database
    ):
        target_is_ducklake = database_type == "native"
        for _ in range(2):
            events = []
            results = run_dbt(
                ["run", "--vars", json.dumps({"output_database": other_database})],
                callbacks=[events.append],
            )
            assert len(results) == 1
            assert results[0].node.database == other_database
            statements = "\n".join(
                event.data.sql.lower()
                for event in events
                if event.info.name == "SQLQuery"
            )
            assert ("set partitioned by (ds)" in statements) == target_is_ducklake
            assert ('set sorted by ("id")' in statements) == target_is_ducklake

        adapter = project.adapter
        target = adapter.Relation.create(
            database=other_database,
            schema=project.test_schema,
            identifier="layout",
            type="table",
        )
        assert project.run_sql(f"select * from {target}", fetch="all") == [(1, 2)]
        with get_connection(adapter) as connection:
            dependent = target.incorporate(path={"identifier": "dependent"})
            if not target_is_ducklake:
                # Dependency tracking makes a missing CASCADE observable on native targets.
                connection.handle.execute("set enable_view_dependencies = true")
                connection.handle.execute(
                    f"create view {dependent} as select * from {target}"
                )
            adapter.drop_relation(target)
            _, tables = adapter.execute(
                "select count(*) from duckdb_tables() "
                f"where database_name = '{other_database}' "
                f"and schema_name = '{project.test_schema}' and table_name = 'layout'",
                fetch=True,
            )
            assert tables.rows[0][0] == 0
            if not target_is_ducklake:
                _, views = adapter.execute(
                    "select count(*) from duckdb_views() "
                    f"where database_name = '{other_database}' "
                    f"and schema_name = '{project.test_schema}' and view_name = 'dependent'",
                    fetch=True,
                )
                assert views.rows[0][0] == 0
