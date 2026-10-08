from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from threading import Barrier, get_ident

import psycopg
import pytest
from dbt.adapters.contracts.connection import ConnectionState
from dbt.tests.util import run_dbt
from dbt_common.events.contextvars import get_node_info
from dbt_common.exceptions import CompilationError, DbtDatabaseError, DbtInternalError

from dbt.adapters.motherduck.connections import MotherDuckConnectionManager


class TestConnectionReuseBuild:
    @pytest.fixture(scope="class")
    def models(self):
        models = {}
        for suffix in ("a", "b"):
            models[f"root_{suffix}.sql"] = (
                "{{ config(materialized='table') }} select id: 1"
            )
            models[f"incremental_{suffix}.sql"] = (
                "{{ config(materialized='incremental', unique_key='id') }} "
                f"select * from {{{{ ref('root_{suffix}') }}}}"
            )
            models[f"view_{suffix}.sql"] = (
                f"select * from {{{{ ref('incremental_{suffix}') }}}}"
            )
        return models

    @pytest.mark.parametrize("threads", [1, 2])
    def test_build_reuses_worker_connections(self, project, monkeypatch, threads):
        sessions = []
        original_release = MotherDuckConnectionManager.release

        def release_connection(manager):
            connection = manager.get_if_exists()
            if connection is not None and connection.state == ConnectionState.OPEN:
                sessions.append((manager, get_ident(), connection.handle))
            original_release(manager)

        project.adapter.cleanup_connections()
        previous_handles = []
        for _ in range(2):
            sessions.clear()
            # Observe real handles without replacing driver behavior. Reuse and
            # closing at invocation end are the contracts being checked.
            with monkeypatch.context() as patch:
                patch.setattr(
                    MotherDuckConnectionManager, "release", release_connection
                )
                assert len(run_dbt(["build", "--threads", str(threads)])) == 6
            by_thread = defaultdict(list)
            for manager, thread, handle in sessions:
                by_thread[manager, thread].append(handle)
            assert any(len(handles) > 1 for handles in by_thread.values())
            assert all(
                all(handle is handles[0] for handle in handles)
                for handles in by_thread.values()
            )
            assert all(handle.closed for _, _, handle in sessions)
            assert all(handle not in previous_handles for _, _, handle in sessions)
            previous_handles = [handle for _, _, handle in sessions]


def test_worker_sessions_are_reused_and_closed_at_cleanup(connection_adapter):
    adapter = connection_adapter
    ready = Barrier(2)

    def run_models(worker):
        handles = []
        for model in range(3):
            with adapter.connection_named(f"worker_{worker}_model_{model}"):
                _, table = adapter.execute(
                    "SELECT 42; SELECT 43", auto_begin=True, fetch=True
                )
                assert table.rows[0][0] == 43
                connection = adapter.connections.get_thread_connection()
                handles.append(connection.handle)
                if model == 0:
                    ready.wait(timeout=30)
                adapter.commit_if_has_connection()
            assert not connection.transaction_open
            assert (
                connection.handle.info.transaction_status
                == psycopg.pq.TransactionStatus.IDLE
            )
            assert (
                connection.handle.pgconn.pipeline_status
                == psycopg.pq.PipelineStatus.OFF
            )
        assert all(handle is handles[0] for handle in handles)
        return handles[0]

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run_models, worker) for worker in range(2)]
        handles = [future.result(timeout=60) for future in futures]
    assert handles[0] is not handles[1]
    assert all(not handle.closed for handle in handles)
    adapter.cleanup_connections()
    assert all(handle.closed for handle in handles)


def test_session_settings_persist_between_models(connection_adapter):
    adapter = connection_adapter
    with adapter.connection_named("set_timezone"):
        adapter.execute("SET TimeZone = 'Europe/Amsterdam'")
        handle = adapter.connections.get_thread_connection().handle
    with adapter.connection_named("read_timezone"):
        assert adapter.connections.get_thread_connection().handle is handle
        _, table = adapter.execute("SELECT current_setting('TimeZone')", fetch=True)
        assert table.rows[0][0] == "Europe/Amsterdam"


@pytest.mark.parametrize("fail", [False, True])
def test_release_rolls_back_unfinished_work(connection_adapter, fail):
    adapter = connection_adapter
    with adapter.connection_named("rollback_setup"):
        adapter.execute("CREATE TABLE main.release_probe (id INTEGER)")
    try:
        expected_error = (
            pytest.raises(CompilationError, match="intentional compilation failure")
            if fail
            else nullcontext()
        )
        with expected_error:
            with adapter.connection_named("unfinished_model"):
                adapter.execute(
                    "INSERT INTO main.release_probe VALUES (1)",
                    auto_begin=True,
                )
                connection = adapter.connections.get_thread_connection()
                handle = connection.handle
                assert connection.transaction_open
                assert (
                    handle.info.transaction_status
                    == psycopg.pq.TransactionStatus.INTRANS
                )
                if fail:
                    raise CompilationError("intentional compilation failure")
        assert not handle.closed
        assert not connection.transaction_open
        assert handle.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        with adapter.connection_named("next_model"):
            assert adapter.connections.get_thread_connection().handle is handle
            _, table = adapter.execute(
                "SELECT count(*) FROM main.release_probe", fetch=True
            )
            assert table.rows[0][0] == 0
    finally:
        with adapter.connection_named("rollback_cleanup"):
            adapter.execute("DROP TABLE main.release_probe")


@pytest.mark.parametrize(
    "transaction_sql, expected_rows, advice",
    [
        ("BEGIN", 0, "auto_begin=True"),
        ("COMMIT", 1, "adapter.commit()"),
        ("ROLLBACK", 0, "adapter.commit()"),
    ],
)
def test_release_rejects_raw_transaction_mismatches(
    connection_adapter, transaction_sql, expected_rows, advice
):
    adapter = connection_adapter
    with adapter.connection_named("transaction_mismatch_setup"):
        adapter.execute("CREATE TABLE main.transaction_mismatch_probe (id INTEGER)")
    try:
        with pytest.raises(DbtInternalError) as raised:
            with adapter.connection_named("raw_transaction"):
                if transaction_sql == "BEGIN":
                    adapter.execute(transaction_sql)
                adapter.execute(
                    "INSERT INTO main.transaction_mismatch_probe VALUES (1)",
                    auto_begin=transaction_sql != "BEGIN",
                )
                connection = adapter.connections.get_thread_connection()
                handle = connection.handle
                if transaction_sql != "BEGIN":
                    adapter.execute(transaction_sql)
                assert connection.transaction_open is (transaction_sql != "BEGIN")
                assert handle.info.transaction_status == (
                    psycopg.pq.TransactionStatus.INTRANS
                    if transaction_sql == "BEGIN"
                    else psycopg.pq.TransactionStatus.IDLE
                )
        assert advice in str(raised.value)
        assert "support@motherduck.com" in str(raised.value)
        assert handle.closed
        assert not connection.transaction_open
        assert adapter.connections.get_if_exists() is None
        with adapter.connection_named("transaction_mismatch_recovered"):
            _, table = adapter.execute(
                "SELECT count(*) FROM main.transaction_mismatch_probe", fetch=True
            )
            assert table.rows[0][0] == expected_rows
            assert adapter.connections.get_thread_connection().handle is not handle
    finally:
        with adapter.connection_named("transaction_mismatch_cleanup"):
            adapter.execute("DROP TABLE main.transaction_mismatch_probe")


class TestConnectionReuseJinjaFailure:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "failed_model.sql": """
{{ config(materialized='table', post_hook='{{ fail_after_write() }}') }}
select id: 1
"""
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "fail_after_write.sql": """
{% macro fail_after_write() %}
  {% if execute %}
    {% do run_query('insert into ' ~ this.schema ~ '.jinja_probe values (1)') %}
    {{ exceptions.raise_compiler_error('intentional Jinja failure after write') }}
  {% endif %}
{% endmacro %}
"""
        }

    def test_jinja_failure_rolls_back_before_final_cleanup(self, project, monkeypatch):
        released = []
        original_release = MotherDuckConnectionManager.release

        def release_connection(manager):
            connection = manager.get_if_exists()
            handle = None
            if (
                connection is not None
                and connection.state == ConnectionState.OPEN
                and get_node_info().get("node_name") == "failed_model"
            ):
                handle = connection.handle
            original_release(manager)
            if handle is not None:
                assert not handle.closed
                assert not connection.transaction_open
                assert (
                    handle.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
                )
                released.append(handle)

        monkeypatch.setattr(MotherDuckConnectionManager, "release", release_connection)
        project.run_sql(f"create table {project.test_schema}.jinja_probe (id integer)")
        results = run_dbt(["run", "--threads", "1"], expect_pass=False)
        assert "intentional Jinja failure after write" in results.results[0].message
        assert released
        assert all(handle.closed for handle in released)
        assert project.run_sql(
            f"select count(*) from {project.test_schema}.jinja_probe", fetch="one"
        ) == (0,)


def test_release_discards_session_when_rollback_fails(connection_adapter, monkeypatch):
    adapter = connection_adapter
    with adapter.connection_named("rollback_failure_setup"):
        adapter.execute("CREATE TABLE main.rollback_failure_probe (id INTEGER)")
    original_rollback = psycopg.Connection.rollback

    def fail_rollback(handle):
        if handle is failed_handle:
            raise psycopg.OperationalError("intentional rollback failure")
        return original_rollback(handle)

    try:
        with pytest.raises(DbtDatabaseError, match="intentional rollback failure"):
            with adapter.connection_named("rollback_failure"):
                adapter.execute(
                    "INSERT INTO main.rollback_failure_probe VALUES (1)",
                    auto_begin=True,
                )
                failed_handle = adapter.connections.get_thread_connection().handle
                monkeypatch.setattr(psycopg.Connection, "rollback", fail_rollback)
        assert failed_handle.closed
        assert adapter.connections.get_if_exists() is None
        with adapter.connection_named("rollback_failure_recovered"):
            _, table = adapter.execute(
                "SELECT count(*) FROM main.rollback_failure_probe", fetch=True
            )
            assert table.rows[0][0] == 0
            assert (
                adapter.connections.get_thread_connection().handle is not failed_handle
            )
    finally:
        with adapter.connection_named("rollback_failure_cleanup"):
            adapter.execute("DROP TABLE main.rollback_failure_probe")
