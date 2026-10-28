import json
import os
import signal
import subprocess
import sys
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import get_context
from pathlib import Path
from queue import Queue
from threading import Event, Thread
from time import monotonic, sleep

import psycopg
import pytest
from dbt.adapters.factory import FACTORY, AdapterContainer
from dbt.cli.main import dbtRunner
from dbt.config.profile import Profile
from dbt.config.renderer import ProfileRenderer
from dbt.flags import set_from_args
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from psycopg import sql as pg_sql


@pytest.mark.parametrize(
    "pipeline, opening", [(False, False), (True, False), (False, True)]
)
def test_cancellation_disconnects_workers_and_rejects_later_work(
    profile_data, pipeline, opening
):
    # A stuck driver or worker must fail this test, not hang the entire test suite.
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from tests.integration.test_cancellation import _exercise_shutdown; "
            "_exercise_shutdown()",
        ],
        input=json.dumps(
            {"profile": profile_data, "pipeline": pipeline, "opening": opening}
        ),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


class TestCancelledBuild:
    @pytest.fixture(scope="class")
    def models(self):
        sql = """
{% set slow_query = 'select total: sum(sin(i)) from range(1000000000) t(i) /* md_cancel_probe */' %}
{{ config(materialized='incremental', unique_key='id',
          pre_hook=('select first: 1; ' ~ slow_query ~ '; select last: 2')
                   if var('slow', false) and var('pipeline', false) else []) }}
{% if var('slow', false) and not var('pipeline', false) %}
select id: 1, value: sum(sin(i)) from range(1000000000) t(i) /* md_cancel_probe */
{% else %}
select id: 1, value: {{ var('value', 1) }}
{% endif %}
"""
        return {f"incremental_{name}.sql": sql for name in ("a", "b")}

    @pytest.mark.parametrize("pipeline", [False, True])
    def test_sigint_returns_and_next_invocation_succeeds(self, project, pipeline):
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from tests.integration.test_cancellation import _exercise_dbt_cancellation; "
                "_exercise_dbt_cancellation()",
            ],
            input=json.dumps(
                {
                    "project": str(project.project_root),
                    "profiles": str(project.profiles_dir),
                    "schema": project.test_schema,
                    "pipeline": pipeline,
                }
            ),
            cwd=Path(__file__).resolve().parents[2],
            env={**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"},
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        for name in ("a", "b"):
            relation = project.adapter.Relation.create(
                database=project.database,
                schema=project.test_schema,
                identifier=f"incremental_{name}",
            )
            assert project.run_sql(
                f"select id, value from {relation}", fetch="all"
            ) == [(1, 2)]


def _exercise_shutdown():
    data = json.load(sys.stdin)
    set_from_args(Namespace(send_anonymous_usage_stats=False), None)
    profile = Profile.from_raw_profile_info(
        {"target": "test", "outputs": {"test": data["profile"]}},
        "motherduck_test",
        ProfileRenderer(),
    )
    container = AdapterContainer()
    container.load_plugin("motherduck")
    container.register_adapter(profile, get_context("spawn"))
    adapter = container.lookup_adapter("motherduck")
    connections = Queue()
    proceed = Event()
    slow_query = "select total: sum(sin(i)) from range(1000000000) t(i)"
    sql = (
        f"select first: 1; {slow_query}; select last: 2"
        if data["pipeline"]
        else slow_query
    )

    def active_worker():
        with adapter.connection_named("active_worker"):
            adapter.execute("select 1")
            handle = adapter.connections.get_thread_connection().handle
            connections.put(handle)
            with pytest.raises(DbtDatabaseError):
                adapter.execute(sql, auto_begin=True, fetch=True)
        assert handle.closed

    def late_worker():
        if data["opening"]:
            with (
                adapter.connection_named("late_worker"),
                pytest.raises(DbtRuntimeError, match="invocation was cancelled"),
            ):
                adapter.connections.open(adapter.connections.get_thread_connection())
            return
        with adapter.connection_named("late_worker"):
            adapter.execute("select 1")
            handle = adapter.connections.get_thread_connection().handle
            connections.put(handle)
            assert proceed.wait(timeout=30)
            with pytest.raises(DbtRuntimeError, match="invocation was cancelled"):
                adapter.execute("select 2")
        assert handle.closed
        with (
            adapter.connection_named("late_reconnect"),
            pytest.raises(DbtRuntimeError, match="invocation was cancelled"),
        ):
            adapter.connections.open(adapter.connections.get_thread_connection())

    original_connect = psycopg.connect

    def paused_connect(*args, **kwargs):
        # Keep the real handshake and session. Only gate the return to reproduce
        # a successful connection completing after the cancellation sweep.
        handle = original_connect(*args, **kwargs)
        connections.put(handle)
        assert proceed.wait(timeout=30)
        return handle

    try:
        with adapter.connection_named("controller"):
            adapter.execute("select 1")
            with ThreadPoolExecutor(max_workers=2) as executor:
                try:
                    active = executor.submit(active_worker)
                    handle = connections.get(timeout=30)
                    with pytest.MonkeyPatch.context() as patch:
                        if data["opening"]:
                            patch.setattr(psycopg, "connect", paused_connect)
                        late = executor.submit(late_worker)
                        late_handle = connections.get(timeout=30)
                        deadline = monotonic() + 10
                        while (
                            handle.info.transaction_status
                            != psycopg.pq.TransactionStatus.ACTIVE
                        ):
                            assert not active.done(), (
                                "Query finished before cancellation"
                            )
                            assert monotonic() < deadline, "Query did not become active"
                            sleep(0.01)
                        assert set(adapter.connections.cancel_open()) == {
                            "active_worker",
                            "late_worker",
                        }
                        proceed.set()
                        active.result(timeout=10)
                        late.result(timeout=10)
                        assert late_handle.closed
                    with pytest.raises(
                        DbtRuntimeError, match="invocation was cancelled"
                    ):
                        adapter.execute("select 3")
                finally:
                    proceed.set()
    finally:
        container.reset_adapters()


def _exercise_dbt_cancellation():
    data = json.load(sys.stdin)
    args = [
        "run",
        "--project-dir",
        data["project"],
        "--profiles-dir",
        data["profiles"],
        "--threads",
        "2",
    ]
    assert dbtRunner().invoke([*args, "--full-refresh"]).success
    query = Queue(maxsize=1)
    signalled = Event()
    finished = Event()
    elapsed = []

    def observe(event):
        if (
            event.info.name == "SQLQuery"
            and "md_cancel_probe" in event.data.sql
            and not signalled.is_set()
        ):
            signalled.set()
            manager = FACTORY.lookup_adapter("motherduck").connections
            query.put(manager.get_thread_connection().handle)

    def interrupt():
        handle = query.get(timeout=60)
        deadline = monotonic() + 10
        while handle.info.transaction_status != psycopg.pq.TransactionStatus.ACTIVE:
            assert not finished.is_set(), "dbt finished before interruption"
            assert monotonic() < deadline, "Query did not become active"
            sleep(0.01)
        elapsed.append(monotonic())
        os.kill(os.getpid(), signal.SIGINT)

    interrupter = Thread(target=interrupt, daemon=True)
    interrupter.start()
    try:
        result = dbtRunner(callbacks=[observe]).invoke(
            [*args, "--vars", json.dumps({"slow": True, "pipeline": data["pipeline"]})]
        )
    finally:
        finished.set()
    interrupter.join(timeout=10)
    assert not interrupter.is_alive(), "Interruption thread did not finish"
    assert elapsed, "SIGINT was not sent"
    shutdown_seconds = monotonic() - elapsed[0]
    assert shutdown_seconds < 10, "dbt did not return promptly after SIGINT"
    assert not result.success
    assert isinstance(result.exception, KeyboardInterrupt), repr(result.exception)
    # Verify committed targets independently of the cancelled adapter.
    credentials = FACTORY.lookup_adapter("motherduck").config.credentials
    with psycopg.connect(
        host=credentials.host,
        port=credentials.port,
        user="postgres",
        password=credentials.token,
        dbname=credentials.database,
        sslmode="verify-full",
        sslrootcert=credentials.resolved_sslrootcert,
        connect_timeout=credentials.connect_timeout,
        options="-c attach_mode=single",
        autocommit=True,
    ) as observer:
        for name in ("a", "b"):
            relation = pg_sql.Identifier(
                credentials.database, data["schema"], f"incremental_{name}"
            )
            assert observer.execute(
                pg_sql.SQL("select id, value from {}").format(relation)
            ).fetchall() == [(1, 1)]
    print(f"Cancellation returned after {shutdown_seconds:.3f}s", flush=True)
    # The Python host survives cancellation, and a new invocation is not poisoned.
    assert dbtRunner().invoke([*args, "--vars", json.dumps({"value": 2})]).success
