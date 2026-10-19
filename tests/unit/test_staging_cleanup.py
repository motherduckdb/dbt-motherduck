from argparse import Namespace
from multiprocessing import get_context
from threading import Event
from time import monotonic

import psycopg
import pytest
from dbt.adapters.factory import AdapterContainer
from dbt.config.profile import Profile
from dbt.config.renderer import ProfileRenderer
from dbt.flags import set_from_args

from dbt.adapters.motherduck import impl
from dbt.adapters.motherduck.connections import MotherDuckConnectionManager


class FakeHandle:
    def __init__(self, execute=None):
        self.closed = Event()
        self.statements = []
        self._execute = execute

    def execute(self, sql):
        self.statements.append(sql)
        if self._execute is not None:
            self._execute(self)

    def close(self):
        self.closed.set()


@pytest.fixture
def adapter():
    set_from_args(Namespace(send_anonymous_usage_stats=False), None)
    profile = Profile.from_raw_profile_info(
        {
            "target": "test",
            "outputs": {
                "test": {
                    "type": "motherduck",
                    "host": "127.0.0.1",
                    "port": 1,
                    "database": "unused",
                    "schema": "main",
                    "token": "secret-token",
                    "threads": 1,
                }
            },
        },
        "motherduck_test",
        ProfileRenderer(),
    )
    container = AdapterContainer()
    container.load_plugin("motherduck")
    container.register_adapter(profile, get_context("spawn"))
    try:
        yield container.lookup_adapter("motherduck")
    finally:
        container.reset_adapters()


@pytest.fixture
def warnings(monkeypatch):
    messages = []

    class Logger:
        def __init__(self, name):
            pass

        def warning(self, message):
            messages.append(message)

    monkeypatch.setattr(impl, "AdapterLogger", Logger)
    return messages


def track(adapter, identifier):
    relation = adapter.Relation.create(
        database="analytics", schema="dbt_temp", identifier=identifier
    )
    adapter._staging_relations.add(relation)
    return relation


def use_handle(monkeypatch, handle):
    attached = []
    monkeypatch.setattr(
        MotherDuckConnectionManager, "_connect", staticmethod(lambda *_: handle)
    )
    monkeypatch.setattr(
        MotherDuckConnectionManager,
        "_attach_databases",
        staticmethod(lambda _, databases: attached.append(databases)),
    )
    return attached


def test_cleanup_drops_tracked_staging_after_cancellation(
    adapter, monkeypatch, warnings
):
    handle = FakeHandle()
    attached = use_handle(monkeypatch, handle)
    relation = track(adapter, "orders__abc")
    adapter.connections.invocation_canceled.set()

    adapter.cleanup_connections()

    assert handle.statements == [f"drop table if exists {relation}"]
    assert attached == [{"analytics"}]
    assert handle.closed.is_set()
    assert adapter._staging_relations == set()
    assert warnings == []


def test_cleanup_without_tracked_staging_opens_no_session(adapter, monkeypatch):
    def fail(*_):
        raise AssertionError("cleanup should not connect")

    monkeypatch.setattr(MotherDuckConnectionManager, "_connect", staticmethod(fail))
    adapter.cleanup_connections()


def test_failed_cleanup_warns_with_remaining_names(adapter, monkeypatch, warnings):
    def fail_second(handle):
        if len(handle.statements) == 2:
            raise psycopg.OperationalError("drop failed for secret-token")

    use_handle(monkeypatch, FakeHandle(fail_second))
    dropped = track(adapter, "a__abc")
    failed = track(adapter, "b__abc")
    untried = track(adapter, "c__abc")

    adapter.cleanup_connections()

    assert len(warnings) == 1
    assert str(dropped) not in warnings[0]
    assert str(failed) in warnings[0]
    assert str(untried) in warnings[0]
    assert "[REDACTED]" in warnings[0]
    assert "secret-token" not in warnings[0]


def test_cleanup_is_bounded_when_drop_hangs(adapter, monkeypatch, warnings):
    def hang(handle):
        # Like psycopg, a query blocked on the server fails once the session closes.
        assert handle.closed.wait(timeout=10)
        raise psycopg.OperationalError("the connection is closed")

    monkeypatch.setattr(impl, "STAGING_CLEANUP_TIMEOUT_SECONDS", 0.2)
    use_handle(monkeypatch, FakeHandle(hang))
    relation = track(adapter, "orders__abc")

    started = monotonic()
    adapter.cleanup_connections()

    assert monotonic() - started < 5
    assert len(warnings) == 1
    assert "timed out" in warnings[0]
    assert str(relation) in warnings[0]
