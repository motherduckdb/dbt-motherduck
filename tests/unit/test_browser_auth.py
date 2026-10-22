import base64
import contextlib
import http.client
import io
import json
import multiprocessing
import stat
import threading
import time
from types import SimpleNamespace

import psycopg
import pytest
from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.exceptions import FailedToConnectError

from dbt.adapters.motherduck import browser_auth
from dbt.adapters.motherduck.browser_auth import (
    BrowserAuthenticator,
    BrowserAuthError,
    token_expired,
)
from dbt.adapters.motherduck.connections import MotherDuckConnectionManager
from dbt.adapters.motherduck.credentials import MotherDuckCredentials

DEVICE = {
    "device_code": "device-123",
    "user_code": "ABCD-EFGH",
    "verification_uri_complete": "https://auth.motherduck.com/activate?user_code=ABCD-EFGH",
    "expires_in": 600,
    "interval": 5,
}


def jwt(expires_at):
    payload = base64.urlsafe_b64encode(json.dumps({"exp": expires_at}).encode())
    return f"header.{payload.decode().rstrip('=')}.signature"


VALID_TOKEN = jwt(time.time() + 86400)
EXPIRED_TOKEN = jwt(time.time() - 60)


@pytest.fixture(autouse=True)
def motherduck_home(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTHERDUCK_HOME", str(tmp_path / "home"))
    return tmp_path / "home"


def credentials_file(home):
    return home / "credentials.json"


def write_saved_token(home, token):
    home.mkdir(parents=True, exist_ok=True)
    credentials_file(home).write_text(
        json.dumps({"version": 1, "motherduck_token": token})
    )


class FakeServer:
    """Replays scripted responses per URL and records each request."""

    def __init__(self, token_responses, lookup_responses=None):
        self.responses = {
            browser_auth.OAUTH_DEVICE_CODE_URL: [(200, DEVICE)],
            browser_auth.OAUTH_TOKEN_URL: list(token_responses),
            f"{browser_auth.MOTHERDUCK_API_ORIGIN}/mom/lookup-user": list(
                lookup_responses or [(200, {"token": VALID_TOKEN})]
            ),
        }
        self.requests = []

    def __call__(self, url, *, data=None, headers=None):
        self.requests.append((url, data, headers))
        response = self.responses[url].pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def count(self, url):
        return sum(1 for requested, _, _ in self.requests if requested == url)


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds

    def __call__(self):
        return self.now


def make_authenticator(server, opened=None, **kwargs):
    clock = FakeClock()
    output = io.StringIO()
    authenticator = BrowserAuthenticator(
        request=server,
        open_browser=(opened.append if opened is not None else lambda url: None),
        can_open_browser=kwargs.pop("can_open_browser", lambda: True),
        sleep=kwargs.pop("sleep", clock.sleep),
        **kwargs,
        clock=clock,
        output=output,
    )
    return authenticator, clock, output


def test_saved_token_is_reused_without_login(motherduck_home):
    write_saved_token(motherduck_home, VALID_TOKEN)
    server = FakeServer([])
    authenticator, _, _ = make_authenticator(server)
    assert authenticator.get_token() == VALID_TOKEN
    assert server.requests == []


def test_device_flow_logs_in_and_saves_token(motherduck_home):
    server = FakeServer(
        [
            (403, {"error": "authorization_pending"}),
            (429, None),
            (403, {"error": "slow_down"}),
            OSError("network blip"),
            (200, {"id_token": "id-token"}),
        ]
    )
    opened = []
    authenticator, clock, output = make_authenticator(server, opened)

    assert authenticator.get_token() == VALID_TOKEN

    assert opened == [DEVICE["verification_uri_complete"]]
    assert DEVICE["user_code"] in output.getvalue()
    assert DEVICE["verification_uri_complete"] in output.getvalue()
    # 429 and slow_down each back off by a second.
    assert clock.sleeps == [5, 5, 6, 7, 7]
    lookup_url, _, headers = server.requests[-1]
    assert lookup_url.endswith("/mom/lookup-user")
    assert headers == {"Authorization": "Bearer id-token"}
    path = credentials_file(motherduck_home)
    assert json.loads(path.read_text()) == {
        "version": 1,
        "motherduck_token": VALID_TOKEN,
    }
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    # Later calls in the same process reuse the token.
    assert authenticator.get_token() == VALID_TOKEN
    assert server.count(browser_auth.OAUTH_DEVICE_CODE_URL) == 1


@pytest.mark.parametrize(
    "saved",
    [
        EXPIRED_TOKEN,
        # Expiring inside the leeway counts as expired.
        jwt(time.time() + 60),
    ],
    ids=["expired", "inside_leeway"],
)
def test_expired_saved_token_triggers_login(motherduck_home, saved):
    write_saved_token(motherduck_home, saved)
    server = FakeServer([(200, {"id_token": "id-token"})])
    authenticator, _, _ = make_authenticator(server)
    assert authenticator.get_token() == VALID_TOKEN
    assert json.loads(credentials_file(motherduck_home).read_text())[
        "motherduck_token"
    ] == (VALID_TOKEN)


@pytest.mark.parametrize(
    "contents",
    [
        "not json",
        json.dumps({"version": 2, "motherduck_token": VALID_TOKEN}),
        json.dumps({"version": 1, "motherduck_token": " "}),
        json.dumps([VALID_TOKEN]),
    ],
    ids=["not_json", "wrong_version", "blank_token", "not_object"],
)
def test_unreadable_saved_token_triggers_login(motherduck_home, contents):
    motherduck_home.mkdir()
    credentials_file(motherduck_home).write_text(contents)
    server = FakeServer([(200, {"id_token": "id-token"})])
    authenticator, _, _ = make_authenticator(server)
    assert authenticator.get_token() == VALID_TOKEN


def test_denied_login_raises():
    server = FakeServer(
        [(403, {"error": "access_denied", "error_description": "User denied"})]
    )
    authenticator, _, _ = make_authenticator(server)
    with pytest.raises(BrowserAuthError, match="User denied"):
        authenticator.get_token()


def test_login_times_out_when_code_expires():
    server = FakeServer([(403, {"error": "authorization_pending"})] * 200)
    authenticator, clock, _ = make_authenticator(server)
    with pytest.raises(BrowserAuthError, match="timed out"):
        authenticator.get_token()
    assert clock.now >= DEVICE["expires_in"]


def test_unknown_account_raises_with_sso_hint():
    server = FakeServer(
        [(200, {"id_token": "id-token"})],
        [
            (500, None),
            (
                404,
                {
                    "message": "User does not exist",
                    "requiredAlternateLoginMethodDisplayName": "SSO",
                },
            ),
        ],
    )
    authenticator, _, _ = make_authenticator(server)
    with pytest.raises(BrowserAuthError, match="User does not exist; log in with SSO"):
        authenticator.get_token()


def test_unreachable_login_service_raises():
    server = FakeServer([])
    server.responses[browser_auth.OAUTH_DEVICE_CODE_URL] = [OSError("no route")]
    authenticator, _, _ = make_authenticator(server)
    with pytest.raises(BrowserAuthError, match="no route"):
        authenticator.get_token()


def test_invalidate_forgets_rejected_token(motherduck_home):
    write_saved_token(motherduck_home, VALID_TOKEN)
    fresh = jwt(time.time() + 7200)
    server = FakeServer([(200, {"id_token": "id-token"})], [(200, {"token": fresh})])
    authenticator, _, _ = make_authenticator(server)
    assert authenticator.get_token() == VALID_TOKEN

    authenticator.invalidate("some-other-token")
    assert authenticator.get_token() == VALID_TOKEN

    authenticator.invalidate(VALID_TOKEN)
    assert authenticator.get_token() == fresh
    assert {VALID_TOKEN, fresh} <= set(browser_auth._issued_tokens)


def test_invalidate_keeps_file_written_by_another_login(motherduck_home):
    write_saved_token(motherduck_home, VALID_TOKEN)
    authenticator, _, _ = make_authenticator(FakeServer([]))
    authenticator.get_token()
    other = jwt(time.time() + 7200)
    write_saved_token(motherduck_home, other)
    authenticator.invalidate(VALID_TOKEN)
    assert authenticator.get_token() == other


def test_concurrent_callers_log_in_once():
    server = FakeServer([(200, {"id_token": "id-token"})])
    authenticator, _, _ = make_authenticator(server)
    results = []
    threads = [
        threading.Thread(target=lambda: results.append(authenticator.get_token()))
        for _ in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert results == [VALID_TOKEN] * 8
    assert server.count(browser_auth.OAUTH_DEVICE_CODE_URL) == 1


def test_relative_motherduck_home_is_rejected(monkeypatch):
    monkeypatch.setenv("MOTHERDUCK_HOME", "relative/path")
    authenticator, _, _ = make_authenticator(FakeServer([]))
    with pytest.raises(BrowserAuthError, match="absolute"):
        authenticator.get_token()


def test_default_credentials_path(monkeypatch, tmp_path):
    monkeypatch.delenv("MOTHERDUCK_HOME")
    monkeypatch.setenv("HOME", str(tmp_path))
    assert browser_auth.credentials_file_path() == (
        tmp_path / ".motherduck" / "credentials.json"
    )


@pytest.mark.parametrize("token", ["opaque-token", "a.b.c", "a.e30.c"])
def test_tokens_without_readable_expiry_are_not_expired(token):
    assert not token_expired(token)


class FakeHandle:
    closed = False

    def cursor(self):
        return contextlib.nullcontext()

    def close(self):
        self.closed = True


def open_connection(monkeypatch, credentials, outcomes):
    """Open a connection where each psycopg.connect call takes the next outcome."""
    passwords = []

    def connect(**kwargs):
        passwords.append(kwargs["password"])
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(psycopg, "connect", connect)
    manager = MotherDuckConnectionManager(
        SimpleNamespace(credentials=credentials), multiprocessing.get_context("spawn")
    )
    manager.databases_needed_callback = list
    connection = Connection(
        type="motherduck",
        name="test",
        credentials=credentials,
        state=ConnectionState.INIT,
    )
    return manager, connection, passwords


def browser_credentials():
    return MotherDuckCredentials(
        host="example.com",
        database="db",
        schema="main",
        authenticator="externalbrowser",
    )


def test_open_uses_browser_token(monkeypatch, motherduck_home):
    write_saved_token(motherduck_home, VALID_TOKEN)
    handle = FakeHandle()
    manager, connection, passwords = open_connection(
        monkeypatch, browser_credentials(), [handle]
    )
    manager.open(connection)
    assert connection.state == ConnectionState.OPEN
    assert connection.handle is handle
    assert passwords == [VALID_TOKEN]


def test_open_logs_in_again_when_token_is_rejected(monkeypatch, motherduck_home):
    write_saved_token(motherduck_home, VALID_TOKEN)
    fresh = jwt(time.time() + 7200)
    server = FakeServer([(200, {"id_token": "id-token"})], [(200, {"token": fresh})])
    handle = FakeHandle()
    manager, connection, passwords = open_connection(
        monkeypatch,
        browser_credentials(),
        [psycopg.errors.InvalidPassword("Jwt is expired"), handle],
    )
    manager.browser_authenticator, _, _ = make_authenticator(server)
    manager.open(connection)
    assert connection.handle is handle
    assert passwords == [VALID_TOKEN, fresh]
    assert json.loads(credentials_file(motherduck_home).read_text())[
        "motherduck_token"
    ] == (fresh)


def test_open_reports_login_failure_and_redacts(monkeypatch, motherduck_home):
    write_saved_token(motherduck_home, VALID_TOKEN)
    manager, connection, _ = open_connection(
        monkeypatch,
        browser_credentials(),
        [psycopg.OperationalError(f"server rejected {VALID_TOKEN}")],
    )
    with pytest.raises(FailedToConnectError) as excinfo:
        manager.open(connection)
    assert VALID_TOKEN not in str(excinfo.value)
    assert "[REDACTED]" in str(excinfo.value)
    assert connection.state == ConnectionState.FAIL


def test_open_wraps_browser_auth_errors(monkeypatch):
    manager, connection, passwords = open_connection(
        monkeypatch, browser_credentials(), []
    )
    server = FakeServer([(403, {"error": "access_denied"})])
    manager.browser_authenticator, _, _ = make_authenticator(server)
    with pytest.raises(FailedToConnectError, match="access_denied"):
        manager.open(connection)
    assert connection.state == ConnectionState.FAIL
    assert passwords == []


def test_token_authenticator_does_not_use_browser(monkeypatch):
    credentials = MotherDuckCredentials(
        host="example.com", token="secret", database="db", schema="main"
    )
    manager, connection, passwords = open_connection(
        monkeypatch, credentials, [psycopg.errors.InvalidPassword("bad")]
    )
    with pytest.raises(FailedToConnectError):
        manager.open(connection)
    assert passwords == ["secret"]


def test_failed_login_is_not_retried_by_other_callers():
    server = FakeServer([(403, {"error": "access_denied"})])
    authenticator, _, _ = make_authenticator(server)
    for _ in range(3):
        with pytest.raises(BrowserAuthError, match="access_denied"):
            authenticator.get_token()
    assert server.count(browser_auth.OAUTH_DEVICE_CODE_URL) == 1


def test_cancellation_stops_waiting_for_login():
    canceled = threading.Event()
    server = FakeServer([(403, {"error": "authorization_pending"})] * 10)
    authenticator, _, _ = make_authenticator(
        server, canceled=canceled, sleep=lambda seconds: canceled.set()
    )
    with pytest.raises(BrowserAuthError, match="cancelled"):
        authenticator.get_token()
    assert server.count(browser_auth.OAUTH_TOKEN_URL) == 0


def test_default_wait_returns_early_when_canceled():
    canceled = threading.Event()
    canceled.set()
    server = FakeServer([(403, {"error": "authorization_pending"})])
    authenticator = BrowserAuthenticator(
        request=server,
        open_browser=lambda url: None,
        can_open_browser=lambda: True,
        canceled=canceled,
        output=io.StringIO(),
    )
    started = time.monotonic()
    with pytest.raises(BrowserAuthError, match="cancelled"):
        authenticator.get_token()
    assert time.monotonic() - started < DEVICE["interval"]


def test_browser_not_opened_without_display():
    server = FakeServer([(200, {"id_token": "id-token"})])
    opened = []
    authenticator, _, output = make_authenticator(
        server, opened, can_open_browser=lambda: False
    )
    assert authenticator.get_token() == VALID_TOKEN
    assert opened == []
    assert DEVICE["verification_uri_complete"] in output.getvalue()


@pytest.mark.parametrize(
    "platform, env, expected",
    [
        ("linux", {}, False),
        ("linux", {"DISPLAY": ":0"}, True),
        ("linux", {"WAYLAND_DISPLAY": "wayland-0"}, True),
        ("darwin", {}, True),
        ("win32", {}, True),
    ],
)
def test_can_open_browser(monkeypatch, platform, env, expected):
    monkeypatch.setattr(browser_auth.sys, "platform", platform)
    monkeypatch.delenv("DISPLAY", raising=False)
    monkeypatch.delenv("WAYLAND_DISPLAY", raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    assert browser_auth.can_open_browser() is expected


def test_dropped_http_connection_is_a_network_error(monkeypatch):
    def urlopen(request, timeout):
        raise http.client.RemoteDisconnected("closed")

    monkeypatch.setattr(browser_auth.urllib.request, "urlopen", urlopen)
    with pytest.raises(ConnectionError, match="RemoteDisconnected"):
        browser_auth.http_request(browser_auth.OAUTH_TOKEN_URL, data={})


def test_failed_save_leaves_no_token_behind(monkeypatch, tmp_path):
    def replace(source, destination):
        raise OSError("replace failed")

    monkeypatch.setattr(browser_auth.os, "replace", replace)
    with pytest.raises(OSError, match="replace failed"):
        browser_auth.save_token(tmp_path / "credentials.json", VALID_TOKEN)
    assert list(tmp_path.iterdir()) == []


def test_rollback_failure_is_redacted_without_profile_token(monkeypatch):
    browser_auth._record_issued_token(VALID_TOKEN)
    events = []
    monkeypatch.setattr("dbt.adapters.motherduck.connections.fire_event", events.append)

    class FailingHandle:
        def rollback(self):
            raise psycopg.OperationalError(f"rollback failed for {VALID_TOKEN}")

    connection = SimpleNamespace(
        name="test", handle=FailingHandle(), credentials=browser_credentials()
    )
    MotherDuckConnectionManager._rollback_handle(connection)
    (event,) = events
    assert "rollback failed" in event.exc_info
    assert VALID_TOKEN not in event.exc_info


def test_login_uses_requested_client_id():
    server = FakeServer([(200, {"id_token": "id-token"})])
    authenticator, _, _ = make_authenticator(server)
    authenticator.get_token("custom-client")
    client_ids = [
        data["client_id"]
        for url, data, _ in server.requests
        if url in (browser_auth.OAUTH_DEVICE_CODE_URL, browser_auth.OAUTH_TOKEN_URL)
    ]
    assert client_ids == ["custom-client", "custom-client"]


@pytest.mark.parametrize(
    "override, expected",
    [(None, browser_auth.OAUTH_CLIENT_ID), ("custom-client", "custom-client")],
)
def test_open_passes_profile_client_id(monkeypatch, override, expected):
    credentials = MotherDuckCredentials(
        host="example.com",
        database="db",
        schema="main",
        authenticator="externalbrowser",
        oauth_client_id=override,
    )
    manager, connection, _ = open_connection(monkeypatch, credentials, [FakeHandle()])
    requested = []

    class RecordingAuthenticator:
        def get_token(self, client_id):
            requested.append(client_id)
            return VALID_TOKEN

    manager.browser_authenticator = RecordingAuthenticator()
    manager.open(connection)
    assert requested == [expected]
