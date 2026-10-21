import json
from io import StringIO

import psycopg
import pytest
from dbt.adapters.contracts.connection import Connection, ConnectionState, Identifier
from dbt_common.events.base_types import EventLevel
from dbt_common.events.event_manager import EventManager
from dbt_common.events.event_manager_client import (
    ctx_set_event_manager,
    get_event_manager,
)
from dbt_common.events.functions import env_scrubber
from dbt_common.events.logger import LineFormat, LoggerConfig

from dbt.adapters.motherduck.connections import MotherDuckConnectionManager
from dbt.adapters.motherduck.credentials import MotherDuckCredentials


@pytest.fixture
def connection():
    credentials = MotherDuckCredentials(
        host="localhost",
        database="unused",
        schema="main",
        token="synthetic-sensitive-token",
    )
    # An invalid option creates a failed libpq handle without contacting a server.
    pgconn = psycopg.pq.PGconn.connect(b"invalid_redaction_test_option=unused")
    handle = psycopg.Connection(pgconn)
    handle.close()
    return Connection(
        type=Identifier("motherduck"),
        name="redaction",
        handle=handle,
        credentials=credentials,
        transaction_open=True,
        state=ConnectionState.OPEN,
    )


@pytest.fixture
def rollback_logs():
    stream = StringIO()
    messages = []
    events = EventManager()
    events.add_logger(
        LoggerConfig(
            name="rollback_redaction",
            line_format=LineFormat.Json,
            level=EventLevel.DEBUG,
            scrubber=env_scrubber,
            output_stream=stream,
        )
    )
    events.add_callback(messages.append)
    previous = get_event_manager()
    ctx_set_event_manager(events)
    try:
        yield stream, messages
    finally:
        ctx_set_event_manager(previous)


def test_rollback_failure_redacts_event_and_json(connection, rollback_logs):
    token = connection.credentials.token
    try:
        # libpq includes the invalid option in the error, without making a connection.
        psycopg.connect(f"{token}=unused")
    except psycopg.ProgrammingError as exc:
        assert token in str(exc)
        # A real rollback on the closed connection fails while this error is active.
        MotherDuckConnectionManager.close(connection)
    else:
        pytest.fail("The invalid connection option did not raise a driver error")

    stream, messages = rollback_logs
    (failure,) = [event for event in messages if event.info.name == "RollbackFailed"]
    assert "[REDACTED]" in failure.data.exc_info
    assert token not in failure.data.exc_info
    logged = [json.loads(line) for line in stream.getvalue().splitlines()]
    (logged_failure,) = [
        event for event in logged if event["info"]["name"] == "RollbackFailed"
    ]
    assert logged_failure["data"]["exc_info"] == failure.data.exc_info
