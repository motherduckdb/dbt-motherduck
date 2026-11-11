from types import SimpleNamespace

import pytest
from dbt.adapters.contracts.connection import AdapterResponse
from dbt_common.exceptions import DbtRuntimeError

import dbt.adapters.motherduck.flights as flights
from dbt.adapters.motherduck.credentials import FlightConfig
from dbt.adapters.motherduck.flights import (
    DEFAULT_DUCKDB_VERSION,
    FLIGHT_NAME_KEY,
    FlightRunner,
    FlightSpec,
    FlightSqlCursor,
    build_requirements,
    build_source,
    sanitize_flight_name,
    unsupported_submission_message,
)

COMPILED_CODE = """
def model(dbt, session):
    return dbt.ref("upstream")
"""


def parsed_model(**config):
    return {
        "package_name": "my_project",
        "database": "my_db",
        "schema": "main",
        "alias": "my_model",
        "config": config,
    }


class FakeCursor:
    """Records SQL and replays canned rows."""

    def __init__(self, responses):
        self.responses = responses
        self.executed = []
        self._rows = []

    def execute(self, sql, bindings=None):
        self.executed.append(sql)
        for prefix, rows in self.responses.items():
            if prefix in sql:
                self._rows = rows() if callable(rows) else rows
                return self
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def sql_containing(self, needle):
        return [sql for sql in self.executed if needle in sql]


def test_flight_name_comes_from_the_macro():
    model = parsed_model()
    model[FLIGHT_NAME_KEY] = "my-custom-flight"
    assert FlightRunner(FlightConfig()).flight_name(model) == "my-custom-flight"


def test_flight_name_falls_back_when_the_macro_did_not_run():
    name = FlightRunner(FlightConfig()).flight_name(parsed_model())
    assert name == "dbt-my_project-my_db-main-my_model"


def test_flight_name_is_sanitized():
    assert sanitize_flight_name("dbt.dev schema/model") == "dbt.dev_schema_model"


def test_over_long_flight_name_is_rejected_not_truncated():
    with pytest.raises(DbtRuntimeError, match="character limit"):
        sanitize_flight_name("x" * 201)
    assert len(sanitize_flight_name("x" * 200)) == 200


def test_flight_name_rejects_an_empty_macro_result():
    with pytest.raises(DbtRuntimeError, match="empty Flight name"):
        sanitize_flight_name("///")


def test_build_source_appends_entrypoint():
    source = build_source(COMPILED_CODE)
    assert source.startswith("def model(dbt, session):")
    assert "def main():" in source
    assert 'duckdb.connect("md:")' in source
    assert "__dbt_settings = []" in source


def test_build_source_rejects_oversized_model():
    with pytest.raises(
        DbtRuntimeError, match="too large to run as a MotherDuck Flight"
    ):
        build_source("# " + "x" * (200 * 1024))


def test_build_source_applies_profile_settings_and_drops_secrets():
    source = build_source(
        COMPILED_CODE,
        {"TimeZone": "UTC", "motherduck_token": "secret-value", "custom": "a'b"},
    )
    assert "SET TimeZone = 'UTC'" in source
    assert "SET custom = 'a''b'" in source
    assert "secret-value" not in source
    assert source.count("__dbt_apply_settings(") == 3


def test_build_requirements_pins_the_documented_duckdb_version():
    requirements = build_requirements(parsed_model(), FlightConfig())
    assert requirements.splitlines()[0] == f"duckdb=={DEFAULT_DUCKDB_VERSION}"


def test_build_requirements_lets_the_model_override_the_profile():
    config = FlightConfig(requirements=["pandas==2.2.3"], duckdb_version="1.4.0")
    requirements = build_requirements(
        parsed_model(packages=["pandas==2.1.0", "duckdb==1.4.2"]), config
    )
    assert requirements.splitlines() == ["duckdb==1.4.2", "pandas==2.1.0"]


def test_build_requirements_normalizes_distribution_names():
    config = FlightConfig(requirements=["scikit_learn==1.4.0"])
    requirements = build_requirements(
        parsed_model(packages=["Scikit-Learn==1.5.0"]), config
    )
    assert "scikit_learn==1.4.0" not in requirements
    assert "Scikit-Learn==1.5.0" in requirements


def test_build_requirements_passes_pip_options_through():
    config = FlightConfig(requirements=["--index-url https://example.com/simple"])
    lines = build_requirements(parsed_model(), config).splitlines()
    assert lines[0] == "--index-url https://example.com/simple"


def test_local_submission_is_rejected_with_a_reason():
    message = unsupported_submission_message("local")
    assert "MotherDuck Flights" in message
    assert "submission_method 'local' is not supported" in message


def _runner_cursor(status="SUCCEEDED", existing=None):
    return FakeCursor(
        {
            "MD_LIST_FLIGHTS": existing or [],
            "MD_CREATE_FLIGHT": [("11111111-2222-3333-4444-555555555555",)],
            "MD_UPDATE_FLIGHT": [("11111111-2222-3333-4444-555555555555",)],
            "MD_GET_FLIGHT_VERSION": [("stale source", "stale requirements")],
            "MD_GET_FLIGHT(": [(2,)],
            "MD_RUN_FLIGHT": [(7,)],
            "MD_GET_FLIGHT_RUN": [(status, 0 if status == "SUCCEEDED" else 1)],
            "MD_GET_FLIGHT_LOGS": [
                ("Traceback (most recent call last):",),
                ("ValueError: nope",),
            ],
        }
    )


def test_submit_creates_runs_and_waits():
    cursor = _runner_cursor()
    response = FlightRunner(FlightConfig()).submit(
        cursor, parsed_model(), COMPILED_CODE
    )
    assert response._message == "OK"
    assert isinstance(response, AdapterResponse)
    assert "def main():" in cursor.sql_containing("MD_CREATE_FLIGHT")[0]
    assert cursor.sql_containing("MD_RUN_FLIGHT")


def test_documented_status_prefix_counts_as_success():
    cursor = _runner_cursor(status="RUN_STATUS_SUCCEEDED")
    response = FlightRunner(FlightConfig()).submit(
        cursor, parsed_model(), COMPILED_CODE
    )
    assert response._message == "OK"


def test_submit_reuses_an_unchanged_flight():
    existing = [
        ("11111111-2222-3333-4444-555555555555", "dbt-my_project-my_db-main-my_model")
    ]
    cursor = _runner_cursor(existing=existing)
    config = FlightConfig()
    cursor.responses["MD_GET_FLIGHT_VERSION"] = [
        (build_source(COMPILED_CODE), build_requirements(parsed_model(), config))
    ]
    FlightRunner(config).submit(cursor, parsed_model(), COMPILED_CODE)
    assert not cursor.sql_containing("MD_UPDATE_FLIGHT")
    assert not cursor.sql_containing("MD_CREATE_FLIGHT")
    assert cursor.sql_containing("MD_RUN_FLIGHT")


def test_submit_updates_a_changed_flight():
    existing = [
        ("11111111-2222-3333-4444-555555555555", "dbt-my_project-my_db-main-my_model")
    ]
    cursor = _runner_cursor(existing=existing)
    FlightRunner(FlightConfig()).submit(cursor, parsed_model(), COMPILED_CODE)
    assert cursor.sql_containing("MD_UPDATE_FLIGHT")
    assert not cursor.sql_containing("MD_CREATE_FLIGHT")


def test_failure_points_at_the_logs_instead_of_dumping_them():
    cursor = _runner_cursor(status="FAILED")
    with pytest.raises(DbtRuntimeError) as excinfo:
        FlightRunner(FlightConfig()).submit(cursor, parsed_model(), COMPILED_CODE)
    message = str(excinfo.value)
    assert "status FAILED" in message
    assert (
        "https://app.motherduck.com/flights/11111111-2222-3333-4444-555555555555/runs/7"
        in message
    )
    assert "ValueError: nope" not in message


def test_failure_inlines_a_log_tail_when_asked():
    cursor = _runner_cursor(status="FAILED")
    with pytest.raises(DbtRuntimeError, match="ValueError: nope"):
        FlightRunner(FlightConfig(log_lines=20)).submit(
            cursor, parsed_model(), COMPILED_CODE
        )
    assert '"limit" := 20' in cursor.sql_containing("MD_GET_FLIGHT_LOGS")[0]


def _expire_immediately(monkeypatch):
    clock = iter(range(1, 1000))
    monkeypatch.setattr(flights.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(flights.time, "sleep", lambda _seconds: None)


def test_submit_cancels_the_run_on_timeout(monkeypatch):
    _expire_immediately(monkeypatch)
    cursor = _runner_cursor(status="RUNNING")
    cursor.responses["MD_CANCEL_FLIGHT_RUN"] = [(True,)]
    statuses = iter([[("RUNNING",)], [("CANCELLED",)]])
    cursor.responses["MD_GET_FLIGHT_RUN"] = lambda: next(statuses, [("CANCELLED",)])
    with pytest.raises(DbtRuntimeError, match="was cancelled") as excinfo:
        FlightRunner(FlightConfig(timeout_sec=1, poll_interval_sec=1)).submit(
            cursor, parsed_model(), COMPILED_CODE
        )
    assert "Timed out" in str(excinfo.value)
    assert cursor.sql_containing("MD_CANCEL_FLIGHT_RUN")


def test_submit_reports_a_failed_cancel_without_masking_the_timeout(monkeypatch):
    _expire_immediately(monkeypatch)
    cursor = _runner_cursor(status="RUNNING")
    with pytest.raises(DbtRuntimeError, match="could not be cancelled") as excinfo:
        FlightRunner(FlightConfig(timeout_sec=1, poll_interval_sec=1)).submit(
            cursor, parsed_model(), COMPILED_CODE
        )
    assert "Timed out" in str(excinfo.value)


def test_invocation_cancel_cancels_the_flight():
    cursor = _runner_cursor(status="CANCELLED")
    cursor.responses["MD_CANCEL_FLIGHT_RUN"] = [(True,)]
    with pytest.raises(DbtRuntimeError, match="dbt invocation"):
        FlightRunner(FlightConfig(), canceled=lambda: True).submit(
            cursor, parsed_model(), COMPILED_CODE
        )
    assert cursor.sql_containing("MD_CANCEL_FLIGHT_RUN")


def test_a_run_that_never_stops_is_not_waited_on_forever(monkeypatch):
    _expire_immediately(monkeypatch)
    monkeypatch.setattr(flights, "CANCEL_WAIT_SEC", 0)
    cursor = _runner_cursor(status="RUNNING")
    cursor.responses["MD_CANCEL_FLIGHT_RUN"] = [(True,)]
    with pytest.raises(DbtRuntimeError, match="could not be cancelled"):
        FlightRunner(FlightConfig(timeout_sec=1, poll_interval_sec=1)).submit(
            cursor, parsed_model(), COMPILED_CODE
        )


def test_submit_passes_optional_flight_settings():
    cursor = _runner_cursor()
    config = FlightConfig(access_token_name="analytics-token", max_runtime_sec=900)
    FlightRunner(config).submit(cursor, parsed_model(), COMPILED_CODE)
    create = cursor.sql_containing("MD_CREATE_FLIGHT")[0]
    assert "access_token_name := 'analytics-token'" in create
    assert "max_runtime_sec := 900" in create


def test_access_token_name_is_kept_out_of_errors():
    cursor = _runner_cursor()

    def echo_sql():
        raise RuntimeError("Binder Error in: access_token_name := 'super-secret-token'")

    cursor.responses["MD_CREATE_FLIGHT"] = echo_sql
    with pytest.raises(DbtRuntimeError) as excinfo:
        FlightRunner(FlightConfig(access_token_name="super-secret-token")).submit(
            cursor, parsed_model(), COMPILED_CODE
        )
    assert "super-secret-token" not in str(excinfo.value)
    assert "***" in str(excinfo.value)


def test_duplicate_name_from_a_concurrent_run_updates_that_flight():
    cursor = _runner_cursor()
    existing = [
        ("11111111-2222-3333-4444-555555555555", "dbt-my_project-my_db-main-my_model")
    ]

    def create_then_conflict():
        raise DbtRuntimeError('Catalog Error: Flight with name "..." already exists')

    cursor.responses["MD_CREATE_FLIGHT"] = create_then_conflict
    cursor.responses["MD_LIST_FLIGHTS"] = existing
    FlightRunner(FlightConfig()).submit(cursor, parsed_model(), COMPILED_CODE)
    assert cursor.sql_containing("MD_UPDATE_FLIGHT")
    assert cursor.sql_containing("MD_RUN_FLIGHT")


def test_duplicate_name_owned_by_someone_else_is_explained():
    cursor = _runner_cursor()

    def conflict():
        raise DbtRuntimeError('Catalog Error: Flight with name "..." already exists')

    cursor.responses["MD_CREATE_FLIGHT"] = conflict
    with pytest.raises(DbtRuntimeError, match="motherduck__flight_name"):
        FlightRunner(FlightConfig()).submit(cursor, parsed_model(), COMPILED_CODE)


EXISTING = [
    ("11111111-2222-3333-4444-555555555555", "dbt-my_project-my_db-main-my_model")
]


def _current_version_row(spec, **changed):
    fields = spec.version_fields()
    fields.update(changed)
    return [tuple(fields.values())]


def _spec(config, **model_config):
    model = parsed_model(**model_config)
    runner = FlightRunner(config)
    return FlightSpec(
        source=build_source(COMPILED_CODE),
        requirements=build_requirements(model, config),
        instance_type=runner.instance_type(model),
        access_token_name=config.access_token_name,
        max_runtime_sec=config.max_runtime_sec,
    )


def test_model_instance_type_overrides_the_profile():
    cursor = _runner_cursor()
    FlightRunner(FlightConfig(instance_type="F4")).submit(
        cursor, parsed_model(instance_type="f32"), COMPILED_CODE
    )
    assert "instance_type := 'F32'" in cursor.sql_containing("MD_CREATE_FLIGHT")[0]


def test_changing_instance_type_updates_the_flight():
    config = FlightConfig(instance_type="F32")
    cursor = _runner_cursor(existing=EXISTING)
    cursor.responses["MD_GET_FLIGHT_VERSION"] = _current_version_row(
        _spec(config), instance_type="F16"
    )
    FlightRunner(config).submit(cursor, parsed_model(), COMPILED_CODE)
    assert "instance_type := 'F32'" in cursor.sql_containing("MD_UPDATE_FLIGHT")[0]


def test_unchanged_settings_still_reuse_the_flight():
    config = FlightConfig(
        instance_type="F32", access_token_name="tok", max_runtime_sec=900
    )
    cursor = _runner_cursor(existing=EXISTING)
    cursor.responses["MD_GET_FLIGHT_VERSION"] = _current_version_row(_spec(config))
    FlightRunner(config).submit(cursor, parsed_model(), COMPILED_CODE)
    assert not cursor.sql_containing("MD_UPDATE_FLIGHT")
    assert cursor.sql_containing("MD_RUN_FLIGHT")


def test_version_comparison_ignores_fields_we_do_not_set():
    spec = FlightSpec(source="s", requirements="r")
    assert list(spec.version_fields()) == ["source_code", "requirements_txt"]


def test_a_short_version_row_is_not_treated_as_current():
    config = FlightConfig(instance_type="F32")
    cursor = _runner_cursor(existing=EXISTING)
    cursor.responses["MD_GET_FLIGHT_VERSION"] = [(build_source(COMPILED_CODE),)]
    FlightRunner(config).submit(cursor, parsed_model(), COMPILED_CODE)
    assert cursor.sql_containing("MD_UPDATE_FLIGHT")


def test_flight_cursor_does_not_open_a_worker_transaction():
    class Connections:
        def __init__(self):
            self.calls = []

        def execute(
            self,
            sql,
            auto_begin=False,
            fetch=False,
            limit=None,
            allow_when_canceled=False,
        ):
            self.calls.append(
                {
                    "sql": sql,
                    "auto_begin": auto_begin,
                    "allow_when_canceled": allow_when_canceled,
                }
            )

            return None, SimpleNamespace(rows=[("ok",)])

    connections = Connections()
    cursor = FlightSqlCursor(connections)
    cursor.execute("select 1")
    cursor.permit_when_canceled(True)
    cursor.execute("cancel")
    assert connections.calls[0]["auto_begin"] is False
    assert connections.calls[0]["allow_when_canceled"] is False
    assert connections.calls[1]["allow_when_canceled"] is True
    assert cursor.fetchone() == ("ok",)
