# Run dbt Python models on MotherDuck Flights.
#
# The Postgres endpoint has no local Python runtime. A Flight is a single-file
# Python program MotherDuck runs in its own container. dbt's compiled model
# already defines model() and dbtObj(); py_write_table adds materialize(); the
# generated main() opens duckdb.connect("md:") and runs both. The worker drives
# that lifecycle with MD_*_FLIGHT SQL on its own connection, then renames or
# merges the table the Flight committed.
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Any

from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError

logger = AdapterLogger("MotherDuck")

FLIGHT_SUBMISSION = "flight"
# Omitted submission_method means flight. "flights" is the same choice.
FLIGHT_SUBMISSIONS = frozenset({FLIGHT_SUBMISSION, "flights"})
FLIGHT_NAME_KEY = "__dbt_motherduck_flight_name"

# SQL functions return the short form. The docs' RUN_STATUS_* names are accepted too.
TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED", "CANCELLED"})

# MotherDuck's limits on a Flight definition.
MAX_SOURCE_BYTES = 200 * 1024
MAX_REQUIREMENTS_BYTES = 20 * 1024
# A longer name fails in MotherDuck with an opaque internal error.
MAX_FLIGHT_NAME_LENGTH = 200

# How long to wait for a cancelled run to stop before giving up on it.
CANCEL_WAIT_SEC = 60

DEFAULT_LOG_URL_TEMPLATE = (
    "https://app.motherduck.com/flights/{flight_id}/runs/{run_number}"
)
# Pinned because an unpinned install can resolve a client MotherDuck rejects.
# Override with flights.duckdb_version. This is the client the Flights docs pin.
DEFAULT_DUCKDB_VERSION = "1.5.3"

# Profile settings that must not be copied into Flight source. The runtime
# injects MOTHERDUCK_TOKEN itself; the dbt token never belongs in the source.
SECRET_SETTINGS = frozenset({"motherduck_token", "motherduck_access_token", "token"})

# The message is raised from the materialization, before a Flight is created.
UNSUPPORTED_SUBMISSION = (
    "Python models on dbt-motherduck run on MotherDuck Flights. "
    "submission_method {method!r} is not supported because the Postgres endpoint "
    "cannot run a local Python runtime. Remove submission_method or set it to 'flight'."
)

FLIGHT_ENTRYPOINT = """

# --- dbt-motherduck flight entrypoint (generated) ---
__dbt_settings = {settings!r}


def __dbt_apply_settings(cursor):
    for statement in __dbt_settings:
        cursor.execute(statement)


def main():
    import duckdb as _duckdb

    con = _duckdb.connect("md:")
    __dbt_apply_settings(con)

    def load_df_function(table_name):
        return con.query(f"select * from {{table_name}}")

    dbt = dbtObj(load_df_function)
    df = model(dbt, con)
    if isinstance(df, _duckdb.DuckDBPyRelation):
        # A relation can reference temp tables that do not cross cursors.
        materialize(df, con)
    else:
        write_cursor = con.cursor()
        __dbt_apply_settings(write_cursor)
        materialize(df, write_cursor)


if __name__ == "__main__":
    main()
"""


def is_flight_submission(method: Any) -> bool:
    """Missing, blank, flight, and flights all run on a Flight."""
    if method is None:
        return True
    return str(method).strip().lower() in FLIGHT_SUBMISSIONS or not str(method).strip()


def unsupported_submission_message(method: str) -> str:
    return UNSUPPORTED_SUBMISSION.format(method=method)


def _distribution_name(requirement: str) -> str | None:
    """The distribution a requirements line pins, normalized per PEP 503."""
    match = re.match(r"^([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[|[=<>!~;@]|$)", requirement)
    if not match:
        return None
    return re.sub(r"[-_.]+", "-", match.group(1)).lower()


def escape_sql_string(value: Any) -> str:
    return str(value).replace("'", "''")


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sanitize_flight_name(name: str) -> str:
    """Make a name safe for MotherDuck, rejecting one it could not carry.

    Deliberately not truncated: two models whose names agree up to the cut
    would share one Flight and overwrite each other's source on every run.
    """
    name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_-")
    if not name:
        raise DbtRuntimeError("The flight_name macro returned an empty Flight name.")
    if len(name) > MAX_FLIGHT_NAME_LENGTH:
        raise DbtRuntimeError(
            f"Flight name '{name}' is {len(name)} characters, over MotherDuck's "
            f"{MAX_FLIGHT_NAME_LENGTH} character limit. Override the "
            "`motherduck__flight_name` macro to give this project's Flights shorter names."
        )
    return name


def _normalize_status(status: Any) -> str:
    text = str(status)
    prefix = "RUN_STATUS_"
    return text[len(prefix) :] if text.startswith(prefix) else text


def settings_statements(settings: dict[str, Any] | None) -> list[str]:
    """Profile settings as SET statements, omitting anything that is a secret."""
    return [
        f"SET {quote_identifier(key)} = '{escape_sql_string(value)}'"
        for key, value in (settings or {}).items()
        if key.lower() not in SECRET_SETTINGS
    ]


def build_source(compiled_code: str, settings: dict[str, Any] | None = None) -> str:
    """Turn a model's compiled Python into a Flight entrypoint."""
    entrypoint = FLIGHT_ENTRYPOINT.format(settings=settings_statements(settings))
    source = compiled_code.lstrip() + entrypoint
    size = len(source.encode("utf-8"))
    if size > MAX_SOURCE_BYTES:
        raise DbtRuntimeError(
            f"Python model is too large to run as a MotherDuck Flight: {size} bytes "
            f"exceeds the {MAX_SOURCE_BYTES} byte limit. Move the bulk of the code "
            "into a package listed in the model's `packages` config."
        )
    return source


def build_requirements(parsed_model: dict[str, Any], config: Any) -> str:
    """Assemble requirements.txt. Later sources win per distribution, so a model's
    `packages` override `flights.requirements`, which override the duckdb pin.
    """
    packages: list[str] = [f"duckdb=={config.duckdb_version or DEFAULT_DUCKDB_VERSION}"]
    packages.extend(config.requirements or [])
    packages.extend((parsed_model.get("config") or {}).get("packages") or [])

    resolved: dict[str, str] = {}
    passthrough: list[str] = []
    for package in packages:
        entry = package.strip()
        if not entry:
            continue
        name = _distribution_name(entry)
        if name is None:
            passthrough.append(entry)
        else:
            resolved[name] = entry

    requirements = "\n".join(passthrough + list(resolved.values())) + "\n"
    size = len(requirements.encode("utf-8"))
    if size > MAX_REQUIREMENTS_BYTES:
        raise DbtRuntimeError(
            f"Python model requirements are too large for a MotherDuck Flight: {size} "
            f"bytes exceeds the {MAX_REQUIREMENTS_BYTES} byte limit."
        )
    return requirements


@dataclass
class FlightSpec:
    """Fields that mint a new Flight version when they change."""

    source: str
    requirements: str
    instance_type: str | None = None
    access_token_name: str | None = None
    max_runtime_sec: int | None = None

    def sql_args(self) -> str:
        args = (
            f"source_code := '{escape_sql_string(self.source)}', "
            f"requirements_txt := '{escape_sql_string(self.requirements)}'"
        )
        if self.access_token_name:
            args += (
                f", access_token_name := '{escape_sql_string(self.access_token_name)}'"
            )
        if self.max_runtime_sec is not None:
            args += f", max_runtime_sec := {int(self.max_runtime_sec)}"
        if self.instance_type:
            args += f", instance_type := '{escape_sql_string(self.instance_type)}'"
        return args

    def version_fields(self) -> dict[str, Any]:
        """Only fields this adapter sets. An omitted field takes a server default
        the snapshot reports concretely, so comparing it would version every run.
        """
        fields: dict[str, Any] = {
            "source_code": self.source,
            "requirements_txt": self.requirements,
        }
        if self.access_token_name:
            fields["access_token_name"] = self.access_token_name
        if self.max_runtime_sec is not None:
            fields["max_runtime_sec"] = int(self.max_runtime_sec)
        if self.instance_type:
            fields["instance_type"] = self.instance_type
        return fields


class FlightSqlCursor:
    """Flight lifecycle SQL on this thread's worker connection.

    auto_begin stays off so polling does not open a transaction. Cancellation
    SQL is the one case that still has to run after the invocation is cancelled.
    """

    def __init__(self, connections):
        self._connections = connections
        self._allow_when_canceled = False
        self._rows: list[tuple] = []

    def permit_when_canceled(self, allowed: bool) -> None:
        self._allow_when_canceled = allowed

    def execute(self, sql: str):
        _, table = self._connections.execute(
            sql,
            auto_begin=False,
            fetch=True,
            allow_when_canceled=self._allow_when_canceled,
        )
        self._rows = [tuple(row) for row in table.rows]
        return self

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class FlightRunner:
    """One Flight per model, so each keeps its own run history."""

    def __init__(
        self,
        config,
        settings: dict[str, Any] | None = None,
        canceled: Callable[[], bool] | None = None,
    ):
        self._config = config
        self._settings = settings
        self._canceled = canceled
        self._flight_ids: dict[str, str] = {}
        self._lock = Lock()

    def submit(
        self, cursor, parsed_model: dict[str, Any], compiled_code: str
    ) -> AdapterResponse:
        name = self.flight_name(parsed_model)
        spec = FlightSpec(
            source=build_source(compiled_code, self._settings),
            requirements=build_requirements(parsed_model, self._config),
            instance_type=self.instance_type(parsed_model),
            access_token_name=self._config.access_token_name,
            max_runtime_sec=self._config.max_runtime_sec,
        )
        flight_id = self._upsert_flight(cursor, name, spec)
        run_number = self._start_run(cursor, flight_id)
        logger.debug(f"Flight {name} ({flight_id}) started run {run_number}")
        status, exit_code = self._await_run(cursor, flight_id, run_number, name)
        if status != "SUCCEEDED":
            raise DbtRuntimeError(
                f"Python model failed on MotherDuck Flight '{name}' (run {run_number}, "
                f"status {status}, exit code {exit_code}).\n"
                + self._log_pointer(cursor, flight_id, run_number)
            )
        return AdapterResponse(_message="OK")

    def flight_name(self, parsed_model: dict[str, Any]) -> str:
        name = parsed_model.get(FLIGHT_NAME_KEY)
        if not name:
            parts = [
                parsed_model.get("package_name"),
                parsed_model.get("database"),
                parsed_model.get("schema"),
                parsed_model.get("alias") or parsed_model.get("name"),
            ]
            name = "-".join(["dbt"] + [str(part) for part in parts if part])
        return sanitize_flight_name(str(name))

    def instance_type(self, parsed_model: dict[str, Any]) -> str | None:
        """Model config wins over the profile. MotherDuck rejects an unknown size."""
        value = (parsed_model.get("config") or {}).get("instance_type")
        if value is None:
            value = self._config.instance_type
        return str(value).upper() if value else None

    def _is_canceled(self) -> bool:
        return bool(self._canceled and self._canceled())

    def _permit(self, cursor, allowed: bool) -> None:
        permit = getattr(cursor, "permit_when_canceled", None)
        if permit is not None:
            permit(allowed)

    def _upsert_flight(self, cursor, name: str, spec: FlightSpec) -> str:
        flight_id = self._find_flight(cursor, name)
        if flight_id is None:
            return self._create_flight(cursor, name, spec)
        if self._is_current(cursor, flight_id, spec):
            logger.debug(f"MotherDuck Flight {name} is up to date; reusing it")
            return flight_id
        self._execute(
            cursor,
            "SELECT flight_id FROM MD_UPDATE_FLIGHT("
            f"flight_id := '{flight_id}', {spec.sql_args()})",
        )
        logger.debug(f"Updated MotherDuck Flight {name} ({flight_id})")
        return flight_id

    def _create_flight(self, cursor, name: str, spec: FlightSpec) -> str:
        sql = (
            "SELECT flight_id FROM MD_CREATE_FLIGHT("
            f"name := '{escape_sql_string(name)}', {spec.sql_args()})"
        )
        try:
            row = self._execute(cursor, sql).fetchone()
        except DbtRuntimeError as err:
            if "already exists" not in str(err):
                raise
            # A concurrent run created it, or another user owns the name.
            # Names are unique per user, and only the owner can run one.
            with self._lock:
                self._flight_ids.pop(name, None)
            flight_id = self._find_flight(cursor, name)
            if flight_id is None:
                raise DbtRuntimeError(
                    f"A MotherDuck Flight named '{name}' already exists but is not yours, "
                    "so this model cannot run on it. Override the `motherduck__flight_name` "
                    "macro to give this project's Flights a distinct name."
                ) from err
            return self._upsert_flight(cursor, name, spec)
        flight_id = str(row[0])
        with self._lock:
            self._flight_ids[name] = flight_id
        logger.debug(f"Created MotherDuck Flight {name} ({flight_id})")
        return flight_id

    def _find_flight(self, cursor, name: str) -> str | None:
        with self._lock:
            if name in self._flight_ids:
                return self._flight_ids[name]
        page, offset = 200, 0
        while True:
            rows = self._execute(
                cursor,
                "SELECT flight_id, flight_name FROM MD_LIST_FLIGHTS("
                f'"limit" := {page}, "offset" := {offset}, owner_only := true)',
            ).fetchall()
            if not rows:
                return None
            with self._lock:
                for flight_id, flight_name in rows:
                    self._flight_ids[flight_name] = str(flight_id)
                found = self._flight_ids.get(name)
            if found is not None:
                return found
            if len(rows) < page:
                return None
            offset += page

    def _is_current(self, cursor, flight_id: str, spec: FlightSpec) -> bool:
        current = self._execute(
            cursor,
            f"SELECT current_version FROM MD_GET_FLIGHT(flight_id := '{flight_id}')",
        ).fetchone()
        if current is None or current[0] is None:
            return False
        expected = spec.version_fields()
        columns = ", ".join(expected)
        row = self._execute(
            cursor,
            f"SELECT {columns} FROM MD_GET_FLIGHT_VERSION("
            f"flight_id := '{flight_id}', version_number := {int(current[0])})",
        ).fetchone()
        if not row or len(row) != len(expected):
            return False
        return all(
            actual == value
            for actual, value in zip(row, expected.values(), strict=True)
        )

    def _start_run(self, cursor, flight_id: str) -> int:
        row = self._execute(
            cursor, f"SELECT run_number FROM MD_RUN_FLIGHT(flight_id := '{flight_id}')"
        ).fetchone()
        return int(row[0])

    def _await_run(self, cursor, flight_id: str, run_number: int, name: str):
        deadline = time.monotonic() + self._config.timeout_sec
        while True:
            if self._is_canceled():
                self._raise_canceled(cursor, flight_id, run_number, name)
            try:
                row = self._execute(
                    cursor,
                    "SELECT status, exit_code FROM MD_GET_FLIGHT_RUN("
                    f"flight_id := '{flight_id}', run_number := {run_number})",
                ).fetchone()
            except DbtRuntimeError as err:
                if self._is_canceled() or "was cancelled" in str(err):
                    self._raise_canceled(cursor, flight_id, run_number, name)
                raise
            status = _normalize_status(row[0])
            if status in TERMINAL_STATUSES:
                return status, row[1]
            if time.monotonic() >= deadline:
                # An abandoned run would commit the model table after dbt failed the node.
                cancelled = self._cancel_run(cursor, flight_id, run_number)
                raise DbtRuntimeError(
                    f"Timed out after {self._config.timeout_sec}s waiting for MotherDuck "
                    f"Flight '{name}' run {run_number} (last status: {status}). "
                    + (
                        "The run was cancelled. "
                        if cancelled
                        else "The run could not be cancelled and may still be going. "
                    )
                    + "Raise `flights.timeout_sec` if the model needs longer.\n"
                    + self._log_pointer(cursor, flight_id, run_number)
                )
            time.sleep(self._config.poll_interval_sec)

    def _raise_canceled(self, cursor, flight_id: str, run_number: int, name: str):
        cancelled = self._cancel_run(cursor, flight_id, run_number)
        raise DbtRuntimeError(
            f"Python model on MotherDuck Flight '{name}' was cancelled with the dbt "
            f"invocation (run {run_number}). "
            + (
                "The Flight run was cancelled.\n"
                if cancelled
                else "The Flight run could not be cancelled and may still be going.\n"
            )
            + self._log_pointer(cursor, flight_id, run_number)
        )

    def _cancel_run(self, cursor, flight_id: str, run_number: int) -> bool:
        self._permit(cursor, True)
        try:
            try:
                self._execute(
                    cursor,
                    "SELECT * FROM MD_CANCEL_FLIGHT_RUN("
                    f"flight_id := '{flight_id}', run_number := {run_number})",
                )
            except Exception as err:
                logger.debug(f"Could not cancel flight run {run_number}: {err}")
                return False
            return self._wait_until_stopped(cursor, flight_id, run_number)
        finally:
            self._permit(cursor, False)

    def _wait_until_stopped(self, cursor, flight_id: str, run_number: int) -> bool:
        """dbt drops the temp relation once this raises. A run still writing
        would recreate it, and the per-invocation name would never be cleaned up.
        """
        deadline = time.monotonic() + CANCEL_WAIT_SEC
        while time.monotonic() < deadline:
            row = self._execute(
                cursor,
                "SELECT status FROM MD_GET_FLIGHT_RUN("
                f"flight_id := '{flight_id}', run_number := {run_number})",
            ).fetchone()
            if row and _normalize_status(row[0]) in TERMINAL_STATUSES:
                return True
            time.sleep(self._config.poll_interval_sec)
        logger.warning(
            f"MotherDuck Flight run {run_number} did not stop within {CANCEL_WAIT_SEC}s "
            "of being cancelled; it may leave a temp relation behind."
        )
        return False

    def _log_pointer(self, cursor, flight_id: str, run_number: int) -> str:
        lines = [f"Logs: {self._log_location(flight_id, run_number)}"]
        if self._config.log_lines:
            lines.append(self._log_tail(cursor, flight_id, run_number))
        return "\n".join(line for line in lines if line)

    def _log_location(self, flight_id: str, run_number: int) -> str:
        template = self._config.log_url_template or DEFAULT_LOG_URL_TEMPLATE
        return template.format(flight_id=flight_id, run_number=run_number)

    def _log_tail(self, cursor, flight_id: str, run_number: int) -> str:
        try:
            rows = self._execute(
                cursor,
                "SELECT line FROM MD_GET_FLIGHT_LOGS("
                f"flight_id := '{flight_id}', run_number := {run_number}, "
                f'"limit" := {self._config.log_lines}, "order" := \'desc\') '
                "ORDER BY line_number",
            ).fetchall()
        except Exception as err:
            return f"(could not read flight logs: {err})"
        return "\n".join(str(row[0]) for row in rows)

    def _execute(self, cursor, sql: str):
        """Keep the access token label out of any error that echoes the statement."""
        try:
            return cursor.execute(sql)
        except Exception as err:
            raise DbtRuntimeError(self._redact(str(err))) from None

    def _redact(self, message: str) -> str:
        token = self._config.access_token_name
        if token:
            message = message.replace(token, "***")
        return message
