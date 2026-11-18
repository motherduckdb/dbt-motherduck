from dataclasses import dataclass, field
from typing import Any, Literal, get_args

import certifi
from dbt.adapters.contracts.connection import Credentials
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.exceptions import DbtRuntimeError

Authenticator = Literal["token", "externalbrowser"]
AUTHENTICATORS = get_args(Authenticator)


def _non_empty_string(value: str | None) -> bool:
    return isinstance(value, str) and bool(value.strip()) and "\x00" not in value


@dataclass
class FlightConfig(dbtClassMixin):
    """How Python models run on MotherDuck Flights.

    The profile token is not sent to the Flight. `access_token_name` is the
    label of a token stored in MotherDuck; the runtime injects it. Omit it to
    use MotherDuck's built-in Flights token.
    """

    access_token_name: str | None = None
    max_runtime_sec: int | None = None
    instance_type: str | None = None
    timeout_sec: int = 3600
    poll_interval_sec: float = 2.0
    log_lines: int = 0
    log_url_template: str | None = None
    requirements: list[str] | None = None
    duckdb_version: str | None = None

    def __post_init__(self):
        if self.access_token_name is not None and not _non_empty_string(
            self.access_token_name
        ):
            raise DbtRuntimeError(
                "flights.access_token_name must be a non-empty string without NUL characters"
            )
        if self.instance_type is not None and not _non_empty_string(self.instance_type):
            raise DbtRuntimeError(
                "flights.instance_type must be a non-empty string without NUL characters"
            )
        if self.duckdb_version is not None and not _non_empty_string(
            self.duckdb_version
        ):
            raise DbtRuntimeError(
                "flights.duckdb_version must be a non-empty string without NUL characters"
            )
        if type(self.timeout_sec) is not int or self.timeout_sec < 1:
            raise DbtRuntimeError("flights.timeout_sec must be a positive integer")
        if type(self.max_runtime_sec) not in (int, type(None)) or (
            self.max_runtime_sec is not None and self.max_runtime_sec < 0
        ):
            raise DbtRuntimeError(
                "flights.max_runtime_sec must be a non-negative integer or omitted"
            )
        if type(self.log_lines) is not int or self.log_lines < 0:
            raise DbtRuntimeError("flights.log_lines must be a non-negative integer")
        if (
            isinstance(self.poll_interval_sec, bool)
            or not isinstance(self.poll_interval_sec, (int, float))
            or self.poll_interval_sec <= 0
        ):
            raise DbtRuntimeError("flights.poll_interval_sec must be a positive number")
        if self.requirements is not None and (
            not isinstance(self.requirements, list)
            or any(not _non_empty_string(entry) for entry in self.requirements)
        ):
            raise DbtRuntimeError(
                "flights.requirements must be a list of non-empty requirement strings"
            )
        if self.log_url_template is not None:
            if not _non_empty_string(self.log_url_template):
                raise DbtRuntimeError(
                    "flights.log_url_template must be a non-empty string without NUL characters"
                )
            try:
                self.log_url_template.format(flight_id="flight", run_number=1)
            except Exception as err:
                raise DbtRuntimeError(
                    "flights.log_url_template must format {flight_id} and {run_number}"
                ) from err


@dataclass
class MotherDuckCredentials(Credentials):
    host: str
    token: str | None = field(default=None, repr=False)
    authenticator: Authenticator = "token"
    # Overrides the Auth0 application that `externalbrowser` logs in through.
    oauth_client_id: str | None = None
    port: int = 5432
    sslrootcert: str = "certifi"
    connect_timeout: int = 240
    session_name: str | None = None
    attach_databases: list[str] = field(default_factory=list)
    settings: dict[str, Any] = field(default_factory=dict)
    # Absent from _connection_keys(): access_token_name is a MotherDuck token label.
    flights: FlightConfig | None = None

    def __post_init__(self):
        if self.authenticator not in AUTHENTICATORS:
            raise DbtRuntimeError(
                f"authenticator must be one of {', '.join(map(repr, AUTHENTICATORS))}"
            )
        if self.authenticator == "token" and self.token is None:
            raise DbtRuntimeError(
                "token is a required property when authenticator is 'token'"
            )
        # An empty token is allowed so profiles can template
        # `token: "{{ env_var('X', '') }}"` for both modes.
        if self.authenticator == "externalbrowser" and self.token:
            raise DbtRuntimeError(
                "token must not be set when authenticator is 'externalbrowser'"
            )
        if self.authenticator != "externalbrowser" and self.oauth_client_id is not None:
            raise DbtRuntimeError(
                "oauth_client_id is only used when authenticator is 'externalbrowser'"
            )
        names = ("host", "database", "schema", "sslrootcert")
        if self.authenticator == "token":
            names = ("token", *names)
        if self.oauth_client_id is not None:
            names = (*names, "oauth_client_id")
        if self.session_name is not None:
            names = (*names, "session_name")
        for name in names:
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip() or "\x00" in value:
                raise DbtRuntimeError(
                    f"{name} must be a non-empty string without NUL characters"
                )
        if "=" in self.database or self.database.startswith(
            ("postgres://", "postgresql://")
        ):
            raise DbtRuntimeError(
                "database must be a database name, not a connection string"
            )
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise DbtRuntimeError("port must be an integer between 1 and 65535")
        if type(self.connect_timeout) is not int or self.connect_timeout < 1:
            raise DbtRuntimeError("connect_timeout must be a positive integer")
        if not isinstance(self.attach_databases, list) or any(
            not isinstance(name, str)
            or not name.strip()
            or "\x00" in name
            or "=" in name
            or name.startswith(("md:", "postgres://", "postgresql://"))
            for name in self.attach_databases
        ):
            raise DbtRuntimeError(
                "attach_databases must be a list of non-empty database names, not connection strings"
            )
        if not isinstance(self.settings, dict) or any(
            not isinstance(name, str)
            or not name.strip()
            or "\x00" in name
            or not isinstance(value, (str, bool, int, float))
            or (isinstance(value, str) and "\x00" in value)
            for name, value in self.settings.items()
        ):
            raise DbtRuntimeError(
                "settings must be a mapping of non-empty setting names to string, boolean, or numeric values, without NUL characters"
            )
        if isinstance(self.flights, dict):
            self.flights = FlightConfig.from_dict(self.flights)
        elif self.flights is not None and not isinstance(self.flights, FlightConfig):
            raise DbtRuntimeError("flights must be a mapping of Flight settings")

    @property
    def resolved_sslrootcert(self):
        return certifi.where() if self.sslrootcert == "certifi" else self.sslrootcert

    @property
    def type(self):
        return "motherduck"

    @property
    def unique_field(self):
        return self.host

    def _connection_keys(self):
        return (
            "host",
            "authenticator",
            "oauth_client_id",
            "port",
            "database",
            "schema",
            "sslrootcert",
            "connect_timeout",
            "session_name",
            "attach_databases",
            "settings",
        )
