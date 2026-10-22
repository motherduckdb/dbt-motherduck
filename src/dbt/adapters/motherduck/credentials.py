from dataclasses import dataclass, field
from typing import Any

import certifi
from dbt.adapters.contracts.connection import Credentials
from dbt_common.exceptions import DbtRuntimeError

AUTHENTICATORS = ("token", "externalbrowser")


@dataclass
class MotherDuckCredentials(Credentials):
    host: str
    token: str | None = field(default=None, repr=False)
    authenticator: str = "token"
    port: int = 5432
    user: str = "postgres"
    sslrootcert: str = field(default_factory=certifi.where)
    connect_timeout: int = 240
    attach_databases: list[str] = field(default_factory=list)
    settings: dict[str, Any] = field(default_factory=dict)

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
        names = ("host", "database", "schema", "user", "sslrootcert")
        if self.authenticator == "token":
            names = ("token", *names)
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
            "port",
            "user",
            "database",
            "schema",
            "sslrootcert",
            "connect_timeout",
            "attach_databases",
            "settings",
        )
