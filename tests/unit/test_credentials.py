import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.motherduck.credentials import MotherDuckCredentials


def test_defaults():
    credentials = MotherDuckCredentials(
        host="example.com", token="secret", database="db", schema="main"
    )
    assert credentials.port == 5432
    assert credentials.user == "postgres"
    assert credentials.sslrootcert == "system"
    assert credentials.connect_timeout == 10
    assert credentials.attach_databases == []


@pytest.mark.parametrize(
    "changes",
    [
        {"host": ""},
        {"token": " "},
        {"database": ""},
        {"database": "host=other-server"},
        {"database": "postgresql://other-server/db"},
        {"schema": ""},
        {"user": ""},
        {"sslrootcert": ""},
        {"port": 0},
        {"port": 65536},
        {"port": True},
        {"connect_timeout": 0},
        {"attach_databases": "other"},
        {"attach_databases": None},
        {"attach_databases": [""]},
        {"attach_databases": [" "]},
        {"attach_databases": [1]},
        {"attach_databases": ["other\x00"]},
        {"attach_databases": ["md:other"]},
        {"attach_databases": ["host=other-server"]},
    ],
)
def test_invalid_credentials(changes):
    values = {
        "host": "example.com",
        "token": "secret",
        "database": "db",
        "schema": "main",
    }
    with pytest.raises(DbtRuntimeError):
        MotherDuckCredentials(**(values | changes))


@pytest.mark.parametrize("missing", ["host", "token", "database", "schema"])
def test_required_credentials(missing):
    values = {
        "host": "example.com",
        "token": "secret",
        "database": "db",
        "schema": "main",
    }
    del values[missing]
    with pytest.raises(TypeError):
        MotherDuckCredentials(**values)


def test_attachment_names_round_trip():
    credentials = MotherDuckCredentials(
        host="example.com",
        token="secret",
        database="db",
        schema="main",
        attach_databases=["other", "MixedCase"],
    )
    assert MotherDuckCredentials.from_dict(credentials.to_dict()).attach_databases == [
        "other",
        "MixedCase",
    ]
