import json

import certifi
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.motherduck.credentials import MotherDuckCredentials
from tests.unit.helpers import invoke_dbt


def test_defaults():
    credentials = MotherDuckCredentials(
        host="example.com", token="secret", database="db", schema="main"
    )
    assert credentials.port == 5432
    assert credentials.user == "postgres"
    assert credentials.sslrootcert == certifi.where()
    assert credentials.connect_timeout == 240
    assert credentials.attach_databases == []
    assert credentials.settings == {}


@pytest.mark.parametrize("sslrootcert", ["system", "/path/to/custom-ca.pem"])
def test_certificate_override(sslrootcert):
    credentials = MotherDuckCredentials.from_dict(
        {
            "host": "example.com",
            "token": "secret",
            "database": "db",
            "schema": "main",
            "sslrootcert": sslrootcert,
        }
    )
    assert credentials.sslrootcert == sslrootcert


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
        {"settings": None},
        {"settings": []},
        {"settings": {"": False}},
        {"settings": {1: False}},
        {"settings": {"TimeZone": None}},
        {"settings": {"TimeZone": "UTC\x00"}},
        {"settings": {"TimeZone": ["UTC"]}},
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
def test_required_credentials(cli_project, missing):
    profiles_file = cli_project / "profiles.yml"
    profiles = json.loads(profiles_file.read_text())
    del profiles["test"]["outputs"]["test"][missing]
    profiles_file.write_text(json.dumps(profiles))
    code, output = invoke_dbt(cli_project, "parse")
    assert code != 0, output
    assert missing in output
    assert "required" in output.lower()


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


def test_settings_exposed_in_target(cli_project):
    profiles_file = cli_project / "profiles.yml"
    profiles = json.loads(profiles_file.read_text())
    profiles["test"]["outputs"]["test"]["settings"] = {
        "preserve_insertion_order": False,
        "TimeZone": "Europe/Amsterdam",
        "custom_setting": 7,
    }
    profiles_file.write_text(json.dumps(profiles))
    (cli_project / "models" / "settings.sql").write_text(
        "select insertion_order: {{ target.settings.preserve_insertion_order | lower }}, "
        "time_zone: '{{ target.settings.TimeZone }}', "
        "custom: {{ target.settings.custom_setting }}"
    )
    code, output = invoke_dbt(
        cli_project, "compile", "--no-populate-cache", "--no-introspect"
    )
    assert code == 0, output
    manifest = json.loads((cli_project / "target" / "manifest.json").read_text())
    assert manifest["nodes"]["model.cli_test.settings"]["compiled_code"] == (
        "select insertion_order: false, time_zone: 'Europe/Amsterdam', custom: 7"
    )
