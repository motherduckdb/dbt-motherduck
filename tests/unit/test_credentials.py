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
    ],
)
def test_invalid_credentials(changes):
    values = dict(host="example.com", token="secret", database="db", schema="main")
    with pytest.raises(DbtRuntimeError):
        MotherDuckCredentials(**(values | changes))


@pytest.mark.parametrize("missing", ["host", "token", "database", "schema"])
def test_required_credentials(missing):
    values = dict(host="example.com", token="secret", database="db", schema="main")
    del values[missing]
    with pytest.raises(TypeError):
        MotherDuckCredentials(**values)
