from types import SimpleNamespace

import pytest

from dbt.adapters.motherduck.connections import MotherDuckConnectionManager


def cursor(statusmessage, command_tuples):
    return SimpleNamespace(
        statusmessage=statusmessage,
        pgresult=SimpleNamespace(command_tuples=command_tuples),
    )


@pytest.mark.parametrize(
    "statusmessage, command_tuples, code, rows_affected",
    [
        ("INSERT 0 5", 5, "INSERT", 5),
        ("UPDATE 3", 3, "UPDATE", 3),
        ("DELETE 0", 0, "DELETE", 0),
        ("MERGE 3", 3, "MERGE", 3),
        ("SELECT 5", 5, "SELECT", 5),
        ("CREATE TABLE", None, "CREATE", None),
        (None, None, "OK", None),
    ],
)
def test_get_response_reads_command_tag(
    statusmessage, command_tuples, code, rows_affected
):
    response = MotherDuckConnectionManager.get_response(
        cursor(statusmessage, command_tuples)
    )
    assert response.code == code
    assert response.rows_affected == rows_affected
    assert response._message == (statusmessage or "OK")
