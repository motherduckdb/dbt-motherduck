import os
import re
from contextlib import contextmanager
from itertools import count

import psycopg
import pytest
from psycopg import sql

from dbt.adapters.motherduck.credentials import MotherDuckCredentials

pytest_plugins = ["dbt.tests.fixtures.project"]

# Directory names under tests/ that become the marker for everything beneath them.
MARKER_DIRECTORIES = ("unit", "integration", "ecosystem")

# Database names travel over the PostgreSQL wire protocol, so keep the generated
# name well inside the 63-byte identifier limit that protocol assumes.
RUN_ID_MAX_LENGTH = 36


def pytest_addoption(parser):
    parser.addoption(
        "--database-type", choices=("native", "ducklake"), default="native"
    )


def pytest_collection_modifyitems(items):
    for item in items:
        category = item.path.relative_to(item.config.rootpath / "tests").parts[0]
        if category in MARKER_DIRECTORIES:
            item.add_marker(getattr(pytest.mark, category))
        for marker in item.iter_markers("skip_database_type"):
            if item.config.getoption("--database-type") in marker.args:
                item.add_marker(pytest.mark.skip(reason=marker.kwargs["reason"]))


def sanitize_run_id(raw):
    # Keep the tail: callers put the varying part (run number, matrix leg) last.
    cleaned = re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")
    return cleaned[-RUN_ID_MAX_LENGTH:].strip("_")


@pytest.fixture(scope="session")
def test_run_id():
    # Each xdist worker runs its own session and restarts the counter below, so the
    # worker has to be part of the name or workers would replace each other's databases.
    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    run_id = sanitize_run_id(f"{os.environ.get('MOTHERDUCK_TEST_RUN_ID', '')}-{worker}")
    if not run_id:
        pytest.fail(
            "Set MOTHERDUCK_TEST_RUN_ID to a value unique to this run. It names the "
            "disposable databases, so concurrent runs sharing a value collide, and "
            "databases left behind by an interrupted run are traceable to it.",
            pytrace=False,
        )
    return run_id


@pytest.fixture(scope="session")
def provisioning_profile_data():
    token = os.environ.get("DBT_ENV_SECRET_MOTHERDUCK_TOKEN")
    if not token:
        pytest.fail(
            "Set DBT_ENV_SECRET_MOTHERDUCK_TOKEN to run endpoint tests, or select "
            "only the unit tests with -m unit.",
            pytrace=False,
        )
    return {
        "type": "motherduck",
        "host": os.environ.get("MOTHERDUCK_PG_HOST", "pg.us-east-1-aws.motherduck.com"),
        # The workspace: the admin connection needs no database of its own, and
        # profile_data swaps in the disposable one for dbt.
        "database": "md:",
        "schema": "main",
        "token": token,
        "threads": 2,
        "connect_timeout": 30,
    }


@pytest.fixture(scope="session")
def database_type(request):
    return request.config.getoption("--database-type")


@pytest.fixture(scope="session")
def admin_connection(provisioning_profile_data):
    credentials = MotherDuckCredentials.from_dict(provisioning_profile_data)
    connection = psycopg.connect(
        host=credentials.host,
        port=credentials.port,
        user=credentials.user,
        password=credentials.token,
        dbname=credentials.database,
        sslmode="verify-full",
        sslrootcert=credentials.sslrootcert,
        connect_timeout=credentials.connect_timeout,
        autocommit=True,
    )
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture(scope="session")
def temporary_database(admin_connection, database_type, test_run_id):
    counter = count()

    @contextmanager
    def create(*, prefix="dbt_motherduck_test_", kind=None):
        kind = database_type if kind is None else kind
        if kind not in ("native", "ducklake"):
            raise ValueError(f"Unsupported test database type: {kind}")
        name = f"{prefix}{test_run_id}_{next(counter)}"
        options = sql.SQL(" (TYPE DUCKLAKE)" if kind == "ducklake" else "")
        admin_connection.execute(
            sql.SQL("CREATE OR REPLACE DATABASE {}{}").format(
                sql.Identifier(name), options
            )
        )
        try:
            admin_connection.execute(
                sql.SQL("ATTACH IF NOT EXISTS {}").format(sql.Literal(f"md:{name}"))
            )
            yield name
        finally:
            admin_connection.execute(
                sql.SQL("DROP DATABASE {}").format(sql.Identifier(name))
            )

    return create


@pytest.fixture(scope="session")
def test_database(temporary_database):
    with temporary_database() as name:
        yield name


@pytest.fixture(scope="session")
def profile_data(provisioning_profile_data, test_database):
    return {**provisioning_profile_data, "database": test_database}


@pytest.fixture(scope="class")
def dbt_profile_target(profile_data):
    return {
        **profile_data,
        "token": "{{ env_var('DBT_ENV_SECRET_MOTHERDUCK_TOKEN') }}",
    }
