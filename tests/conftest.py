import os
import re
import shutil
import tempfile
import time
from contextlib import contextmanager
from itertools import count
from pathlib import Path

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
def temporary_database(admin_connection, database_type, test_run_id, pytestconfig):
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
        deferred = kind == "ducklake" and defer_ducklake_drop(pytestconfig, name)
        try:
            admin_connection.execute(
                sql.SQL("ATTACH IF NOT EXISTS {}").format(sql.Literal(f"md:{name}"))
            )
            yield name
        finally:
            if not deferred:
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


# Temporary workaround for a bug: drop all ducklake databases once all workers are done

DEFERRED_DROPS = pytest.StashKey[Path]()


def defer_ducklake_drop(config, name):
    if os.environ.get("MOTHERDUCK_TEST_DEFER_DUCKLAKE_DROPS", "1") == "0":
        return False
    # Recorded at creation, so the sweep also covers a worker that dies before teardown.
    worker = os.environ.get("PYTEST_XDIST_WORKER", "main")
    with open(config.stash[DEFERRED_DROPS] / f"{worker}.txt", "a") as ledger:
        ledger.write(f"{name}\n")
    return True


def pytest_configure(config):
    workerinput = getattr(config, "workerinput", None)
    if workerinput is None:
        config.stash[DEFERRED_DROPS] = Path(
            tempfile.mkdtemp(prefix="dbt-motherduck-deferred-drops-")
        )
    else:
        config.stash[DEFERRED_DROPS] = Path(workerinput["deferred_drops"])


@pytest.hookimpl(optionalhook=True)
def pytest_configure_node(node):
    node.workerinput["deferred_drops"] = str(node.config.stash[DEFERRED_DROPS])


def pytest_sessionfinish(session):
    # Workers return here before the controller's sweep; only the controller runs
    # after every worker has finished.
    if hasattr(session.config, "workerinput"):
        return
    directory = session.config.stash[DEFERRED_DROPS]
    try:
        names = sorted(
            {
                name
                for ledger in directory.glob("*.txt")
                for name in ledger.read_text().splitlines()
                if name
            }
        )
        undropped = drop_deferred_databases(names) if names else []
    finally:
        shutil.rmtree(directory, ignore_errors=True)
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if undropped and reporter is not None:
        reporter.write_line(
            "Could not drop deferred DuckLake databases: " + ", ".join(undropped),
            yellow=True,
        )


def drop_deferred_databases(names):
    token = os.environ.get("DBT_ENV_SECRET_MOTHERDUCK_TOKEN")
    if not token:
        return names
    # Mirrors provisioning_profile_data and admin_connection; fixtures are not
    # available this late in the session.
    credentials = MotherDuckCredentials.from_dict(
        {
            "type": "motherduck",
            "host": os.environ.get(
                "MOTHERDUCK_PG_HOST", "pg.us-east-1-aws.motherduck.com"
            ),
            "database": "md:",
            "schema": "main",
            "token": token,
            "connect_timeout": 30,
        }
    )
    undropped = []
    connection = None
    for name in names:
        deadline = time.monotonic() + 120
        while True:
            try:
                if connection is None:
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
                connection.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name))
                )
                break
            except psycopg.Error:
                # Each drop can itself abort the duckling; retry once its
                # replacement is up, which has taken 12-31 s.
                if connection is not None:
                    connection.close()
                    connection = None
                if time.monotonic() > deadline:
                    undropped.append(name)
                    break
                time.sleep(5)
    if connection is not None:
        connection.close()
    return undropped
