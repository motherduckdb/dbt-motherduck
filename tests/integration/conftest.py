import os
from argparse import Namespace
from contextlib import contextmanager
from multiprocessing import get_context
from uuid import uuid4

import psycopg
import pytest
from dbt.adapters.factory import AdapterContainer
from dbt.config.profile import Profile
from dbt.config.renderer import ProfileRenderer
from dbt.flags import get_flags, set_flags, set_from_args
from dbt.tests.fixtures.project import adapter as adapter
from psycopg import sql

from dbt.adapters.motherduck.credentials import MotherDuckCredentials


@pytest.fixture(scope="session")
def dbt_flags():
    previous = get_flags()
    set_from_args(Namespace(send_anonymous_usage_stats=False), None)
    try:
        yield
    finally:
        set_flags(previous)


@pytest.fixture(scope="session")
def provisioning_profile_data():
    token = os.environ.get("DBT_ENV_SECRET_MOTHERDUCK_TOKEN")
    if not token:
        pytest.skip("Set DBT_ENV_SECRET_MOTHERDUCK_TOKEN to run endpoint tests")
    return {
        "type": "motherduck",
        "host": os.environ.get("MOTHERDUCK_HOST", "pg.us-east-1-aws.motherduck.com"),
        "database": os.environ["MOTHERDUCK_DATABASE"],
        "schema": "main",
        "token": token,
        "sslrootcert": os.environ.get("MOTHERDUCK_SSLROOTCERT", "system"),
        "threads": 2,
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
        options="-c attach_mode=single",
        autocommit=True,
    )
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture(scope="session")
def temporary_database(admin_connection, database_type):
    @contextmanager
    def create(*, prefix="dbt_motherduck_test_", kind=None):
        kind = database_type if kind is None else kind
        if kind not in ("native", "ducklake"):
            raise ValueError(f"Unsupported test database type: {kind}")
        name = prefix + uuid4().hex
        options = sql.SQL(" (TYPE DUCKLAKE)" if kind == "ducklake" else "")
        admin_connection.execute(
            sql.SQL("CREATE DATABASE {}{}").format(sql.Identifier(name), options)
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


@pytest.fixture
def connection_adapter(profile_data, dbt_flags):
    profile = Profile.from_raw_profile_info(
        {"target": "test", "outputs": {"test": profile_data}},
        "motherduck_test",
        ProfileRenderer(),
    )
    container = AdapterContainer()
    container.load_plugin("motherduck")
    container.register_adapter(profile, get_context("spawn"))
    adapter = container.lookup_adapter("motherduck")
    try:
        yield adapter
    finally:
        container.reset_adapters()


@pytest.fixture(scope="class")
def dbt_profile_target(profile_data):
    return {
        **profile_data,
        "token": "{{ env_var('DBT_ENV_SECRET_MOTHERDUCK_TOKEN') }}",
    }
