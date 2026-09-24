import os
from argparse import Namespace
from multiprocessing import get_context

import pytest
from dbt.adapters.factory import AdapterContainer
from dbt.config.profile import Profile
from dbt.config.renderer import ProfileRenderer
from dbt.flags import get_flags, set_flags, set_from_args


@pytest.fixture(scope="session")
def dbt_flags():
    previous = get_flags()
    set_from_args(Namespace(send_anonymous_usage_stats=False), None)
    try:
        yield
    finally:
        set_flags(previous)


@pytest.fixture
def profile_data():
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


@pytest.fixture
def adapter(profile_data, dbt_flags):
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
