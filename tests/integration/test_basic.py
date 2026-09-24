# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_basic.py.
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection


class TestSimpleMaterializations(BaseSimpleMaterializations):
    pass


class TestIncremental(BaseIncremental):
    pass


class TestValidateConnection(BaseValidateConnection):
    pass
