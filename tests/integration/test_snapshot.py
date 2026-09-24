# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_simple_snapshot.py.
from dbt.tests.adapter.simple_snapshot.test_snapshot import (
    BaseSimpleSnapshot,
    BaseSnapshotCheck,
)


class TestSimpleSnapshotDuckDB(BaseSimpleSnapshot):
    pass


class TestSnapshotCheckDuckDB(BaseSnapshotCheck):
    pass
