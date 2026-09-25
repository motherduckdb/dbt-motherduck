# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_basic.py.
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection


class TestSimpleMaterializations(BaseSimpleMaterializations):
    pass


class TestIncremental(BaseIncremental):
    pass


class TestValidateConnection(BaseValidateConnection):
    pass


class TestAdapterMethods(BaseAdapterMethod):
    pass


class TestEmpty(BaseEmpty):
    pass


class TestEphemeral(BaseEphemeral):
    pass


class TestGenericTests(BaseGenericTests):
    pass


class TestIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass


class TestSingularTests(BaseSingularTests):
    pass


class TestSingularTestsEphemeral(BaseSingularTestsEphemeral):
    pass


class TestSnapshotCheckCols(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestamp(BaseSnapshotTimestamp):
    pass
