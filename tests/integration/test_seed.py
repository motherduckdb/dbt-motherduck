# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/simple_seed/test_fast_seed.py.
import pytest
from dbt.tests.adapter.simple_seed import seeds
from dbt.tests.adapter.simple_seed.test_seed import SeedUniqueDelimiterTestBase
from dbt.tests.util import run_dbt


class TestSeedWithUniqueDelimiter(SeedUniqueDelimiterTestBase):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        # The endpoint cannot resolve the INSERT while preparing a CREATE+INSERT batch.
        create_expected, insert_expected = seeds.seeds__expected_sql.split(";", 1)
        project.run_sql(create_expected)
        project.run_sql(insert_expected)

    def test_seed_with_unique_delimiter(self, project):
        """Testing correct run of seeds with a unique delimiter (pipe in this case)"""
        self._build_relations_for_test(project)
        self._check_relation_end_state(
            run_result=run_dbt(["seed"]), project=project, exists=True
        )
