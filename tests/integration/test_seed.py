# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/simple_seed/test_fast_seed.py.
from datetime import datetime, timezone
from decimal import Decimal

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


class TestSeedTypes:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "typed.csv": (
                "id,amount,ts,tz\n"
                "1,123456789.123456789,2025-01-02 03:04:05.123456,2025-01-02 03:04:05+02:00\n"
                "2,,,\n"
            )
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "test": {
                    "typed": {
                        "+column_types": {
                            "id": "INTEGER",
                            "amount": "DECIMAL(20,9)",
                            "ts": "TIMESTAMP",
                            "tz": "TIMESTAMPTZ",
                        }
                    }
                }
            }
        }

    def test_scalar_types_and_nulls(self, project):
        relation = f'"{project.database}"."{project.test_schema}".typed'
        for args in (["seed"], ["seed", "--full-refresh"]):
            run_dbt(args)
            assert project.run_sql(
                f"SELECT * FROM {relation} ORDER BY id", fetch="all"
            ) == [
                (
                    1,
                    Decimal("123456789.123456789"),
                    datetime(2025, 1, 2, 3, 4, 5, 123456),
                    datetime(2025, 1, 2, 1, 4, 5, tzinfo=timezone.utc),
                ),
                (2, None, None, None),
            ]
