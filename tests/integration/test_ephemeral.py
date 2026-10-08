# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_ephemeral.py.
import pytest
from dbt.tests.adapter.ephemeral.test_ephemeral import (
    BaseEphemeral,
    BaseEphemeralMulti,
    ephemeral_errors__base__base_copy_sql,
    ephemeral_errors__base__base_sql,
    ephemeral_errors__dependent_sql,
    models_n__ephemeral_level_two_sql,
    models_n__ephemeral_sql,
    models_n__root_view_sql,
    models_n__source_table_sql,
)
from dbt.tests.util import check_relations_equal, run_dbt


class TestEphemeralMulti(BaseEphemeralMulti):
    def test_ephemeral_multi(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3

        check_relations_equal(project.adapter, ["seed", "dependent"])
        check_relations_equal(project.adapter, ["seed", "double_dependent"])
        check_relations_equal(project.adapter, ["seed", "super_dependent"])


class TestEphemeralNested(BaseEphemeral):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral_level_two.sql": models_n__ephemeral_level_two_sql,
            "root_view.sql": models_n__root_view_sql,
            "ephemeral.sql": models_n__ephemeral_sql,
            "source_table.sql": models_n__source_table_sql,
        }

    def test_ephemeral_nested(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        check_relations_equal(project.adapter, ["source_table", "root_view"])


class TestEphemeralErrorHandling(BaseEphemeral):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dependent.sql": ephemeral_errors__dependent_sql,
            "base": {
                "base.sql": ephemeral_errors__base__base_sql,
                "base_copy.sql": ephemeral_errors__base__base_copy_sql,
            },
        }

    def test_ephemeral_error_handling(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "skipped"
        assert "Compilation Error" in results[0].message
