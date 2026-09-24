import pytest
from dbt.tests.util import run_dbt


class TestUnsupportedPython:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "unsupported.py": "def model(dbt, session):\n    return None\n",
        }

    def test_python_model_rejected(self, project):
        result = run_dbt(["run"], expect_pass=False)
        assert (
            "only supports languages ['sql']; got \"python\""
            in result.results[0].message
        )


class TestUnsupportedPlugin:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "unsupported.sql": "{{ config(meta={'plugin': 'local_loader'}) }} select 1 as id",
        }

    def test_plugin_rejected(self, project):
        from dbt_common.exceptions import DbtRuntimeError

        with pytest.raises(DbtRuntimeError, match="Local plugins are not supported"):
            run_dbt(["run"], expect_pass=False)
