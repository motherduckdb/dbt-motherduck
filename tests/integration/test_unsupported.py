import pytest
from dbt.tests.util import run_dbt


class TestUnsupportedPython:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "unsupported.py": (
                "def model(dbt, session):\n"
                "    dbt.config(submission_method='local')\n"
                "    return session\n"
            ),
        }

    def test_local_python_rejected(self, project):
        # The Postgres endpoint cannot run Python. Flight is the only submission
        # method, and anything else fails while the model is built.
        result = run_dbt(["run"], expect_pass=False)
        message = result.results[0].message
        assert "submission_method 'local' is not supported" in message
        assert "MotherDuck Flights" in message


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
