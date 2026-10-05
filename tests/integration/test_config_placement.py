import json

import pytest
from dbt.exceptions import ParsingError
from dbt.tests.util import relation_from_name, run_dbt


class TestConfigPlacement:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": """
{{ config(var('model_config', {})) }}
select id: 1, value: {{ var('value', 1) }}
""",
        }

    def test_misplaced_merge_preserves_existing_rows(self, project):
        model_config = {
            "materialized": "incremental",
            "incremental_strategy": "merge",
            "unique_key": "id",
            "meta": {"merge_update_condition": "false"},
        }
        run_dbt(["run", "--vars", json.dumps({"model_config": model_config})])
        target = relation_from_name(project.adapter, "model")
        del model_config["meta"]
        model_config["merge_update_condition"] = "false"
        for flags in ([], ["--full-refresh"]):
            with pytest.raises(
                ParsingError,
                match="must be set under config.meta in dbt-motherduck: merge_update_condition",
            ):
                run_dbt(
                    [
                        "run",
                        *flags,
                        "--vars",
                        json.dumps({"model_config": model_config, "value": 2}),
                    ],
                    expect_pass=False,
                )
            assert project.run_sql(f"select value from {target}", fetch="all") == [(1,)]
