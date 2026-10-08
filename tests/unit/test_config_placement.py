import json

import pytest

from tests.unit.helpers import invoke_dbt


@pytest.fixture
def config_project(cli_project):
    (cli_project / "dbt_project.yml").write_text(
        "name: config_placement\nversion: '1.0'\nconfig-version: 2\nprofile: test\n"
    )
    (cli_project / "models" / "model.sql").write_text(
        "{{ config(var('model_config', {})) }} select id: 1"
    )
    return cli_project


def assert_placement_error(code, output, key):
    assert code != 0, output
    assert "Parsing Error in model model" in output
    assert f"must be set under config.meta in dbt-motherduck: {key}" in output


@pytest.mark.parametrize(
    "materialization, key, value",
    [
        ("table_function", "parameters", "start_date"),
        ("incremental", "merge_update_condition", "false"),
        ("incremental", "merge_insert_condition", "false"),
        ("incremental", "merge_on_using_columns", ["id"]),
        (
            "incremental",
            "merge_update_set_expressions",
            {"id": "DBT_INTERNAL_SOURCE.id"},
        ),
        (
            "incremental",
            "merge_clauses",
            {"when_matched": [{"action": "update", "mode": "by_name"}]},
        ),
        ("incremental", "merge_returning_columns", ["id"]),
        ("table", "partitioned_by", "id"),
        ("table", "partition_by", "id"),
        ("table", "sorted_by", "id"),
        ("table", "sort_by", "id"),
    ],
)
def test_root_config_rejected_during_parse(config_project, materialization, key, value):
    model_config = {"materialized": materialization, key: value}
    if materialization == "incremental":
        model_config.update(incremental_strategy="merge", unique_key="id")
    assert_placement_error(
        *invoke_dbt(
            config_project,
            "parse",
            "--vars",
            json.dumps({"model_config": model_config}),
        ),
        key,
    )


@pytest.mark.parametrize("value", [None, False, "", [], {}])
def test_meta_does_not_hide_root_config(config_project, value):
    assert_placement_error(
        *invoke_dbt(
            config_project,
            "parse",
            "--vars",
            json.dumps(
                {
                    "model_config": {
                        "materialized": "table_function",
                        "parameters": value,
                        "meta": {"parameters": "start_date"},
                    }
                }
            ),
        ),
        "parameters",
    )


@pytest.mark.parametrize("placement", ["project", "properties"])
def test_inherited_root_config_rejected(config_project, placement):
    if placement == "project":
        (config_project / "dbt_project.yml").write_text(
            "name: config_placement\nversion: '1.0'\nconfig-version: 2\nprofile: test\n"
            "models:\n  config_placement:\n    +partitioned_by: id\n"
        )
    else:
        (config_project / "models" / "schema.yml").write_text(
            "version: 2\nmodels:\n  - name: model\n    config:\n      partitioned_by: id\n"
        )
    assert_placement_error(*invoke_dbt(config_project, "parse"), "partitioned_by")


def test_literal_config_rejected(config_project):
    (config_project / "models" / "model.sql").write_text(
        "{{ config(materialized='table', partition_by='id') }} select id: 1"
    )
    assert_placement_error(*invoke_dbt(config_project, "parse"), "partition_by")


def test_snapshot_config_rejected(config_project):
    (config_project / "snapshots").mkdir()
    (config_project / "snapshots" / "snapshot_model.sql").write_text(
        "{% snapshot snapshot_model %}\n"
        "{{ config(strategy='timestamp', unique_key='id', updated_at='updated_at', "
        "target_schema='main', partition_by='id') }}\n"
        "select id: 1, updated_at: current_timestamp\n"
        "{% endsnapshot %}\n"
    )
    code, output = invoke_dbt(config_project, "parse")
    assert code != 0, output
    assert "Parsing Error in snapshot snapshot_model" in output
    assert "must be set under config.meta in dbt-motherduck: partition_by" in output


def test_unselected_root_config_rejected(config_project):
    (config_project / "models" / "model.sql").write_text(
        "{{ config(materialized='table', partition_by='id') }} select id: 1"
    )
    (config_project / "models" / "valid.sql").write_text("select id: 1")
    assert_placement_error(
        *invoke_dbt(config_project, "compile", "--select", "valid"), "partition_by"
    )


def test_meta_config_accepted(config_project):
    code, output = invoke_dbt(
        config_project,
        "parse",
        "--vars",
        json.dumps(
            {
                "model_config": {
                    "materialized": "table",
                    "indexes": [{"columns": ["id"]}],
                    "meta": {"partitioned_by": "id", "merge_update_condition": "false"},
                }
            }
        ),
    )
    assert code == 0, output


def test_partial_parse_rejects_new_root_config(config_project):
    code, output = invoke_dbt(config_project, "parse", "--partial-parse")
    assert code == 0, output
    (config_project / "models" / "model.sql").write_text(
        "{{ config(materialized='table', partition_by='id') }} select id: 1"
    )
    code, output = invoke_dbt(config_project, "parse", "--partial-parse")
    assert_placement_error(code, output, "partition_by")
