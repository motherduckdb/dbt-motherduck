import json

import pytest

from tests.unit.helpers import invoke_dbt


@pytest.mark.parametrize(
    "level, placement, options, expected",
    [
        (
            level,
            placement,
            {"external_location": "read_csv('s3://unused/events.csv')"},
            "read_csv('s3://unused/events.csv')",
        )
        for level in ("source", "table")
        for placement in ("meta", "config_meta", "config")
    ]
    + [
        ("table", None, {}, '"unused"."main"."events"'),
        (
            "table",
            "meta",
            {"external_location": "s3://unused/{name}/*.parquet"},
            "'s3://unused/events/*.parquet'",
        ),
        (
            "table",
            "meta",
            {"external_location": "https://example.com/{identifier}.csv"},
            "'https://example.com/events.csv'",
        ),
        (
            "table",
            "meta",
            {"external_location": "'s3://unused/{name}.json'"},
            "'s3://unused/events.json'",
        ),
        (
            "table",
            "meta",
            {
                "external_location": "read_parquet(['s3://unused/a.parquet', 's3://unused/b.parquet'])"
            },
            "read_parquet(['s3://unused/a.parquet', 's3://unused/b.parquet'])",
        ),
        (
            "table",
            "meta",
            {"external_location": "s3://unused/%(name)s.csv", "formatter": "oldstyle"},
            "'s3://unused/events.csv'",
        ),
        (
            "table",
            "config_meta",
            {"external_location": "s3://unused/${name}.csv", "formatter": "template"},
            "'s3://unused/events.csv'",
        ),
        (
            "table",
            "meta",
            {"external_location": "s3://unused/{folder}/{name}.csv", "folder": "daily"},
            "'s3://unused/daily/events.csv'",
        ),
        (
            "table",
            "meta",
            {
                "external_location": "read_csv('s3://unused/events.csv', types={'id': 'INTEGER'})",
                "formatter": "oldstyle",
            },
            "read_csv('s3://unused/events.csv', types={'id': 'INTEGER'})",
        ),
        (
            "table",
            "meta",
            {"external_location": "s3://unused/events.csv", "formatter": "unknown"},
            None,
        ),
    ],
)
def test_external_source_compilation(cli_project, level, placement, options, expected):
    (cli_project / "dbt_project.yml").write_text(
        "name: source_validation\nversion: '1.0'\nconfig-version: 2\nprofile: test\n"
    )
    source = {"name": "upstream", "schema": "main", "tables": [{"name": "events"}]}
    if level == "table" and placement is not None:
        source["meta"] = {"external_location": "s3://unused/parent.parquet"}
    target = source if level == "source" else source["tables"][0]
    if placement == "config_meta":
        target["config"] = {"meta": options}
    elif placement is not None:
        target[placement] = options
    (cli_project / "models" / "sources.yml").write_text(
        json.dumps({"version": 2, "sources": [source]})
    )
    (cli_project / "models" / "model.sql").write_text(
        "select * from {{ source('upstream', 'events') }}"
    )
    code, output = invoke_dbt(
        cli_project,
        "compile",
        "--no-partial-parse",
        "--no-populate-cache",
        "--no-introspect",
    )
    if expected is None:
        assert code != 0
        assert "Formatter unknown not recognized" in output
        return
    assert code == 0, output
    manifest = json.loads((cli_project / "target" / "manifest.json").read_text())
    assert (
        manifest["nodes"]["model.source_validation.model"]["compiled_code"]
        == f"select * from {expected}"
    )
