from importlib import metadata, resources
from pathlib import Path

from dbt.adapters.sql import SQLAdapter

import dbt.adapters.motherduck
from dbt.include.motherduck import PACKAGE_PATH


def test_adapter_namespace_coexists_with_dbt_framework():
    assert dbt.adapters.motherduck.__name__ == "dbt.adapters.motherduck"
    assert SQLAdapter.__module__ == "dbt.adapters.sql.impl"
    assert metadata.version("dbt-motherduck")


def test_bundled_dbt_project():
    project = resources.files("dbt.include.motherduck").joinpath("dbt_project.yml")
    assert project.is_file()
    assert "name: dbt_motherduck" in project.read_text(encoding="utf-8")
    assert (Path(PACKAGE_PATH) / "dbt_project.yml").read_text(encoding="utf-8") == (
        project.read_text(encoding="utf-8")
    )
