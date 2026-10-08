import json

import pytest


@pytest.fixture
def cli_project(tmp_path):
    (tmp_path / "models").mkdir()
    (tmp_path / "dbt_project.yml").write_text(
        "name: cli_test\nversion: '1.0'\nconfig-version: 2\nprofile: test\n"
    )
    (tmp_path / "profiles.yml").write_text(
        json.dumps(
            {
                "test": {
                    "target": "test",
                    "outputs": {
                        "test": {
                            "type": "motherduck",
                            "host": "127.0.0.1",
                            "port": 1,
                            "database": "unused",
                            "schema": "main",
                            "token": "unused",
                            "connect_timeout": 1,
                            "threads": 1,
                        }
                    },
                }
            }
        )
    )
    return tmp_path
