import pytest
from dbt.tests.util import get_connection, relation_from_name, run_dbt


class TestRelationDiscovery:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": "{{ config(materialized='incremental', unique_key='id') }} select id: 1",
            "unselected.sql": "{{ config(materialized='table', schema='other') }} select id: 2",
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"quoting": {"identifier": False, "schema": False}}

    @pytest.mark.parametrize(
        "flags", [[], ["--no-populate-cache"], ["--cache-selected-only"]]
    )
    def test_rerun_discovers_existing_relations(self, project, flags):
        for _ in range(2):
            run_dbt([*flags, "run", "--select", "model"])
            with get_connection(project.adapter):
                relation = project.adapter.get_relation(
                    database=project.database,
                    schema=project.test_schema.upper(),
                    identifier="MODEL",
                )
                assert relation is not None
                assert (
                    project.adapter.get_relation(
                        database=project.database,
                        schema=project.test_schema + "_other",
                        identifier="unselected",
                    )
                    is None
                )
            relation = relation_from_name(project.adapter, "model")
            assert project.run_sql(f"select * from {relation}", fetch="all") == [(1,)]
