import pytest
from dbt.tests.util import relation_from_name, run_dbt


class TestDeleteInsert:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "changes.sql": """
{{ config(materialized='incremental',
          incremental_strategy=var('strategy', 'delete+insert'),
          unique_key=var('key', none),
          incremental_predicates=[var('predicate', 'id = 1')]) }}
{% if is_incremental() %}
select * from (values (1, 'a', 100), (2, 'a', 200)) t(id, part, value)
{% else %}
select * from (values (1, 'a', 10), (1, 'b', 11), (2, 'a', 20)) t(id, part, value)
{% endif %}
"""
        }

    @pytest.mark.parametrize(
        "variables, expected",
        [
            ("{key: id}", [(1, "a", 100), (2, "a", 20), (2, "a", 200)]),
            (
                "{key: [id, part], predicate: 'DBT_INCREMENTAL_TARGET.id = 1'}",
                [(1, "a", 100), (1, "b", 11), (2, "a", 20), (2, "a", 200)],
            ),
            (
                "{key: [id, part], strategy: default, predicate: 'DBT_INCREMENTAL_TARGET.id = 1'}",
                [(1, "a", 100), (1, "b", 11), (2, "a", 20), (2, "a", 200)],
            ),
            (
                "{}",
                [
                    (1, "a", 10),
                    (1, "a", 100),
                    (1, "b", 11),
                    (2, "a", 20),
                    (2, "a", 200),
                ],
            ),
        ],
    )
    def test_keys_and_predicates(self, project, variables, expected):
        run_dbt(["run", "--full-refresh", "--vars", variables])
        run_dbt(["run", "--vars", variables])
        target = relation_from_name(project.adapter, "changes")
        assert (
            project.run_sql(
                f"SELECT * FROM {target} ORDER BY id, part, value", fetch="all"
            )
            == expected
        )
