import pytest

from dbt.adapters.motherduck.relation import MotherDuckRelation


@pytest.mark.parametrize(
    "limit, expected",
    [
        (None, '"db"."schema"."model"'),
        (0, '(select * from "db"."schema"."model" where false limit 0)'),
        (2, '(select * from "db"."schema"."model" limit 2)'),
    ],
)
def test_limited_relation_does_not_add_alias(limit, expected):
    relation = MotherDuckRelation.create(
        database="db", schema="schema", identifier="model", limit=limit
    )
    assert str(relation) == expected
    assert not relation.incorporate(path={"identifier": "replacement"}).require_alias
