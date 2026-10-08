import pytest
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)

from dbt.adapters.motherduck.impl import MotherDuckAdapter


@pytest.mark.parametrize("level", ["column", "model"])
@pytest.mark.parametrize(
    "target, expected_target",
    [
        ("parent", "parent"),
        ("schema.parent", "schema.parent"),
        ("db.schema.parent", "schema.parent"),
        ('"db"."schema"."parent"', '"schema"."parent"'),
        ('"parent.with.dot"', '"parent.with.dot"'),
        ('"schema.with.dot"."parent"', '"schema.with.dot"."parent"'),
        ('"schema"."parent.with.dot"', '"schema"."parent.with.dot"'),
        ('"db.with.dot"."schema"."parent"', '"schema"."parent"'),
        (
            '"db.with.dot"."schema.with.dot"."parent.with.dot"',
            '"schema.with.dot"."parent.with.dot"',
        ),
        (
            '"db"."sche""ma.with.dot"."par""ent.with.dot"',
            '"sche""ma.with.dot"."par""ent.with.dot"',
        ),
    ],
)
def test_foreign_key_target_preserves_quoted_identifiers(
    level, target, expected_target
):
    kwargs = {
        "type": ConstraintType.foreign_key,
        "to": target,
        "to_columns": ["id"],
    }
    if level == "column":
        constraint = ColumnLevelConstraint(**kwargs)
        sql = MotherDuckAdapter.render_column_constraint(constraint)
        assert sql == f"references {expected_target} (id)"
    else:
        constraint = ModelLevelConstraint(columns=["parent_id"], **kwargs)
        sql = MotherDuckAdapter.render_model_constraint(constraint)
        assert sql == f"foreign key (parent_id) references {expected_target} (id)"
