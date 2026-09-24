# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# dbt/adapters/duckdb/relation.py (subquery alias policy).
from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation
from dbt_common.exceptions import DbtRuntimeError


@dataclass(frozen=True, eq=False, repr=False)
class MotherDuckRelation(BaseRelation):
    require_alias: bool = False

    @classmethod
    def create_from(cls, quoting, relation_config, **kwargs):
        meta = relation_config.config.get("meta") or {}
        source_meta = getattr(relation_config, "meta", None) or {}
        if "plugin" in meta or "plugin" in source_meta:
            raise DbtRuntimeError("Local plugins are not supported by dbt-motherduck")
        return super().create_from(quoting, relation_config, **kwargs)
