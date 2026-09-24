from dbt.adapters.base.relation import BaseRelation
from dbt_common.exceptions import DbtRuntimeError


class MotherDuckRelation(BaseRelation):
    @classmethod
    def create_from(cls, quoting, relation_config, **kwargs):
        meta = relation_config.config.get("meta") or {}
        source_meta = getattr(relation_config, "meta", None) or {}
        if "plugin" in meta or "plugin" in source_meta:
            raise DbtRuntimeError("Local plugins are not supported by dbt-motherduck")
        return super().create_from(quoting, relation_config, **kwargs)
