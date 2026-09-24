# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# dbt/adapters/duckdb/relation.py and utils.py (source configuration).
from dataclasses import dataclass
from string import Template
from typing import Optional

from dbt.adapters.base.relation import BaseRelation
from dbt_common.exceptions import DbtRuntimeError


@dataclass(frozen=True, eq=False, repr=False)
class MotherDuckRelation(BaseRelation):
    require_alias: bool = False
    external: Optional[str] = None

    @classmethod
    def create_from(cls, quoting, relation_config, **kwargs):
        meta = relation_config.config.get("meta") or {}
        source_meta = getattr(relation_config, "meta", None) or {}
        if "plugin" in meta or "plugin" in source_meta:
            raise DbtRuntimeError("Local plugins are not supported by dbt-motherduck")
        if relation_config.resource_type == "source":
            options = {**source_meta, **meta, **relation_config.config.extra}
            if "plugin" in options:
                raise DbtRuntimeError(
                    "Local plugins are not supported by dbt-motherduck"
                )
            if "external_location" in options:
                location = options["external_location"]
                values = {
                    "name": relation_config.name,
                    "identifier": relation_config.identifier,
                    "schema": relation_config.schema,
                    "database": relation_config.database,
                    "tags": relation_config.tags or [],
                    **options,
                }
                formatter = options.get("formatter", "newstyle")
                if formatter == "newstyle":
                    location = location.format_map(values)
                elif formatter == "oldstyle":
                    location = location % values
                elif formatter == "template":
                    location = Template(location).substitute(values)
                else:
                    raise DbtRuntimeError(
                        f"Formatter {formatter} not recognized. Must be one of "
                        "'newstyle', 'oldstyle', or 'template'."
                    )
                if "(" not in location and not location.startswith("'"):
                    location = f"'{location}'"
                kwargs["external"] = location
        return super().create_from(quoting, relation_config, **kwargs)

    def render(self):
        return self.external if self.external else super().render()
