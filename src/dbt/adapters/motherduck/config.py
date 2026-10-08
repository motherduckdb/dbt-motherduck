# Index configuration adapted from dbt-duckdb commit
# acdab5dfc85715572bbf43fcf2b318a5ed1bfa16, dbt/adapters/duckdb/impl.py.
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from dbt.adapters.base import AdapterConfig
from dbt.adapters.exceptions import IndexConfigError, IndexConfigNotDictError
from dbt_common.dataclass_schema import ValidationError, dbtClassMixin
from dbt_common.utils import encoding as dbt_encoding


@dataclass
class MotherDuckIndexConfig(dbtClassMixin):
    columns: List[str]
    unique: bool = False

    def render(self, relation):
        # We append the current timestamp to the index name because otherwise
        # the index will only be created on every other run. See
        # https://github.com/dbt-labs/dbt-core/issues/1945#issuecomment-576714925
        # for an explanation.
        now = datetime.now(timezone.utc).isoformat()
        inputs = self.columns + [
            relation.render(),
            str(self.unique),
            now,
        ]
        string = "_".join(inputs)
        return dbt_encoding.md5(string)

    @classmethod
    def parse(cls, raw_index) -> Optional["MotherDuckIndexConfig"]:
        if raw_index is None:
            return None
        try:
            cls.validate(raw_index)
            return cls.from_dict(raw_index)
        except ValidationError as exc:
            raise IndexConfigError(exc)
        except TypeError:
            raise IndexConfigNotDictError(raw_index)


@dataclass
class MotherDuckConfig(AdapterConfig):
    indexes: Optional[List[MotherDuckIndexConfig]] = None
