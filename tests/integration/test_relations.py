# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_caching.py, test_changing_relation_type.py,
# test_empty.py, test_concurrency.py, and aliases/test_aliases.py.
import pytest
from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliasErrors,
    BaseAliases,
    BaseSameAliasDifferentSchemas,
)
from dbt.tests.adapter.concurrency.test_concurrency import (
    TestConcurenncy as BaseConcurrency,
)
from dbt.tests.adapter.empty.test_empty import (
    BaseTestEmpty,
    BaseTestEmptyInlineSourceRef,
)
from dbt.tests.adapter.relations.test_changing_relation_type import (
    BaseChangeRelationTypeValidator,
)
from dbt.tests.util import relation_from_name, run_dbt
from dbt_common.exceptions import CompilationError


class TestAliases(BaseAliases):
    pass


class TestAliasErrors(BaseAliasErrors):
    def test_alias_dupe_throws_exeption(self, project):
        # The upstream assertion raises TypeError before it ever runs dbt.
        with pytest.raises(CompilationError, match="identical database representation"):
            run_dbt(["run"])


class TestSameAliasDifferentSchemas(BaseSameAliasDifferentSchemas):
    pass


class TestConcurrency(BaseConcurrency):
    pass


class TestEmpty(BaseTestEmpty):
    pass


class TestEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    pass


class TestChangeRelationType(BaseChangeRelationTypeValidator):
    pass


class TestMissingRelationColumns:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "column_probe.sql": """
{% if execute %}
  {% set missing = this.incorporate(path={'identifier': 'intentionally_missing_column_probe'}) %}
  {% set columns = adapter.get_columns_in_relation(missing) %}
  {% set probe = run_query('select answer: 42') %}
  select answer: {{ probe.rows[0][0] }}, missing_columns: {{ columns | length }}
{% else %}
  select answer: 42, missing_columns: 0
{% endif %}
"""
        }

    def test_missing_column_lookup_allows_subsequent_query(self, project):
        run_dbt(["run"])
        relation = relation_from_name(project.adapter, "column_probe")
        assert project.run_sql(f"select * from {relation}", fetch="all") == [(42, 0)]
