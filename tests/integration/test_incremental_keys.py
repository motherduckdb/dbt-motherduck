# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
# tests/functional/adapter/test_incremental.py.
import pytest
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
    BaseIncrementalOnSchemaChangeSetup,
)
from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)
from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
)
from dbt.tests.util import run_dbt, update_config_file


class TestUniqueKey(BaseIncrementalUniqueKey):
    def test__bad_unique_key_list(self, project):
        """expect compilation error from unique key not being a column"""

        (status, _exc) = self.fail_to_build_inc_missing_unique_key_column(
            incremental_model_name="not_found_unique_key_list"
        )

        assert status == RunStatus.Error


class TestIncrementalPredicates(BaseIncrementalPredicates):
    pass


models__incremental_append_new_columns_with_space = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

{% set string_type = dbt.type_string() %}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental()  %}

SELECT id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2,
       cast(field3 as {{string_type}}) as "field 3",
       cast(field4 as {{string_type}}) as "field 4"
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2
FROM source_data where id <= 3

{% endif %}
"""


class TestIncrementalOnSchemaChangeQuoting(BaseIncrementalOnSchemaChangeSetup):
    @pytest.fixture(scope="class")
    def models(self):
        models = dict(BaseIncrementalOnSchemaChange.models.__wrapped__(self))
        models["incremental_append_new_columns_with_space.sql"] = (
            models__incremental_append_new_columns_with_space
        )
        return models

    @pytest.mark.parametrize("quote_identifiers", [False, True])
    def test_identifier_quoting(self, project, quote_identifiers):
        update_config_file(
            {"quoting": {"identifier": quote_identifiers}}, "dbt_project.yml"
        )
        selection = "model_a incremental_append_new_columns_with_space"
        run_dbt(["run", "--select", selection, "--full-refresh"])
        results = run_dbt(["run", "--select", selection], expect_pass=quote_identifiers)
        model = next(
            result
            for result in results
            if result.node.name == "incremental_append_new_columns_with_space"
        )
        assert model.status == (
            RunStatus.Success if quote_identifiers else RunStatus.Error
        )
