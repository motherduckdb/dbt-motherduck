# Adapted from dbt-duckdb's incremental MERGE tests.
import json

import pytest
from dbt.exceptions import ParsingError
from dbt.tests.util import relation_from_name, run_dbt
from dbt_common.exceptions import CompilationError


class TestIncrementalMerge:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merged.sql": """
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id',
          merge_update_columns=var('update_columns', none),
          merge_exclude_columns=var('exclude_columns', none),
          meta=var('merge', {})) }}
select *, updated_at: current_timestamp, version: 1 from (
{% if is_incremental() %}
    select id: 1, name: 'Alice Updated', age: 26, job: 'Senior Engineer'
    union all select 2, 'Bob Rejected', 29, 'Manager'
    union all select 4, 'Diana', 28, 'Designer'
    union all select 5, 'Eric Rejected', 20, 'Intern'
{% else %}
    select id: 1, name: 'Alice', age: 25, job: 'Engineer'
    union all select 2, 'Bob', 30, 'Manager'
    union all select 3, 'Charlie', 35, 'Director'
{% endif %}
) changes
"""
        }

    @pytest.fixture
    def initial_target(self, project):
        # Start each scenario with a valid target. Invalid configuration must not
        # be required to pass an earlier build or a full refresh.
        run_dbt(["run", "--full-refresh"])
        return relation_from_name(project.adapter, "merged")

    def test_default_merge(self, project, initial_target):
        run_dbt(["run"])
        assert project.run_sql(
            f"select id, name, age, job from {initial_target} order by id", fetch="all"
        ) == [
            (1, "Alice Updated", 26, "Senior Engineer"),
            (2, "Bob Rejected", 29, "Manager"),
            (3, "Charlie", 35, "Director"),
            (4, "Diana", 28, "Designer"),
            (5, "Eric Rejected", 20, "Intern"),
        ]

    def test_update_and_insert_conditions(self, project, initial_target):
        options = {
            "merge_update_condition": "DBT_INTERNAL_DEST.age < DBT_INTERNAL_SOURCE.age",
            "merge_insert_condition": "DBT_INTERNAL_SOURCE.age > 25",
        }
        run_dbt(["run", "--vars", json.dumps({"merge": options})])
        assert project.run_sql(
            f"select id, name, age from {initial_target} order by id", fetch="all"
        ) == [
            (1, "Alice Updated", 26),
            (2, "Bob", 30),
            (3, "Charlie", 35),
            (4, "Diana", 28),
        ]

    @pytest.mark.parametrize(
        "options",
        [
            {"update_columns": ["name", "age"]},
            {"exclude_columns": ["job"]},
        ],
    )
    def test_update_column_selection(self, project, initial_target, options):
        run_dbt(["run", "--vars", json.dumps(options)])
        assert project.run_sql(
            f"select name, age, job from {initial_target} where id = 1", fetch="one"
        ) == ("Alice Updated", 26, "Engineer")

    def test_set_expressions(self, project, initial_target):
        options = {
            "merge_update_set_expressions": {
                "updated_at": "CURRENT_TIMESTAMP",
                "version": "COALESCE(DBT_INTERNAL_DEST.version, 0) + 1",
            }
        }
        run_dbt(["run", "--vars", json.dumps({"merge": options})])
        assert project.run_sql(
            f"select id, version, updated_at is not null from {initial_target} order by id",
            fetch="all",
        ) == [(1, 2, True), (2, 2, True), (3, 1, True), (4, 1, True), (5, 1, True)]

    @pytest.mark.skip_database_type(
        "ducklake", reason="DuckLake supports only one MERGE update or delete action"
    )
    def test_custom_clauses(self, project, initial_target):
        options = {
            "merge_clauses": {
                "when_matched": [
                    {
                        "action": "update",
                        "mode": "by_name",
                        "condition": "DBT_INTERNAL_DEST.age < DBT_INTERNAL_SOURCE.age",
                    },
                    {
                        "action": "delete",
                        "condition": "DBT_INTERNAL_SOURCE.job = 'Manager'",
                    },
                ],
                "when_not_matched": [
                    {
                        "action": "insert",
                        "mode": "by_name",
                        "condition": "DBT_INTERNAL_SOURCE.age > 25",
                    },
                ],
            }
        }
        run_dbt(["run", "--vars", json.dumps({"merge": options})])
        assert project.run_sql(
            f"select id, name from {initial_target} order by id", fetch="all"
        ) == [(1, "Alice Updated"), (3, "Charlie"), (4, "Diana")]

    @pytest.mark.parametrize(
        "options, message",
        [
            (
                {"merge": {"merge_update_condition": 123}},
                "merge_update_condition must be a string",
            ),
            ({"update_columns": "not_a_list"}, "merge_update_columns must be a list"),
            ({"update_columns": False}, "merge_update_columns must be a list"),
            (
                {"merge": {"merge_update_set_expressions": ["not", "a", "dict"]}},
                "merge_update_set_expressions must be a dictionary",
            ),
            (
                {
                    "merge": {
                        "merge_update_condition": "age > 25",
                        "merge_clauses": {
                            "when_matched": [{"action": "update", "mode": "by_name"}]
                        },
                    }
                },
                "basic merge configurations will be ignored",
            ),
            (
                {"merge": {"merge_clauses": "not_a_dict"}},
                "merge_clauses must be a dictionary",
            ),
            (
                {"merge": {"merge_clauses": {}}},
                "must contain at least one of 'when_matched' or 'when_not_matched'",
            ),
            (
                {"merge": {"merge_clauses": {"when_matched": "not_a_list"}}},
                "when_matched must be a list",
            ),
            (
                {"merge": {"merge_clauses": {"when_matched": ["not_a_dict"]}}},
                "elements must be dictionaries",
            ),
            *[
                pytest.param(
                    {"merge": {"merge_clauses": clauses}},
                    "Found 2 UPDATE/DELETE actions",
                    marks=pytest.mark.skip_database_type(
                        "native", reason="DuckLake-specific restriction"
                    ),
                )
                for clauses in [
                    {
                        "when_matched": [
                            {
                                "action": "update",
                                "mode": "by_name",
                                "condition": "DBT_INTERNAL_SOURCE.age > 25",
                            },
                            {"action": "update", "mode": "by_name"},
                        ]
                    },
                    {
                        "when_matched": [
                            {
                                "action": "update",
                                "mode": "by_name",
                                "condition": "DBT_INTERNAL_SOURCE.age > 25",
                            },
                            {"action": "delete"},
                        ]
                    },
                    {
                        "when_matched": [{"action": "update", "mode": "by_name"}],
                        "when_not_matched": [{"by": "source", "action": "delete"}],
                    },
                    {
                        "when_not_matched": [
                            {
                                "by": "source",
                                "action": "update",
                                "set_expressions": {"name": "'Missing'"},
                            },
                            {"by": "source", "action": "delete"},
                        ]
                    },
                ]
            ],
        ],
    )
    def test_invalid_configuration_preserves_target(
        self, project, initial_target, options, message
    ):
        try:
            result = run_dbt(["run", "--vars", json.dumps(options)], expect_pass=False)
        except (CompilationError, ParsingError) as error:
            error_message = str(error)
        else:
            error_message = result.results[0].message
        assert message in error_message
        assert project.run_sql(
            f"select id, name, age from {initial_target} order by id", fetch="all"
        ) == [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]

    def test_single_custom_update_action(self, project, initial_target):
        options = {
            "merge_clauses": {
                "when_matched": [
                    {
                        "action": "update",
                        "mode": "by_name",
                        "condition": "DBT_INTERNAL_DEST.age < DBT_INTERNAL_SOURCE.age",
                    }
                ],
                "when_not_matched": [{"action": "insert", "mode": "by_name"}],
            }
        }
        run_dbt(["run", "--vars", json.dumps({"merge": options})])
        assert project.run_sql(
            f"select id, name from {initial_target} order by id", fetch="all"
        ) == [
            (1, "Alice Updated"),
            (2, "Bob"),
            (3, "Charlie"),
            (4, "Diana"),
            (5, "Eric Rejected"),
        ]
