import pytest
from dbt.adapters.exceptions import FailedToConnectError
from dbt.tests.util import relation_from_name, run_dbt


class TestProfileSettings:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, profile_data):
        return {
            **profile_data,
            "token": "{{ env_var('DBT_ENV_SECRET_MOTHERDUCK_TOKEN') }}",
            "settings": {
                # Unlike the engine default, this value cannot be restored with RESET.
                "preserve_insertion_order": False,
                "TimeZone": "Europe/Amsterdam",
            },
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "on-run-start": [
                "select case when current_setting('preserve_insertion_order') = false "
                "and current_setting('TimeZone') = '{{ target.settings.TimeZone }}' "
                "then 1 else error('profile settings missing on setup connection') end"
            ]
        }

    @pytest.fixture(scope="class")
    def models(self):
        settings_sql = (
            "select insertion_order: current_setting('preserve_insertion_order'), "
            "time_zone: current_setting('TimeZone'), "
            "profile_order: {{ target.settings.preserve_insertion_order | lower }}"
        )
        return {
            "defaults.sql": "{{ config(materialized='table') }} " + settings_sql,
            "override.sql": """
{{ config(
    materialized='table',
    pre_hook=[
        "select case when current_setting('preserve_insertion_order') = false
         then 1 else error('profile value missing before override') end",
        'SET preserve_insertion_order = true'
    ],
    post_hook=[
        'SET preserve_insertion_order = ' ~ (target.settings.get('preserve_insertion_order', true) | lower),
        "select case when current_setting('preserve_insertion_order') = false
         then 1 else error('post-hook did not restore profile value') end"
    ]
) }}
"""
            + settings_sql,
            "after_override.sql": "{{ config(materialized='table') }} "
            + settings_sql
            + " from {{ ref('override') }}",
        }

    @pytest.mark.parametrize("threads", [1, 2])
    def test_profile_settings_can_be_overridden_and_restored(self, project, threads):
        run_dbt(["run", "--threads", str(threads)])
        for name in ("defaults", "override", "after_override"):
            relation = relation_from_name(project.adapter, name)
            assert project.run_sql(f"select * from {relation}", fetch="one") == (
                name == "override",
                "Europe/Amsterdam",
                False,
            )


@pytest.mark.parametrize(
    "settings",
    [
        {"dbt_motherduck_unknown_setting": True},
        {"TimeZone": "Europe/Amsterdam'; SELECT 42 --"},
    ],
)
def test_invalid_settings_fail_connection_initialization(connection_adapter, settings):
    adapter = connection_adapter
    adapter.config.credentials.settings = settings
    with (
        pytest.raises(FailedToConnectError, match=next(iter(settings))),
        adapter.connection_named("invalid_settings"),
    ):
        adapter.execute("select 42")
    adapter.config.credentials.settings = {}
    with adapter.connection_named("valid_settings"):
        _, table = adapter.execute("select 42", fetch=True)
        assert table.rows[0][0] == 42
