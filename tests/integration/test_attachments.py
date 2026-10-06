import json
from pathlib import Path
from shutil import copyfile
from uuid import uuid4

import pytest
from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.exceptions import FailedToConnectError
from dbt.tests.util import get_connection, run_dbt
from dbt_common.exceptions import DbtDatabaseError
from psycopg import sql


class TestConfiguredAttachments:
    @pytest.fixture(scope="class")
    def other_database(self, temporary_database):
        with temporary_database() as name:
            yield name

    @pytest.fixture(scope="class")
    def unrelated_database(self, temporary_database):
        with temporary_database() as name:
            yield name

    @pytest.fixture(scope="class")
    def profile_data(self, profile_data, other_database):
        return {
            **profile_data,
            "attach_databases": [
                other_database,
                other_database.upper(),
                profile_data["database"].upper(),
            ],
        }

    def test_configured_attachments_survive_reconnect(
        self, connection_adapter, other_database, unrelated_database
    ):
        adapter = connection_adapter

        def check_session(handle):
            assert handle.execute(
                "select current_setting('motherduck_attach_mode'), current_database()"
            ).fetchone() == ("single", adapter.config.credentials.database)
            attached = [
                row[0]
                for row in handle.execute("select alias from md_attached_databases()")
            ]
            assert attached.count(other_database) == 1
            assert unrelated_database not in attached

        with adapter.connection_named("attachments"):
            connection = adapter.connections.get_thread_connection()
            original = connection.handle
            unrelated = adapter.Relation.create(database=unrelated_database)
            adapter.is_ducklake(unrelated)
            check_session(original)
            with pytest.raises(DbtDatabaseError, match="missing_attachment_table"):
                adapter.execute("select * from missing_attachment_table")
            assert original.closed
            assert connection.state == ConnectionState.CLOSED

        with adapter.connection_named("reattach"):
            handle = adapter.connections.get_thread_connection().handle
            assert handle is not original
            check_session(handle)

    def test_missing_attachment_does_not_create_database(
        self, connection_adapter, admin_connection
    ):
        missing = "dbt_motherduck_missing_" + uuid4().hex
        adapter = connection_adapter
        adapter.config.credentials.attach_databases.append(missing)
        with adapter.connection_named("missing_attachment"):
            with pytest.raises(FailedToConnectError, match=missing):
                adapter.connections.get_thread_connection().handle
            connection = adapter.connections.get_thread_connection()
            assert connection.state == ConnectionState.FAIL
            assert connection.handle is None
        assert admin_connection.execute(
            "select count(*) from md_all_databases() where alias = %s", (missing,)
        ).fetchone() == (0,)


class TestDiscoveredAttachments:
    @pytest.fixture(scope="class")
    def source_database(self, temporary_database, admin_connection):
        with temporary_database() as name:
            admin_connection.execute(
                sql.SQL("create table {}.main.inputs as select id: 1").format(
                    sql.Identifier(name)
                )
            )
            yield name

    @pytest.fixture(scope="class")
    def output_database(self, temporary_database, database_type):
        kind = "native" if database_type == "ducklake" else "ducklake"
        with temporary_database(kind=kind) as name:
            yield name

    @pytest.fixture(scope="class")
    def extra_database(self, temporary_database, admin_connection):
        with temporary_database() as name:
            admin_connection.execute(
                sql.SQL("create table {}.main.lookup as select amount: 10").format(
                    sql.Identifier(name)
                )
            )
            yield name

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target, extra_database):
        return {**dbt_profile_target, "attach_databases": [extra_database]}

    @pytest.fixture(scope="class")
    def project_config_update(self, source_database, output_database, extra_database):
        return {
            "vars": {
                "source_database": source_database,
                "output_database": output_database,
                "extra_database": extra_database,
            },
            "seeds": {"+database": output_database},
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"attached_seed.csv": "id,factor\n1,2\n"}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "attached_snapshot.sql": """
{% snapshot attached_snapshot %}
{{ config(database=var('output_database'), unique_key='id',
          strategy='check', check_cols=['id']) }}
select * from {{ source('remote', 'inputs') }}
{% endsnapshot %}
"""
        }

    @pytest.fixture(scope="class")
    def models(self):
        models = {
            "sources.yml": """
version: 2
sources:
  - name: remote
    database: "{{ var('source_database') }}"
    schema: main
    tables:
      - name: inputs
  - name: files
    database: missing_file_catalog
    tables:
      - name: numbers
        config:
          meta:
            external_location: 'range(1)'
""",
            "attached_function.sql": """
{{ config(materialized='table_function', database=var('output_database')) }}
select * from {{ source('remote', 'inputs') }}
""",
            "ephemeral.sql": """
{{ config(materialized='ephemeral', database='missing_ephemeral_catalog') }}
select offset_value: 0
""",
        }
        for name, upstream in (
            ("from_source", "source('remote', 'inputs')"),
            ("from_function", "ref('attached_function')"),
        ):
            suffix = "()" if name == "from_function" else ""
            models[f"{name}.sql"] = (
                "{{ config(materialized='table', database=var('output_database')) }}\n"
                "select value: inputs.id + seed.factor + extra.amount + ep.offset_value\n"
                f"from {{{{ {upstream} }}}}{suffix} inputs\n"
                "join {{ ref('attached_seed') }} seed using (id)\n"
                "cross join \"{{ var('extra_database') }}\".main.lookup extra\n"
                "cross join {{ ref('ephemeral') }} ep\n"
                "cross join {{ source('files', 'numbers') }} file"
            )
        return models

    def test_sources_targets_workers_and_catalog(
        self, project, source_database, output_database, extra_database
    ):
        assert len(run_dbt(["build", "--threads", "2"])) == 5
        assert len(run_dbt(["snapshot"])) == 1
        adapter = project.adapter
        with get_connection(adapter) as connection:
            attached = {
                row[0]
                for row in connection.handle.execute(
                    "select alias from md_attached_databases()"
                )
            }
            assert {
                project.database,
                source_database,
                output_database,
                extra_database,
            } <= attached
            assert "missing_file_catalog" not in attached
            assert "missing_ephemeral_catalog" not in attached
            for name in ("from_source", "from_function"):
                relation = adapter.Relation.create(
                    database=output_database,
                    schema=project.test_schema,
                    identifier=name,
                )
                assert connection.handle.execute(
                    f"select * from {relation}"
                ).fetchall() == [(13,)]
        catalog = run_dbt(["docs", "generate"])
        assert (
            catalog.sources["source.test.remote.inputs"].metadata.database
            == source_database
        )
        for name in ("from_source", "from_function"):
            assert (
                catalog.nodes[f"model.test.{name}"].metadata.database == output_database
            )
        assert (
            catalog.nodes["seed.test.attached_seed"].metadata.database
            == output_database
        )
        assert (
            catalog.nodes["snapshot.test.attached_snapshot"].metadata.database
            == output_database
        )


class TestDeferredAttachments:
    @pytest.fixture(scope="class")
    def state_database(self, temporary_database):
        with temporary_database() as name:
            yield name

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": """
{{ config(materialized='table', database=var('output_database', target.database)) }}
select id: 41
""",
            "downstream.sql": """
{{ config(materialized='table') }}
select id: id + 1 from {{ ref('upstream') }}
""",
        }

    def test_deferred_database_is_attached(self, project, state_database, tmp_path):
        assert (
            len(
                run_dbt(
                    [
                        "run",
                        "--select",
                        "upstream",
                        "--vars",
                        json.dumps({"output_database": state_database}),
                    ]
                )
            )
            == 1
        )
        state = tmp_path / "state"
        state.mkdir()
        copyfile(
            Path(str(project.project_root)) / "target" / "manifest.json",
            state / "manifest.json",
        )
        results = run_dbt(
            [
                "run",
                "--select",
                "downstream",
                "--defer",
                "--favor-state",
                "--state",
                str(state),
            ]
        )
        assert len(results) == 1
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="downstream",
        )
        assert project.run_sql(f"select * from {relation}", fetch="all") == [(42,)]
        with get_connection(project.adapter) as connection:
            attached = {
                row[0]
                for row in connection.handle.execute(
                    "select alias from md_attached_databases()"
                )
            }
            assert state_database in attached
