# SQL helpers and staging hooks adapted from dbt-duckdb commit
# acdab5dfc85715572bbf43fcf2b318a5ed1bfa16, dbt/adapters/duckdb/impl.py.
from collections import defaultdict
from threading import Lock
from uuid import uuid4

from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.capability import (
    Capability,
    CapabilityDict,
    CapabilitySupport,
    Support,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql import SQLAdapter
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)
from dbt_common.exceptions import DbtRuntimeError
from sqlparse import lexer

from dbt.adapters.motherduck.column import MotherDuckColumn
from dbt.adapters.motherduck.config import MotherDuckConfig, MotherDuckIndexConfig
from dbt.adapters.motherduck.connections import MotherDuckConnectionManager
from dbt.adapters.motherduck.relation import MotherDuckRelation


class MotherDuckAdapter(SQLAdapter):
    ConnectionManager = MotherDuckConnectionManager
    Column = MotherDuckColumn
    Relation = MotherDuckRelation
    AdapterSpecificConfigs = MotherDuckConfig

    CONSTRAINT_SUPPORT = dict.fromkeys(
        (
            ConstraintType.check,
            ConstraintType.not_null,
            ConstraintType.unique,
            ConstraintType.primary_key,
            ConstraintType.foreign_key,
        ),
        ConstraintSupport.ENFORCED,
    )

    _capabilities = CapabilityDict(
        {
            Capability.MicrobatchConcurrency: CapabilitySupport(support=Support.Full),
            Capability.SchemaMetadataByRelations: CapabilitySupport(
                support=Support.Full
            ),
        }
    )

    def __init__(self, config, mp_context):
        super().__init__(config, mp_context)
        # the ConnectionManager uses this callback to ask for a list of databases to attach
        self.connections.databases_needed_callback = self._get_databases_needed
        self._warnings = set()
        self._temp_relation_ids = defaultdict(lambda: uuid4().hex)
        # DuckDB / MotherDuck can run into write-write conflicts when running concurrent `create
        # schema if not exist`. this lock synchronizes adapter-wide creation of the schemas where
        # incremental models and snapshots build their staging tables.
        self._temp_relations_lock = Lock()

    @classmethod
    def date_function(cls):
        return "now()"

    def valid_incremental_strategies(self):
        return ["append", "delete+insert", "merge", "microbatch"]

    def pre_model_hook(self, config):
        if config.model.get_materialization() not in ("incremental", "snapshot"):
            return None
        relation = self.Relation.create(
            database=config.model.database,
            schema=config.model.schema,
            identifier=config.model.alias,
            type="table",
        )
        path = self.get_temp_relation_path(
            relation,
            config.model.config.meta.get("temp_schema_name", "dbt_temp"),
            self.batch_id_for_model(config.model),
        )
        staging = relation.incorporate(path=path)
        # Concurrent CREATE SCHEMA transactions conflict even with IF NOT EXISTS.
        with self._temp_relations_lock:
            self.commit_if_has_connection()
            self.create_schema(staging)
        self.drop_relation(staging)
        # Prepare before model hooks, so staging setup cannot commit their writes.
        self.commit_if_has_connection()
        return staging

    def post_model_hook(self, config, context):
        if context is not None and not self.connections.invocation_canceled.is_set():  # pyright: ignore[reportAttributeAccessIssue]
            # A Jinja failure can leave the model transaction open. Cleanup must persist.
            self.connections.rollback_if_open()
            # SQL errors discard the session; reconnect only to perform cleanup.
            self.connections.open(self.connections.get_thread_connection())
            self.drop_relation(context)

    def commit_if_has_connection(self):
        connection = self.connections.get_if_exists()
        if connection is not None and not connection.transaction_open:
            return
        self.connections.commit_if_has_connection()

    @available
    def get_temp_relation_path(
        self, relation, temp_schema_name="dbt_temp", batch_id=""
    ):
        if not isinstance(temp_schema_name, str) or not temp_schema_name.strip():
            raise DbtRuntimeError("meta.temp_schema_name must be a non-empty string")
        key = (relation.database, relation.schema, relation.identifier)
        return {
            "database": relation.database,
            "schema": temp_schema_name,
            "identifier": f"{relation.identifier}__{self._temp_relation_ids[key]}"
            + (f"__{batch_id}" if batch_id else ""),
        }

    @available
    def batch_id_for_model(self, model):
        batch = (
            model.get("batch")
            if isinstance(model, dict)
            else getattr(model, "batch", None)
        )
        if not batch:
            return ""
        start = (
            batch.get("event_time_start")
            if isinstance(batch, dict)
            else batch.event_time_start
        )
        return (
            str(start)
            .replace("-", "")
            .replace(":", "")
            .replace(" ", "_")
            .replace("+", "_")
            if start
            else ""
        )

    @available
    def is_ducklake(self, relation):
        database = relation.database.replace("'", "''")
        _, table = self.execute(
            "SELECT is_ducklake FROM md_attached_databases() "
            f"WHERE lower(alias) = lower('{database}')",
            fetch=True,
        )
        return bool(table.rows and table.rows[0][0])

    def get_columns_in_relation(self, relation):
        # calls describe, which raises if relation is missing
        # returning an empty list is just what the adapters do for legacy reasons
        # https://github.com/duckdb/dbt-duckdb/pull/763
        try:
            return super().get_columns_in_relation(relation)
        except DbtRuntimeError as exc:
            if "does not exist" in str(exc).lower():
                return []
            raise

    @available.parse(lambda *a, **k: [])
    def get_column_schema_from_query(self, sql: str) -> list[MotherDuckColumn]:
        """Get a list of the column names and data types from the given sql.

        :param str sql: The sql to execute.
        :return: List[MotherDuckColumn]
        """
        # Taking advantage of yet another amazing DuckDB SQL feature right here: the
        # ability to DESCRIBE a query instead of a relation
        describe_sql = f"DESCRIBE ({sql})"
        _, table = self.execute(describe_sql, fetch=True)
        flattened_columns = []
        for row in table.rows:
            name, dtype = row[0], row[1]
            column = MotherDuckColumn(column=name, dtype=dtype)
            flattened_columns.extend(column.flatten())
        return flattened_columns

    def expand_column_types(self, goal, current):
        # DuckDB VARCHAR has no length constraint; avoid redundant metadata queries.
        return

    @available
    def load_seed(self, relation, table, quote_columns):
        columns = ", ".join(
            self.quote_seed_column(name, quote_columns) for name in table.column_names
        )
        prefix = f"INSERT INTO {relation} ({columns}) VALUES ".replace("%", "%%")
        row_sql = "(" + ", ".join(["%s"] * len(table.column_names)) + ")"
        # The PostgreSQL protocol permits at most 65535 bound parameters.
        batch_size = min(500, 65535 // len(table.column_names))
        first_sql = ""
        for start in range(0, len(table.rows), batch_size):
            rows = table.rows[start : start + batch_size]
            sql = prefix + ", ".join([row_sql] * len(rows))
            bindings = [value for row in rows for value in row]
            _, cursor = self.connections.add_query(
                sql, bindings=bindings, abridge_sql_log=True
            )
            cursor.close()
            if not first_sql:
                first_sql = sql
        return first_sql

    @classmethod
    def render_column_constraint(cls, constraint: ColumnLevelConstraint) -> str | None:
        """Render the given constraint as DDL text. Should be overriden by adapters which need custom constraint
        rendering."""
        if constraint.type == ConstraintType.foreign_key:
            if constraint.to and constraint.to_columns:
                constraint_to = cls._constraint_target_without_database(constraint.to)
                return (
                    f"references {constraint_to} ({', '.join(constraint.to_columns)})"
                )
            else:
                return f"references {constraint.expression}"
        else:
            return super().render_column_constraint(constraint)

    @classmethod
    def render_model_constraint(cls, constraint: ModelLevelConstraint) -> str | None:
        if (
            constraint.type == ConstraintType.foreign_key
            and constraint.to
            and constraint.to_columns
        ):
            constraint_prefix = (
                f"constraint {constraint.name} " if constraint.name else ""
            )
            column_list = ", ".join(constraint.columns)
            constraint_to = cls._constraint_target_without_database(constraint.to)
            return (
                f"{constraint_prefix}foreign key ({column_list}) references "
                f"{constraint_to} ({', '.join(constraint.to_columns)})"
            )
        return super().render_model_constraint(constraint)

    @staticmethod
    def _constraint_target_without_database(constraint_to: str) -> str:
        # DuckDB does not accept a catalog-qualified foreign key target, even when
        # the target is in the current catalog.
        tokens = list(lexer.tokenize(constraint_to))
        # Quoted identifiers can contain dots that are not namespace separators.
        separators = [i for i, (_, value) in enumerate(tokens) if value == "."]
        if len(separators) > 1:
            return "".join(value for _, value in tokens[separators[0] + 1 :]).lstrip()
        return constraint_to

    @available
    def parse_index(self, raw_index):
        return MotherDuckIndexConfig.parse(raw_index)

    @available
    def warn_once(self, msg):
        if msg not in self._warnings:
            self._warnings.add(msg)
            AdapterLogger("MotherDuck").warning(msg)

    def _get_databases_needed(self):
        credentials = self.config.credentials
        databases = {
            name.casefold(): name
            for name in (credentials.database, *credentials.attach_databases)
        }
        manifest = self.get_macro_resolver()
        for collection in ("nodes", "sources", "functions"):
            for node in getattr(manifest, collection, {}).values():
                if getattr(node, "is_ephemeral", False):
                    continue
                if collection == "nodes" and not node.is_relational:
                    continue
                if collection == "sources":
                    relation = self.Relation.create_from(self.config, node)
                    if relation.external:
                        continue
                if node.database:
                    databases.setdefault(node.database.casefold(), node.database)
                for field in ("defer_relation", "defer_function"):
                    deferred = getattr(node, field, None)
                    if deferred is not None and deferred.database:
                        databases.setdefault(
                            deferred.database.casefold(), deferred.database
                        )
        return tuple(databases[key] for key in sorted(databases))
