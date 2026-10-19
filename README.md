# dbt-motherduck

```yaml
dwh:
  target: dev
  outputs:
    dev:
      type: motherduck
      host: "pg.us-east-1-aws.motherduck.com"
      database:
      schema:
      token: "{{ env_var('DBT_ENV_SECRET_MOTHERDUCK_TOKEN') }}"
      threads: 2
```


## breaking changes

### adapter-specific configs under config.meta

for dbt v2 compat, errors at parse time

### attach mode

Connections use MotherDuck's `single` attachment mode. Each session attaches automatically
the databases declared by enabled models, seeds, snapshots, functions, and database-backed
sources in the parsed project, including deferred relations.

For databases referenced only through raw SQL, hooks, or custom macros, add an
additive list to the profile output:

```yaml
attach_databases:
  - additional_database
```


### connection reuse

Each worker thread reuses its MotherDuck PG endpoint connection across models for the duration of
a dbt run.

Macros that manage transactions should use `statement(auto_begin=True)` and
`adapter.commit()` rather than raw transaction SQL. Mismatches between dbt's
transaction flag and the driver raise an error and discard the session.

on dbt-duckdb since there's only 1 connection you could use on-run-start hooks to `SET` duckdb options

in dbt-motherduck each connection is independent, and the main connection that runs on-run-start hooks is not used by models

so we support profile settings

Use `settings` in the profile output to initialize every session, including setup,
compilation, model workers, and replacement connections:

```yaml
settings:
  preserve_insertion_order: false
  TimeZone: Europe/Amsterdam
```


For a model-specific override, restore the profile value in a post-hook:

```jinja
{{ config(
    pre_hook="SET preserve_insertion_order = true",
    post_hook="SET preserve_insertion_order = " ~
        (target.settings.get('preserve_insertion_order', true) | lower)
) }}
```

### staging tables and cancellation

MotherDuck has no remote temporary tables, so incremental models and snapshots stage
rows in regular tables, in `dbt_temp` or the schema set by `meta.temp_schema_name`.
Each staging table name carries a random suffix, so concurrent runs and same-named
models in different schemas never share one. Because of that suffix, a later run
cannot discover tables an earlier run left behind.

When a run is cancelled, dbt first disconnects the worker sessions, then the adapter
drops the staging tables those workers left, using a new session limited to 5 seconds.
If that cleanup fails or times out, dbt logs a warning naming each remaining table so
you can drop it yourself. Cleanup cannot run if the dbt process is killed outright,
for example with `SIGKILL` or a second Ctrl-C during shutdown.


## 2.0 limitations

### nested schemas

DuckDB 2.0's nested schemas, such as `database.schema_1.schema_2.table_1`, are not
yet supported by the adapter. Schema values are treated as a single identifier,
not a nested path. Use a single schema level for models, sources, snapshots, and
staging tables.

Future support requires namespace-aware relation rendering, parent-aware metadata
discovery, schema/staging DDL, and foreign-key and index handling.
