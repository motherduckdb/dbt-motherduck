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


### browser login (SSO)

For local development, log in through your browser instead of storing a token.
Organizations that enforce SSO get their identity provider's login page:

```yaml
dev:
  type: motherduck
  authenticator: externalbrowser
  host: "pg.us-east-1-aws.motherduck.com"
  database:
  schema:
```

On the first connection dbt prints a URL and code and opens your browser. The
resulting token is saved to `~/.motherduck/credentials.json` (or
`$MOTHERDUCK_HOME/credentials.json`), the same file `motherduck login` uses, so
either one logs in both. All threads share one login, and dbt logs in again if
the saved token has expired or is rejected. `token` must be unset or empty with
`authenticator: externalbrowser`; use the default `authenticator: token` in CI.

dbt logs in through MotherDuck's own public Auth0 application by default. Set
`oauth_client_id` to log in through a different MotherDuck-registered application,
for example when testing against a staging environment.


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


### complex types in contracts

Complex types on model contracts are now more strict, and it needs to match exactly how DuckDB prints the type.

```yaml
columns:
  - name: payload
    data_type: struct(a integer, b varchar) # real type, works
    data_type: struct(b varchar, a integer) # passes on dbt-duckdb, fails on dbt-motherduck
```

Field names, types, and order must all match.


## 2.0 limitations

### nested schemas

DuckDB 2.0's nested schemas, such as `database.schema_1.schema_2.table_1`, are not
yet supported by the adapter. Schema values are treated as a single identifier,
not a nested path. Use a single schema level for models, sources, snapshots, and
staging tables.

Future support requires namespace-aware relation rendering, parent-aware metadata
discovery, schema/staging DDL, and foreign-key and index handling.
