# dbt-motherduck

[MotherDuck](https://motherduck.com/) is a Cloud Data Warehouse that uses [DuckDB](https://duckdb.org/) as it's compute engine.

[dbt](http://getdbt.com) is a framework to engineer data transformation pipelines. This project, `dbt-motherduck`, is an adapter to run dbt v1 with MotherDuck.

This dbt v1 adapter allows you to run dbt v1 projects in MotherDuck. The [DuckDB Adapter](https://github.com/duckdb/dbt-duckdb) also supports running queries against MotherDuck,
but each adapter supports MotherDuck in different ways.
`dbt-motherduck` leverages MotherDuck's [Postgres Endpoint](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/postgres-endpoint/) and executes queries via Postgres connections,
while `dbt-duckdb` spawns a DuckDB process that loads the MotherDuck DuckDB extension.

- With `dbt-duckdb` you need to manage the version of the DuckDB client, since there are frequent releases and old versions get deprecated. With `dbt-motherduck` you don't need to manage the DuckDB client life-cycle.
- `dbt-motherduck` consumes less resources client-side than `dbt-duckdb`.
- The DuckDB client running with `dbt-duckdb` needs to dynamically load the MotherDuck extensions at runtime, which is undesirable to teams running dbt on restricted networking environments.
- If you need Hybrid Execution (individual queries executing both in your client and on MotherDuck), you cannot use `dbt-motherduck` and need to use `dbt-duckdb` since this adapter doesn't use a local DuckDB client. Most dbt workloads only use data reachable from the Ducklings (e.g. hosted on MD, on blob storage, REST catalogs), so they don't need Hybrid Execution.

A dbt project built using `dbt-duckdb` should work with `dbt-motherduck` with very few changes (documented [here](docs/coming-from-dbt-duckdb.md)), and execute similar SQL statements against MotherDuck.

To get started, install `dbt-motherduck` from PyPI and add your MotherDuck credentials on `profiles.yml`. E.g.:

```yaml
dwh:
  target: dev
  outputs:
    dev:
      type: motherduck
      host: "pg.us-east-1-aws.motherduck.com" # needs to match the region of your MotherDuck organization
      database: # target database
      schema: # default target schema
      token: "{{ env_var('DBT_ENV_SECRET_MOTHERDUCK_TOKEN') }}"
      threads: 4
      settings:
        preserve_insertion_order: false
        TimeZone: Europe/Amsterdam
```

## Documentation

- [Connection](docs/connection.md): profiles, single attach mode, database discovery, and settings.
- [Incremental models](docs/incremental-models.md): strategies and timestamp-based pruning.
- [DuckLake](docs/ducklake.md): partitioning, sort order, and supported options.
- [Reading and writing external files](docs/external-files.md): remote sources and exports.
- [Python models](docs/python-models.md): current support and upcoming MotherDuck Flights integration.
- [Coming from dbt-duckdb](docs/coming-from-dbt-duckdb.md): overview of the few caveats that might impact you if you are migrating a project from `dbt-duckdb`.

For general dbt concepts, project configuration, and commands, see the [dbt documentation](https://docs.getdbt.com/docs/introduction).
For MotherDuck SQL and platform features, see the [MotherDuck documentation](https://motherduck.com/docs/).
