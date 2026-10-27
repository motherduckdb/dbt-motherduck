# dbt-motherduck

[MotherDuck](https://motherduck.com/) is a Cloud Data Warehouse that uses [DuckDB](https://duckdb.org/) as it's compute engine.

[dbt](http://getdbt.com) is a framework to engineer data transformation pipelines. This project, `dbt-motherduck`, is an adapter to run dbt v1 with MotherDuck.

This dbt v1 adapter allows you to run dbt v1 projects in MotherDuck. The [DuckDB Adapter](https://github.com/duckdb/dbt-duckdb) also supports running queries against MotherDuck,
but each adapter supports MotherDuck in different ways.
`dbt-motherduck` leverages MotherDuck's [Postgres Endpoint](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/postgres-endpoint/) and executes queries via Postgres connections,
while `dbt-duckdb` spawns a DuckDB process that loads the MotherDuck DuckDB extension.

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
- [Coming from dbt-duckdb](docs/coming-from-dbt-duckdb.md): configuration and behavior changes.

For general dbt concepts, project configuration, and commands, see the [dbt documentation](https://docs.getdbt.com/docs/introduction).
For MotherDuck SQL and platform features, see the [MotherDuck documentation](https://motherduck.com/docs/).
