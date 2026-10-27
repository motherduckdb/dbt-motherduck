# Coming from dbt-duckdb

A dbt project built using `dbt-duckdb` should work with `dbt-motherduck` with very few changes.
This document collects intentional breaking changes, and how to migrate adapters.

## Adapter-specific configuration

In dbt v1 adapter-specific configurations could be placed at the top-level of the `config` objects,
but on dbt v2 they need to be under `config.meta`. This adapter aims to be ready for a v2 upgrade,
so it will reject the legacy way of setting configurations at parse time.

TODO: add 2 examples

## DuckDB settings

TODO: link to connection.md/## Settings and hooks
In `dbt-motherduck` each worker thread reuses a MotherDuck connection across models, so settings apply at the worker-level.
In `dbt-duckdb` settings were applied at the process-level, affecting all threads.

To configure settings at the project level use the `settings` dictionary on your profile.

## Transaction handling

This adapter expects user-defined macros that manage transactions to use `statement(auto_begin=True)` and
`adapter.commit()` rather than raw transaction SQL like `BEGIN` or `COMMIT`, which should raise errors at compile time.

TODO: add an example of how to define a macro to run stuff in a transaction

## Complex types in contracts

Complex type contracts are stricter than in `dbt-duckdb`: field names, types, and order
must match the actual data. With dbt contracts (TODO: add link to dbt contracts docs) enabled:

```yaml
columns:
  - name: col_1
    data_type: struct(a integer, b varchar) # real struc type
    # data_type: struct(b varchar, a integer) # fails on dbt-motherduck, passes on dbt-duckdb
```

TODO: also add an example about dbt-duckdb supporting `col_1.a` on the yaml directly and dbt-motherduck not

## Attach mode

TODO link to attachment mode MD docs
TODO add ref to connections.md/## Single attach mode and database discovery
Connections use MotherDuck's `single` attachment mode, while `dbt-duckdb` used workspace mode.
If your project includes SQL statements that reference databases that are not tracked by dbt,
you will need to declare those in your profile configuration.

```yaml
attach_databases:
  - additional_database
```
