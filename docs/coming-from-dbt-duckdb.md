# Coming from dbt-duckdb

A dbt project built using `dbt-duckdb` should work with `dbt-motherduck` with very few changes.
This document collects intentional breaking changes, and how to migrate adapters.

## Adapter-specific configuration

In dbt v1 adapter-specific configurations could be placed at the top-level of the `config` objects,
but on dbt v2 they need to be under `config.meta`. This adapter aims to be ready for a v2 upgrade,
so it will reject the legacy way of setting configurations at parse time.


- In `dbt-duckdb`:

```jinja
{{ config(partitioned_by='day(event_time)') }}
{{ config(merge_update_condition='DBT_INTERNAL_SOURCE.is_active') }}
```

- In `dbt-motherduck`:

```jinja
{{ config(meta={'partitioned_by': 'day(event_time)'}) }}
{{ config(meta={'merge_update_condition': 'DBT_INTERNAL_SOURCE.is_active'}) }}
```

## DuckDB settings

In `dbt-motherduck` each worker thread reuses its own MotherDuck connection across models, so settings apply at the worker-level. On `dbt-duckdb` they would affect all workers. To configure settings globally use the profile `settings` dictionary.

See the [Settings](connection.md#settings) documentation of this adapter for more info.

## Transaction handling

Use `statement(auto_begin=True)` and `adapter.commit()` in macros that manage
transactions. Don't use raw `BEGIN` or `COMMIT`, which can desynchronize transaction
tracking and raise an error when the connection is released.

```jinja
{% macro run_in_transaction(sql) %}
  {% if execute %}
    {% call statement('run_in_transaction', auto_begin=True) %}
      {{ sql }}
    {% endcall %}
    {% do adapter.commit() %}
  {% endif %}
{% endmacro %}
```

## Complex types in contracts

Complex type contracts are stricter than in `dbt-duckdb`: field names, types, and order
must match the actual data. With [dbt contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts) enabled:

```yaml
columns:
  - name: col_1
    data_type: struct(a integer, b varchar) # actual struct type
    # data_type: struct(b varchar, a integer) # fails on dbt-motherduck, passes on dbt-duckdb
```

Also, unlike `dbt-duckdb`, `dbt-motherduck` does not accept dotted field declarations for
struct contracts. The following examples won't work and need to be replaced by the full struct syntax.

```yaml
columns:
  - name: col_1.a
    data_type: integer
  - name: col_1.b
    data_type: varchar
```

## Attach mode

Connections use MotherDuck's [`single` attachment mode](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/attach-modes/), while `dbt-duckdb` defaults to workspace mode.
If your project includes SQL statements that reference databases that are not tracked by dbt,
you will need to declare those in your profile configuration.

```yaml
attach_databases:
  - additional_database
```

See the [single attach mode and database discovery](connection.md#single-attach-mode-and-database-discovery) adapter documentation.
