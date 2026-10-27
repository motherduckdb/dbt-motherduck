# DuckLake

The type of each database is detected automatically, so to use DuckLake you just need to point the dbt profile or a model's `database` at an existing DuckLake database.

You can create a DuckLake in MotherDuck before running dbt with [`CREATE DATABASE (type ducklake)`](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-database/):

```sql
create database lakehouse (type ducklake);
```

See the [MotherDuck DuckLake guide](https://motherduck.com/docs/integrations/file-formats/ducklake/) for managed storage and setup options.

## Partitioning and sort order

Partitioning and sort order are supported on DuckLake databases. Configure
them under `config.meta`:

```sql
{{ config(
    materialized='table',
    database='lakehouse',
    meta={
        'partitioned_by': ['day(event_time)', 'region'],
        'sorted_by': ['event_time', 'event_id']
    }
) }}

select event_id, event_time, region, payload
from {{ source('raw', 'events') }}
```

| Option | Alias | Accepted values |
| --- | --- | --- |
| `partitioned_by` | `partition_by` | A SQL partition expression or a list of expressions, such as `day(event_time)`, `bucket(16, event_id)`, or `region`. |
| `sorted_by` | `sort_by` | A column name or a list of column names, such as `event_time` or `['event_time', 'event_id']`. |

[Partitioning](https://ducklake.select/docs/stable/duckdb/advanced_features/partitioning) groups new data into files by partition keys. A [sort order](https://ducklake.select/docs/stable/duckdb/advanced_features/sorted_tables) organizes data within files, improving the statistics used to skip irrelevant data. Choose keys that match common filters.

## Limitations

- Set `concurrent_batches=false` for DuckLake microbatch models to avoid conflicting writes to inlined data. See [incremental models](incremental-models.md#microbatch).
