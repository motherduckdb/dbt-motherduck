# Incremental models

TODO: assume the reader know about incremental models, shorter 1 sentence intro linking to the dbt docs. No need to mention the is_incrementla macro or dbt commands. Do mention the materialized config, and `incremental_strategy`.

Use `materialized='incremental'` and an `is_incremental()` filter to select the input for each update. The first build and `dbt run --full-refresh` rebuild the table. See dbt's [incremental models](https://docs.getdbt.com/docs/build/incremental-models) documentation for the general pattern.

## Incremental strategies

TODO: The table is good keep it. Each subsequent section indent it under ## Incremental strategies, and the title of each should be each incremental strategy where it gets introduced first, and then options supported, and then tips if there's anything worth mentioning. For each strategy show a codeblock with a VERY MINIMAL compiled SQL output.


| `incremental_strategy` | Behavior |
| --- | --- |
| `delete+insert` | Deletes target rows matching incoming `unique_key` values, then inserts the incoming rows. Without a key, it appends. |
| `append` | Inserts all incoming rows without checking existing keys. |
| `merge` | Uses `MERGE INTO` to update matching keys and insert new ones. Without a key, the default merge appends. |
| `microbatch` | Replaces each event-time window with delete and insert operations. Requires `event_time`, `batch_size`, and `begin`. |

The default strategy is `delete+insert` if `incremental_strategy` is not set.

## Timestamp pruning with delete+insert

For time-organized data, include a timestamp column in the composite `unique_key` so the delete joins on both the identifier and timestamp:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['event_id', 'event_time']
) }}

select event_id, event_time, payload
from {{ source('raw', 'events') }}
{% if is_incremental() %}
where event_time >= current_timestamp - interval '3 days'
{% endif %}
```

The list form generates a `DELETE ... USING` join with equality predicates for each key column. A narrow timestamp range in the incoming data can enable filter pushdown into the target scan, letting MotherDuck prune row groups by their timestamp statistics. This works best when target row groups cover narrow time ranges; pruning depends on the data layout and query plan. See DuckDB's [zonemap documentation](https://duckdb.org/docs/stable/guides/performance/indexing#zonemaps) for why ordering helps, and use MotherDuck's [`EXPLAIN ANALYZE`](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/explain-analyze/) to inspect the executed query plan.

Use an immutable timestamp, such as an event's creation time, that is identical in the incoming and existing row. Adding a mutable `updated_at` to the key changes the match when it changes, leaving the old row behind. Key columns should be non-null. Choose a lookback window that covers the late-arriving data you need to reprocess.

`incremental_predicates` can further restrict the target delete or merge scan. They must still include every target row that the incoming data should replace, or duplicates can remain.

## Merge options

Standard dbt options such as `merge_update_columns` and `merge_exclude_columns` stay at the config root. Adapter-specific options belong under `meta`: `merge_update_condition`, `merge_insert_condition`, `merge_on_using_columns`, `merge_update_set_expressions`, `merge_clauses`, and `merge_returning_columns`.

For example, update existing rows only when the incoming version is newer:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    meta={
        'merge_update_condition':
            'DBT_INTERNAL_SOURCE.updated_at > DBT_INTERNAL_DEST.updated_at'
    }
) }}

select event_id, event_time, updated_at, payload
from {{ source('raw', 'events') }}
```

DuckLake rejects `merge_returning_columns` and allows only one `UPDATE` or `DELETE` action across custom merge clauses.

## Microbatch

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin='2025-01-01',
    concurrent_batches=false
) }}

select event_id, event_time, payload
from {{ source('raw', 'events') }}
```

Configure `event_time` on upstream sources or models too, so dbt can filter their inputs for each batch. Target configuration alone does not filter raw SQL inputs. dbt manages `lookback` and CLI event-time bounds; see [microbatch configuration](https://docs.getdbt.com/docs/build/incremental-microbatch).

Native MotherDuck databases support concurrent batches. Use at least three dbt threads to overlap batches of a single model, since one worker coordinates it. For DuckLake, set `concurrent_batches=false`: concurrent batches can conflict on inlined data even when their time windows do not overlap. The adapter does not automatically disable concurrency or retry those transactions.

## Staging tables

TODO: synthetize this a lot, just 1 sentence explaning what they are and how to configure then. Another short sentence mentoning that canceled runs try to cleanup, but can leave some behind and suggest people to cleanup those schemas while no other dbt invocation is running

Incremental models and snapshots use regular staging tables in the target database's `dbt_temp` schema. Set `meta={'temp_schema_name': 'custom_staging'}` to choose another schema. The token needs permission to create the schema and create and drop its tables.

The adapter cleans up staging tables after successful builds and ordinary failures. Cancellation or an interrupted process can leave them behind. Avoid separate dbt invocations writing the same target table concurrently.
