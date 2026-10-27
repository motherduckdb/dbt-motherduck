# Incremental models

Configure [incremental models](https://docs.getdbt.com/docs/build/incremental-models) with `materialized='incremental'` and choosing and `incremental_strategy`.

## Incremental strategies

| `incremental_strategy` | Behavior |
| --- | --- |
| `delete+insert` | Deletes target rows matching incoming `unique_key` values, then inserts the incoming rows. Without a key, it appends. |
| `append` | Inserts all incoming rows without checking existing keys. |
| `merge` | Uses `MERGE INTO` to update matching keys and insert new ones. Without a key, the default merge appends. |
| `microbatch` | Replaces each event-time window with delete and insert operations. Requires `event_time`, `batch_size`, and `begin`. |

The default strategy is `delete+insert`.

### delete+insert

Replaces rows matching `unique_key`, which accepts a column name or a list of
columns. Without a key, it appends. `incremental_predicates` can restrict the
target delete, but must include every row the incoming data should replace.

E.g. for `unique_key=['event_id', 'event_time']`:

```sql
BEGIN
delete from target_table as DBT_INCREMENTAL_TARGET
using incoming_rows
where incoming_rows.event_id = DBT_INCREMENTAL_TARGET.event_id
  and incoming_rows.event_time = DBT_INCREMENTAL_TARGET.event_time;

insert into target_table (event_id, event_time)
(select event_id, event_time from incoming_rows);
COMMIT
```

Including an immutable timestamp in the `unique_key` option enables efficient filter pushdown and row-group
pruning if the data is somewhat sorted by that timestamp.
Check out DuckDB's [zonemap documentation](https://duckdb.org/docs/stable/guides/performance/indexing#zonemaps)
and [this blog post](https://duckdb.org/2025/05/14/sorting-for-fast-selective-queries) for more information.
You can inspect the query profiles with [`EXPLAIN ANALYZE`](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/explain-analyze/) to confirm that your incremental models are efficiently pruning row-groups.

### append

Inserts incoming rows without checking existing keys; `unique_key` is ignored.

```sql
insert into target_table (event_id)
(select event_id from incoming_rows);
```

### merge

Updates matching `unique_key` values and inserts new ones. Without a key, the
default merge appends. `incremental_predicates` can restrict the target match.

TODO: rewrite this horrible paragraph as a table explaining each option
Standard options `merge_update_columns` and `merge_exclude_columns` stay at the
config root. Custom options belong under `meta`: `merge_update_condition`,
`merge_insert_condition`, `merge_on_using_columns`, `merge_update_set_expressions`,
`merge_clauses`, and `merge_returning_columns`.

```sql
merge into target_table as DBT_INTERNAL_DEST
using incoming_rows as DBT_INTERNAL_SOURCE
on DBT_INTERNAL_SOURCE.event_id = DBT_INTERNAL_DEST.event_id
when matched then update by name
when not matched then insert by name;
```

TODO: example for this
Use `DBT_INTERNAL_SOURCE` and `DBT_INTERNAL_DEST` in custom merge conditions.

DuckLake rejects `merge_returning_columns` and allows only one `UPDATE` or `DELETE` action across custom merge clauses.

### microbatch

Replaces each event-time window using delete and insert. Requires `event_time`,
`batch_size`, and `begin`. See [the official dbt docs on microbatching](https://docs.getdbt.com/docs/build/incremental-microbatch).

- `incremental_predicates` adds filters to the target delete.
- TODO: explain concurrent batches, don't mention ducklake here

TODO: include incremental predicated in the example
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin='2025-01-01',
) }}

select event_id, event_time
from {{ source('raw', 'events') }}
```

For a daily batch:

```sql
delete from target_table
where event_time >= '2025-01-01' and event_time < '2025-01-02';

insert into target_table (event_id, event_time)
select event_id, event_time from incoming_rows
where event_time >= '2025-01-01' and event_time < '2025-01-02';
```

## Staging tables

Incremental models and snapshots create staging tables in `dbt_temp`. You can change the
staging schema with `meta={'temp_schema_name': 'custom_staging'}`. Normal runs will try to
cleanup after them, but some canceled or aborted runs might leave leftover tables in your
schema. If they accumulate, it's a good idea to clean them up regularly.
