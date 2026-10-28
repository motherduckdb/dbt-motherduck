# Incremental models

Configure [incremental models](https://docs.getdbt.com/docs/build/incremental-models) with `materialized='incremental'` and an `incremental_strategy`.

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
columns. Without a key, it appends.

`incremental_predicates` restricts the scan on the `delete` call against the target table.

E.g. for `unique_key=['event_id', 'event_time']` and
`incremental_predicates=["DBT_INCREMENTAL_TARGET.event_time >= timestamp '2025-01-01'"]`:

```sql
BEGIN;
delete from target_table as DBT_INCREMENTAL_TARGET
using incoming_rows
where incoming_rows.event_id = DBT_INCREMENTAL_TARGET.event_id
  and incoming_rows.event_time = DBT_INCREMENTAL_TARGET.event_time
  and DBT_INCREMENTAL_TARGET.event_time >= timestamp '2025-01-01';

insert into target_table (event_id, event_time)
(select event_id, event_time from incoming_rows);
COMMIT;
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

Updates on matching `unique_key` values and inserts new ones.

Standard dbt options are `unique_key`, `incremental_predicates`, `merge_update_columns`,
and `merge_exclude_columns` ([dbt docs](https://docs.getdbt.com/docs/build/incremental-strategy?version=2#strategy-specific-configs)).

For example:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id'
) }}
```

Produces code like this on incremental runs:

```sql
merge into target_table as DBT_INTERNAL_DEST
using incoming_rows as DBT_INTERNAL_SOURCE
on DBT_INTERNAL_SOURCE.event_id = DBT_INTERNAL_DEST.event_id
when matched then update by name
when not matched then insert by name;
```

This adapter also supports the following options, under `config.meta`:
`DBT_INTERNAL_SOURCE` can be used to refer to incoming rows, and `DBT_INTERNAL_DEST` to the model table.

- `meta.merge_update_condition`: Only update matched rows that satisfy the condition, e.g. only update if the new records are more recent.

  ```sql
  {{ config(meta={
      'merge_update_condition': 'DBT_INTERNAL_SOURCE.updated_at > DBT_INTERNAL_DEST.updated_at'
  }) }}
  ```

- `meta.merge_insert_condition`: Only insert unmatched rows that satisfy a condition.

  ```sql
  {{ config(meta={
      'merge_insert_condition': 'DBT_INTERNAL_SOURCE.is_valid'
  }) }}
  ```

- `meta.merge_update_set_expressions`: Override SQL expressions for matched updates.

  ```sql
  {{ config(meta={
      'merge_update_set_expressions': {
          'version': 'coalesce(DBT_INTERNAL_DEST.version, 0) + 1'
      }
  }) }}
  ```

- `meta.merge_on_using_columns`: Match with `USING (columns)` instead of the generated `unique_key` condition.

  ```sql
  {{ config(meta={
      'merge_on_using_columns': ['event_id', 'event_time']
  }) }}
  ```

- `meta.merge_clauses`: Replace the corresponding default actions with `when_matched`
  and `when_not_matched` lists. Mutually exclusive with `merge_update_columns`,
  `merge_exclude_columns`, `meta.merge_update_condition`, `meta.merge_insert_condition`,
  and `meta.merge_update_set_expressions`.

  ```sql
  {{ config(meta={
      'merge_clauses': {
          'when_matched': [
              {
                  'action': 'update',
                  'mode': 'by_name',
                  'condition': 'DBT_INTERNAL_SOURCE.updated_at > DBT_INTERNAL_DEST.updated_at'
              }
          ],
          'when_not_matched': [
              {
                  'action': 'insert',
                  'mode': 'by_name',
                  'condition': 'DBT_INTERNAL_SOURCE.is_valid'
              }
          ]
      }
  }) }}
  ```

### microbatch

Replaces each event-time window using delete and insert. Requires `event_time`,
`batch_size`, and `begin`. See [the official dbt docs on microbatching](https://docs.getdbt.com/docs/build/incremental-microbatch).

- `incremental_predicates` adds filters to the target delete. Apply matching filters to the model query.
- `concurrent_batches` controls [parallel batch execution](https://docs.getdbt.com/docs/build/parallel-batch-execution): `true` enables parallel execution for independent batches; `false` runs them sequentially. When unset, dbt decides automatically. Use at least three threads to overlap batches of a single model.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin='2025-01-01',
    concurrent_batches=true,
    incremental_predicates=["event_type = 'purchase'"]
) }}

select event_id, event_time, event_type
from {{ source('raw', 'events') }}
where event_type = 'purchase'
```

For a daily batch:

```sql
delete from target_table
where event_time >= '2025-01-01' and event_time < '2025-01-02'
  and (event_type = 'purchase');

insert into target_table (event_id, event_time, event_type)
select event_id, event_time, event_type from incoming_rows
where event_time >= '2025-01-01' and event_time < '2025-01-02';
```

## Staging tables

Incremental models and snapshots create staging tables in `dbt_temp`. You can change the
staging schema with `meta={'temp_schema_name': 'custom_staging'}`. Normal runs will try to
cleanup after them, but some canceled or aborted runs might leave leftover tables in your
schema. If they accumulate, it's a good idea to clean them up regularly.
