# Reading and writing external files

`dbt-motherduck` can query remote CSV, JSON, and Parquet files through MotherDuck SQL. File access runs in MotherDuck, so paths must be reachable from the Ducklings.

## Reading files as sources

Set `external_location` under a source or table's `config.meta`:

```yaml
version: 2
sources:
  - name: files
    config:
      meta:
        external_location: "s3://my-bucket/raw/{name}/*.parquet"
    tables:
      - name: events
      - name: customers
      - name: adjustments
        config:
          meta:
            external_location: "read_csv('s3://my-bucket/adjustments/*.csv', header=true)"
```

Reference the source normally:

```sql
select * from {{ source('files', 'events') }}
```

`source('files', 'events')` expands to `'s3://my-bucket/raw/events/*.parquet'`,
substituting `{name}` with the table's dbt name. Locations also support
`{identifier}`, `{schema}`, and `{database}` from the
source definition. A table-level `external_location` overrides the source default
and can use `read_parquet(...)`, `read_csv(...)`, or `read_json(...)` to configure
reader options.

Local files are supported only through [dbt seeds](https://docs.getdbt.com/docs/build/seeds).

## Storage credentials

To read or write external data that is not public, configure credentials as [MotherDuck secrets](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-secret/).

## Writing files

`materialized='external'` is not supported by `dbt-motherduck`. Use MotherDuck's [`COPY`](https://motherduck.com/docs/sql-reference/duckdb-sql-reference/duckdb-statements/copy/) SQL to export a relation to remote storage. For example, a post-hook can export a table after building it:

```sql
{{ config(
    materialized='table',
    post_hook="COPY {{ this }} TO 's3://my-bucket/exports/events.parquet' (FORMAT PARQUET)"
) }}

select * from {{ source('files', 'events') }}
```

Exports also support [Google Cloud Storage](https://motherduck.com/docs/integrations/cloud-storage/google-cloud-storage/), [Cloudflare R2](https://motherduck.com/docs/integrations/cloud-storage/cloudflare-r2/), and [Azure Blob Storage](https://motherduck.com/docs/integrations/cloud-storage/azure-blob-storage/) with the provider's URI scheme and credentials. See [DuckDB `COPY`](https://duckdb.org/docs/stable/sql/statements/copy) for format and export options.
