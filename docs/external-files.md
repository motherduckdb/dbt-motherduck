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
-- a model .sql file

select * from {{ source('files', 'events') }}
```

TODO: simplify this paragraoh and the next. Just explain how the source macro expands  and mention the other available functions
`source('files', 'events')` renders as `'s3://my-bucket/raw/events/*.parquet'`. A table-level location overrides the source default. You can also supply a reader call such as `read_parquet(...)`, `read_csv(...)`, or `read_json(...)` to set reader options or read a list of paths.

The default formatter substitutes `{name}`, `{identifier}`, `{schema}`, `{database}`, and other source metadata. Reader expressions containing literal braces, such as a CSV `types` mapping, need those braces escaped as `{{` and `}}`, or `formatter: oldstyle` to preserve them. Compatibility formatters also support `%(name)s` with `formatter: oldstyle` and `${name}` with `formatter: template`.


TODO: Just mention that the only local files are seeds, and link to dbt seed docs, don't explain what they are
For small, local CSV files, `dbt seed` is supported: dbt reads the file locally and sends its rows to MotherDuck.

## Storage credentials

If the location of the external source data is not public, you need to configure the required credentials as MotherDuck secrets (TODO docs link here)

TODO: don't mention all this, just rely on people above looking into the docs
`IN MOTHERDUCK` stores the secret in MotherDuck so it is available across that user's sessions, including dbt workers. `SCOPE` selects the paths that use these credentials. For temporary AWS credentials, also include `SESSION_TOKEN '<aws-session-token>'`. See the [MotherDuck `CREATE SECRET` reference](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-secret/) and [S3 credentials guide](https://motherduck.com/docs/integrations/cloud-storage/amazon-s3/) for the supported options.


## Writing files

`materialized='external'` is not supported by `dbt-motherduck`. Use MotherDuck's [`COPY`](https://motherduck.com/docs/sql-reference/duckdb-sql-reference/duckdb-statements/copy/) SQL to export a relation to remote storage. For example, a post-hook can export a table after building it:

```sql
{{ config(
    materialized='table',
    post_hook="COPY {{ this }} TO 's3://my-bucket/exports/events.parquet' (FORMAT PARQUET)"
) }}

select * from {{ source('files', 'events') }}
```

TODO: only to s3? no other location types?
See [writing data to S3](https://motherduck.com/docs/key-tasks/cloud-storage/writing-to-s3/) for supported formats and export options.
