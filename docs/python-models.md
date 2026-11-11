# Python models

`dbt-motherduck` runs [dbt Python models](https://docs.getdbt.com/docs/build/python-models) as [MotherDuck Flights](https://motherduck.com/docs/key-tasks/flights/). The Postgres endpoint that this adapter uses has no local Python runtime, so a model cannot execute in the dbt process.

Leave `submission_method` unset. It defaults to a Flight. `flight` and `flights` are the same choice. Any other value is rejected when the model is built, because this adapter has no local Python runtime.

Table and incremental models run on Flights. Python views and snapshots have no Python materialization here.

An ephemeral model is not run by dbt. dbt pastes its compiled code into the downstream query as a CTE (`with name as ( <compiled code> )`). That paste is a `SELECT` for a SQL model. A Python model compiles to a function, so there is nothing to paste and no adapter call that could run the Flight first. Use `materialized='table'` for a Python model other models `ref()`.

## How a model runs

1. dbt compiles the model and appends `dbt.ref`, `dbt.source`, `dbt.config`, `dbt.this`, and `dbt.is_incremental`.
2. The adapter adds `materialize()` and a `main()` that connects with `duckdb.connect("md:")` inside the Flight.
3. The worker creates or reuses a Flight and polls the run. Reusing a Flight whose source, requirements, token, timeout, and instance size are unchanged does not mint a new version. The Flight runs outside the worker's transaction.
4. The Flight writes an intermediate or staging table and commits when `main()` finishes. Inside-transaction pre-hooks, the rename or merge, indexes, and post-hooks then share one worker transaction, so a hook failure does not publish the final table.
5. Cancelling dbt, or exceeding `flights.timeout_sec`, cancels the Flight run and waits until it has stopped. A run left writing would recreate a temp table that dbt had already dropped.

Each model has its own Flight, named `dbt-<package>-<database>-<schema>-<alias>`. Names are unique per MotherDuck user, and only the owner can run a Flight. Override `motherduck__flight_name` when two projects would share a name.

A failed Flight raises in dbt with the run status, exit code, and a link to the logs. Set `flights.log_lines` to include the last lines of that log in the error. The log also contains the dependency install, so it stays out of dbt's output unless you ask for it.

## What the model can return

`dbt.ref()` and `dbt.source()` return a DuckDB relation from the Flight's connection. The model can return that relation, or anything `duckdb.cursor.register` accepts:

- a DuckDB relation, including `.limit()`, `.filter()`, and `.df()` when pandas is installed
- a pandas DataFrame
- a Polars DataFrame
- a pyarrow Table, Dataset, RecordBatchReader, or Scanner

DuckDB itself is pinned in the Flight (`flights.duckdb_version`, or 1.5.3). Other libraries are not installed unless you list them. An empty DuckDB relation keeps its column types when pyarrow is installed; without it, DuckDB may infer the schema through pandas and collapse an empty result.

```python
def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["pandas==2.2.3"],
        instance_type="F32",  # optional; this model only
    )
    df = dbt.ref("orders").filter("status = 'open'").df()
    return df.groupby("region", as_index=False).size()
```

`dbt.config` supports the usual model settings. For Python models this adapter also reads:

| Config | Purpose |
| --- | --- |
| `materialized` | `table` or `incremental`. |
| `packages` | Extra `requirements.txt` lines for this model. They override `flights.requirements` for the same distribution. |
| `instance_type` | Flight runner size: `F4`, `F16`, or `F32`. Overrides the profile. MotherDuck rejects a size your plan does not allow. |
| `submission_method` | Optional. Defaults to `flight`. `flights` is accepted. Anything else is rejected. |

Incremental models use the same strategies as SQL models. The Flight writes the staging table; the worker applies append, delete+insert, merge, or microbatch. `dbt.is_incremental` is false on the first build and true after that.

Profile `settings` are replayed as `SET` statements inside the Flight, so the model sees the same DuckDB configuration as SQL models on the worker. Settings named `motherduck_token`, `motherduck_access_token`, or `token` are not copied.

## Profile

```yaml
dev:
  type: motherduck
  host: pg.us-east-1-aws.motherduck.com
  database: analytics
  schema: main
  token: "{{ env_var('DBT_ENV_SECRET_MOTHERDUCK_TOKEN') }}"
  flights:
    access_token_name: analytics-writer  # optional; default is MotherDuck's Flights token
    duckdb_version: "1.5.3"              # optional
    timeout_sec: 3600                    # how long dbt waits before cancelling the run
    poll_interval_sec: 2
    max_runtime_sec: 1800                # optional MotherDuck-enforced limit
    instance_type: F16                   # optional default runner
    requirements:                        # installed for every Python model
      - pandas==2.2.3
    log_lines: 0                         # set above 0 to inline a failed run's log tail
```

`access_token_name` is the label of a token stored in MotherDuck, not the token value. The Flight runtime injects that token as `MOTHERDUCK_TOKEN`. The dbt profile token is only used for the Postgres session that creates and polls the Flight, and it is not written into the Flight source.

The Flight token must be allowed to read every relation the model refs or sources, and to write the database the model materializes. A fresh `md:` connection does not attach those databases, so the Flight runs `ATTACH IF NOT EXISTS` for every database dbt discovered before it reads a `ref` or `source`.

## Security

- Model source is stored on the Flight in your MotherDuck account. Anyone who can read that Flight can read the model.
- Do not put secrets in the model, in `packages`, or in profile `settings`. Token-like settings are stripped; other values are embedded as `SET` statements.
- Packages are installed from the Flight's requirements before `main()` runs. The model cannot install more packages, and it cannot see files on the machine running dbt.
- `module_paths` and dbt-duckdb plugins are not available. There is no local DuckDB connection.

## Not supported

- A `submission_method` other than `flight` or `flights`. Omitting it runs on a Flight
- Python views and snapshots
- Ephemeral Python models, because dbt inlines them as SQL CTEs and never runs them
- Reading local files, environment files, or the worker session's temporary tables
- Installing system packages from inside the model
- Returning a Spark `session.createDataFrame(...)` result; return a DuckDB relation or a dataframe library listed above
