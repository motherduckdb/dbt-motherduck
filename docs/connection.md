# Connection

`dbt-motherduck` connects to MotherDuck's [Postgres endpoint](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/postgres-endpoint/).

## Profile configuration

```yaml
dwh:
  target: dev
  outputs:
    dev:
      type: motherduck
      host: pg.us-east-1-aws.motherduck.com
      database: analytics
      schema: main
      token: "{{ env_var('DBT_ENV_SECRET_MOTHERDUCK_TOKEN') }}"
      threads: 4
      settings:
        preserve_insertion_order: false
        TimeZone: Europe/Amsterdam
```

Choose the endpoint for your MotherDuck organization's . `database` must name an existing database that the token can access.

TODO: is the user config supported by the profile? I don't htink it should be configurable since it changes nothing?
TODO: fot the certificate what are options that a user might want to change?
TODO: move threads to the table, and mention "How many dbt models can execute in parallel."
TODO: for ssqlrootcert mention the configuration value that is use

| Option             | Default             | Purpose                                                                                                                                                                          |
|--------------------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `host`             | Required            | [Regional](https://motherduck.com/docs/about-motherduck/cloud-regions/) Postgres endpoint hostname.                                                                              |
| `database`         | Required            | Default target database.                                                                                                                                                         |
| `schema`           | Required            | Default target schema.                                                                                                                                                           |
| `token`            | Required            | [MotherDuck access token](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#authentication-using-an-access-token). |
| `port`             | `5432`              | Endpoint port.                                                                                                                                                                   |
| `user`             | `postgres`          | Postgres protocol username; authentication uses the token.                                                                                                                       |
| `connect_timeout`  | `240`               | Connection timeout in seconds.                                                                                                                                                   |
| `sslrootcert`      | certifi's CA bundle | CA bundle used to verify the server certificate.                                                                                                                                 |
| `attach_databases` | `[]`                | Extra databases to attach to every session.                                                                                                                                      |
| `settings`         | `{}`                | DuckDB settings applied to every new session.                                                                                                                                    |

## Single attach mode and database discovery

Every connection uses MotherDuck's [single attach mode](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/attach-modes/). The adapter attaches all databases that dbt can detect by parsing your dbt project.

For example, this source makes `raw_events` available automatically:

```yaml
version: 2
sources:
  - name: raw
    database: raw_events
    schema: main
    tables:
      - name: events
```

Likewise, a model configured with `database='reporting'` causes `reporting` to be attached. These databases must already exist and be accessible to the token.

The adapter does not inspect raw SQL, hooks, or custom macros for database names. Add those databases to the profile's `attach_databases` list, which dbt also attaches. Use plain names such as `reference_data`, without an `md:` prefix or connection string.

TODO: add example of attached dbs

## Settings and hooks

TODO: link to duckdb settings docs
TODO: link to dbt on-run-start docs
Use the profile's `settings` to configure values for the complete dbt run. `on-run-start` hooks run on a different connection than dbt models, so it's not possible to configure settings in that way.

TODO: add link to pre and post hooks dbt docs
Different models that execute on the same dbt worker will use the same settings, so if you modify a configuration for a specific model via a `pre_hook`, don't forget to restore its value with a `post_hook`.

```sql
{{ config(
    pre_hook="SET preserve_insertion_order = true",
    post_hook="SET preserve_insertion_order = " ~
        (target.settings.get('preserve_insertion_order', true) | lower)
) }}
```

## TLS certificates

TLS always verifies the certificate chain and hostname with `sslmode=verify-full`.
See MotherDuck's [SSL reference](https://motherduck.com/docs/sql-reference/postgres-endpoint/#ssl-and-certificate-verification) for endpoint certificate details.


TODO: make this a nice md table
│ Profile `sslrootcert`  setting                     │ When to use it                                                                                                   │
├─────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Empty                    │ Default: use certifi’s bundled public CA certificates.                                                           │
│ `system`                 │ Use libpq/OpenSSL’s system trust store, useful when the environment centrally manages trusted CAs.               │
│ `/path/to/ca-bundle.pem` │ Use an explicit CA certificate or bundle on the machine running dbt, for example a company-managed trust bundle. │
