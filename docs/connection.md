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
      sslrootcert: certifi
      threads: 4
      settings:
        preserve_insertion_order: false
        TimeZone: Europe/Amsterdam
```

Choose the endpoint for your MotherDuck organization's [region](https://motherduck.com/docs/about-motherduck/cloud-regions/). `database` must name an existing database that the token can access.

| Option | Default | Purpose |
| --- | --- | --- |
| `host` | Required | Regional Postgres endpoint hostname. |
| `database` | Required | Default target database. |
| `schema` | Required | Default target schema. |
| `token` | Required with `authenticator: token` | [MotherDuck access token](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#authentication-using-an-access-token). |
| `authenticator` | `token` | `token`, or `externalbrowser` to [log in through the browser](#browser-login-sso). |
| `oauth_client_id` | MotherDuck's public client | Auth0 application used by `externalbrowser`. |
| `threads` | `1` | How many dbt models can execute in parallel. |
| `port` | `5432` | Endpoint port. |
| `connect_timeout` | `240` | Connection timeout in seconds. |
| `sslrootcert` | `certifi` | `certifi`, `system`, or a CA bundle path. |
| `attach_databases` | `[]` | Extra databases to attach to every session. |
| `settings` | `{}` | DuckDB settings applied to every new session. |

## Browser login (SSO)

For local development, log in through your browser instead of storing a token.
Organizations that enforce SSO get their identity provider's login page:

```yaml
dev:
  type: motherduck
  authenticator: externalbrowser
  host: pg.us-east-1-aws.motherduck.com
  database: analytics
  schema: main
```

On the first connection dbt prints a URL and code and opens your browser. The
resulting token is saved to `~/.motherduck/credentials.json` (or
`$MOTHERDUCK_HOME/credentials.json`), the same file `motherduck login` uses, so
either one logs in both. All threads share one login, and dbt logs in again if
the saved token has expired or is rejected. `token` must be unset or empty with
`authenticator: externalbrowser`; use the default `authenticator: token` in CI.

dbt logs in through MotherDuck's own public Auth0 application by default. Set
`oauth_client_id` to log in through a different MotherDuck-registered application,
for example when testing against a staging environment.

## Single attach mode and database discovery

Every connection uses MotherDuck's [single attach mode](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/attach-modes/). The adapter attaches all databases that dbt can detect by parsing your dbt project.

For example, this source makes `example_source` available automatically:

```yaml
version: 2
sources:
  - name: raw
    database: example_source
    schema: main
    tables:
      - name: events
```

Likewise, a model configured with `database='model_target_database'` causes `model_target_database` to be attached. These databases must already exist and be accessible to the token.

The adapter does not inspect raw SQL, hooks, or custom macros for database names. Add those databases to the profile's `attach_databases` list, which dbt also attaches. Use plain names such as `reference_data`, without an `md:` prefix or connection string.

```yaml
attach_databases:
  - extra_db_1
  - extra_db_2
```

## Settings

Use the profile's `settings` to configure [DuckDB settings](https://duckdb.org/docs/stable/configuration/overview) for every session. [`on-run-start` hooks](https://docs.getdbt.com/reference/project-configs/on-run-start-on-run-end) run on a different connection than dbt models, so they do not configure model workers.

Models on the same worker share settings. If you change a setting in a
[`pre_hook`](https://docs.getdbt.com/reference/resource-configs/pre-hook-post-hook), restore its profile value in a `post_hook`:

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

| Profile `sslrootcert` value | When to use it |
| --- | --- |
| `certifi` | Default: use certifi's bundled public CA certificates. |
| `system` | Use libpq/OpenSSL's system trust store when the environment manages trusted CAs. |
| `/path/to/ca-bundle.pem` | Use an explicit CA certificate or bundle on the machine running dbt. |
