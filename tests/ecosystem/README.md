# Ecosystem tests

Third-party dbt packages run against the adapter by the nightly `Ecosystem`
workflow. Versions are pinned only to a major range, so these can go red from
someone else's release; that is why they are not in the pull request run.

`adapter.dispatch` searches `motherduck__` then `default__` and nothing else, so
the `duckdb__` macros these packages ship are unreachable and the
Snowflake-shaped `default__` runs instead. Gaps go upstream as `motherduck__`
macros in the packages rather than as shims here, which would force a
`dispatch:` block on every user.

## Open upstream work

| package | macro | `default__` behaviour |
| --- | --- | --- |
| dbt_date | `convert_timezone` | emits `convert_timezone()`, absent in DuckDB |
| dbt_date | `day_name` | emits `to_char()`; `duckdb__` uses `dayname()` |
| dbt_date | `month_name` | emits `to_char()`; `duckdb__` uses `monthname()` |
| dbt_date | `iso_week_start` | `date_trunc('isoweek', ...)`, specifier not recognised |
| dbt_date | `iso_week_of_year` | `date_part('isoweek', ...)`, specifier not recognised |
| dbt_date | `week_start` | runs clean, answers wrong: Monday-based, contract is Sunday-based |
| dbt_date | `week_end` | runs clean, answers wrong: follows `week_start`, a day late |
| dbt_expectations | `regexp_instr` | emits `regexp_instr()`; `duckdb__` uses `regexp_matches()` |
| audit_helper | `quick_are_queries_identical` | raises for any adapter without a branch |

Do `convert_timezone` first: `now()` calls it and `today`/`tomorrow`/`yesterday`
route through `now()`, so it breaks every dbt_date macro called without an
explicit date, plus three dbt_expectations tests. Delegate to each package's
`duckdb__`, not its `postgres__` -- five of dbt_date's eleven are DuckDB-native,
and parity with dbt-duckdb is an explicit goal of this adapter.

Each row is a strict xfail, or an `EXPECTED_FAIL` entry in
`test_dbt_expectations.py`, and the value the test asserts is the one the
package's own `duckdb__` produces -- so the test is the spec for the fix, and an
upstream fix turns the nightly red. To retire one, bump the version floor in
that module's `packages` fixture and delete the entry.
