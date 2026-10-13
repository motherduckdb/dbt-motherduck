# Contributing

## Development

Install the project and its development dependencies:

```shell
uv sync --locked
```

Run formatting, linting, and unit tests before opening a pull request:

```shell
uv run ruff format --check .
uv run ruff check .
uv run pytest -m unit
```

## Changelog entries

Pull requests that change adapter source code or package metadata must include a
changelog entry. Install [Changie](https://changie.dev/guide/installation/) and
run:

```shell
changie new
```

Choose the category that best describes the change, write a concise
user-oriented description, and enter the associated Linear issue identifier.
Commit the generated YAML file under `.changes/unreleased`.

Do not edit `CHANGELOG.md` or the version in `pyproject.toml` as part of an
ordinary pull request. The production publishing workflow accepts the target
PEP 440 version, updates `pyproject.toml` and `uv.lock`, batches unreleased
entries for that version, and commits the release preparation before building.
TestPyPI publishes the version already present on the selected branch or tag
without changing it.

Maintainers may apply the `skip:changelog-check` label to changes that do not
need release notes, such as documentation-only or CI-only pull requests.
