from importlib.metadata import version as distribution_version

# dbt's version parser requires a SemVer prerelease separator.
version = distribution_version("dbt-motherduck").replace(".dev", "-dev")
