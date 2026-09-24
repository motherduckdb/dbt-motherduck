import pytest

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption(
        "--database-type", choices=("native", "ducklake"), default="native"
    )


def pytest_collection_modifyitems(items):
    for item in items:
        category = item.path.relative_to(item.config.rootpath / "tests").parts[0]
        if category in ("unit", "integration"):
            item.add_marker(getattr(pytest.mark, category))
