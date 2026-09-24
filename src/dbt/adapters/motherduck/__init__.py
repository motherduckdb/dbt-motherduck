from dbt.adapters.base import AdapterPlugin

from dbt.adapters.motherduck.credentials import MotherDuckCredentials
from dbt.adapters.motherduck.impl import MotherDuckAdapter
from dbt.include.motherduck import PACKAGE_PATH

Plugin = AdapterPlugin(
    adapter=MotherDuckAdapter,
    credentials=MotherDuckCredentials,
    include_path=PACKAGE_PATH,
)
