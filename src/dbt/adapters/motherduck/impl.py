from dbt.adapters.sql import SQLAdapter

from dbt.adapters.motherduck.connections import MotherDuckConnectionManager


class MotherDuckAdapter(SQLAdapter):
    ConnectionManager = MotherDuckConnectionManager

    @classmethod
    def date_function(cls):
        return "current_timestamp"
