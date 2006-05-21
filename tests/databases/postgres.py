import os

from tests.databases.base import DatabaseTest

from storm.databases.postgres import Postgres


class PostgresTest(DatabaseTest):
    
    def is_supported(self):
        return bool(os.environ.get("STORM_POSTGRES_DBNAME"))

    def create_database(self):
        self.database = Postgres(os.environ["STORM_POSTGRES_DBNAME"])

    def create_table(self):
        self.connection.execute("CREATE TABLE test "
                                "(id SERIAL PRIMARY KEY, title VARCHAR)")

del DatabaseTest
