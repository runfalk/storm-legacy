import os

from tests.databases.sqlite import SQLiteMemoryTest

from storm.databases.postgres import Postgres


class PostgresTest(SQLiteMemoryTest):

    def create_sample_data(self):
        self.database = Postgres(os.environ["STORM_POSTGRES_DBNAME"])
        self.connection = self.database.connect()
        self.connection.execute("CREATE TABLE test "
                                "(id SERIAL PRIMARY KEY, title VARCHAR)")
        self.connection.execute("INSERT INTO test VALUES (10, 'Title 10')")
        self.connection.execute("INSERT INTO test VALUES (20, 'Title 20')")
        self.connection.commit()
