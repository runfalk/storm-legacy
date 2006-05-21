import os

from tests.databases.base import DatabaseTest
from tests.helper import MakePath

from storm.databases.sqlite import SQLite


class SQLiteTest(DatabaseTest):

    helpers = [MakePath]

    def create_database(self):
        self.database = SQLite(self.make_path())

    def create_table(self):
        self.connection.execute("CREATE TABLE test "
                                "(id SERIAL PRIMARY KEY, title VARCHAR)")

    def drop_table(self):
        pass


class SQLiteMemoryTest(SQLiteTest):
    
    def create_database(self):
        self.database = SQLite()

    def test_simultaneous_iter(self):
        pass


del DatabaseTest
