import os

from storm.databases.sqlite import SQLite
from storm.database import create_database

from tests.databases.base import DatabaseTest
from tests.helper import TestHelper, MakePath


class SQLiteTest(TestHelper, DatabaseTest):

    helpers = [MakePath]

    def setUp(self):
        TestHelper.setUp(self)
        DatabaseTest.setUp(self)

    def tearDown(self):
        DatabaseTest.setUp(self)
        TestHelper.setUp(self)

    def create_database(self):
        self.database = SQLite(self.make_path())

    def create_tables(self):
        self.connection.execute("CREATE TABLE test "
                                "(id INTEGER PRIMARY KEY, title VARCHAR)")
        self.connection.execute("CREATE TABLE datetime_test "
                                "(id INTEGER PRIMARY KEY,"
                                " dt TIMESTAMP, d DATE, t TIME)")

    def drop_tables(self):
        pass

    def test_wb_create_database(self):
        filename = self.make_path()
        sqlite = create_database("sqlite:%s" % filename)
        self.assertTrue(isinstance(sqlite, SQLite))
        self.assertEquals(sqlite._filename, filename)


class SQLiteMemoryTest(SQLiteTest):
    
    def create_database(self):
        self.database = SQLite()

    def test_simultaneous_iter(self):
        pass

    def test_wb_create_database(self):
        sqlite = create_database("sqlite:")
        self.assertTrue(isinstance(sqlite, SQLite))
        self.assertEquals(sqlite._filename, ":memory:")
