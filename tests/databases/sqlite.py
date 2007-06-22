import os

from storm.databases.sqlite import SQLite
from storm.database import create_database
from storm.uri import URI

from tests.databases.base import DatabaseTest, UnsupportedDatabaseTest
from tests.helper import TestHelper, MakePath


class SQLiteTest(DatabaseTest, TestHelper):

    helpers = [MakePath]

    def create_database(self):
        self.database = SQLite(URI.parse("sqlite:" + self.make_path()))

    def create_tables(self):
        self.connection.execute("CREATE TABLE test "
                                "(id INTEGER PRIMARY KEY, title VARCHAR)")
        self.connection.execute("CREATE TABLE datetime_test "
                                "(id INTEGER PRIMARY KEY,"
                                " dt TIMESTAMP, d DATE, t TIME)")
        self.connection.execute("CREATE TABLE bin_test "
                                "(id INTEGER PRIMARY KEY, b BLOB)")

    def drop_tables(self):
        pass

    def test_wb_create_database(self):
        filename = self.make_path()
        sqlite = create_database("sqlite:%s" % filename)
        self.assertTrue(isinstance(sqlite, SQLite))
        self.assertEquals(sqlite._filename, filename)


class SQLiteMemoryTest(SQLiteTest):
    
    def create_database(self):
        self.database = SQLite(URI.parse("sqlite:"))

    def test_simultaneous_iter(self):
        pass

    def test_wb_create_database(self):
        sqlite = create_database("sqlite:")
        self.assertTrue(isinstance(sqlite, SQLite))
        self.assertEquals(sqlite._filename, ":memory:")


class SQLiteUnsupportedTest(UnsupportedDatabaseTest, TestHelper):
    
    dbapi_module_name = "pysqlite2"
    db_module_name = "sqlite"

