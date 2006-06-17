from datetime import datetime, date, time
import os

from storm.databases.postgres import Postgres
from storm.uri import URI
from storm.database import create_database
from storm.variables import UnicodeVariable, DateTimeVariable

from tests.databases.base import DatabaseTest, UnsupportedDatabaseTest
from tests.helper import TestHelper, MakePath


class PostgresTest(TestHelper, DatabaseTest):

    def setUp(self):
        TestHelper.setUp(self)
        DatabaseTest.setUp(self)

    def tearDown(self):
        DatabaseTest.setUp(self)
        TestHelper.setUp(self)
    
    def is_supported(self):
        return bool(os.environ.get("STORM_POSTGRES_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_POSTGRES_URI"])

    def create_tables(self):
        self.connection.execute("CREATE TABLE test "
                                "(id SERIAL PRIMARY KEY, title VARCHAR)")
        self.connection.execute("CREATE TABLE datetime_test "
                                "(id SERIAL PRIMARY KEY,"
                                " dt TIMESTAMP, d DATE, t TIME)")
        self.connection.execute("CREATE TABLE bin_test "
                                "(id SERIAL PRIMARY KEY, b BYTEA)")

    def test_wb_create_database(self):
        database = create_database("postgres://un:pw@ht:12/db?encoding=en")
        self.assertTrue(isinstance(database, Postgres))
        self.assertEquals(database._dsn,
                          "dbname=db host=ht port=12 user=un password=pw")
        self.assertEquals(database._encoding, "en")

    def test_unicode_with_database_encoding(self):
        encoding = "iso-8859-1"
        raw_str = "\xe1\xe9\xed\xf3\xfa"
        uni_str = raw_str.decode(encoding)

        database = create_database(os.environ["STORM_POSTGRES_URI"]
                                   + "?encoding=%s" % encoding)

        connection = database.connect()
        connection.execute("SET client_encoding=?", (encoding,))
        connection.execute("INSERT INTO test VALUES (1, ?)", (raw_str,))

        result = connection.execute("SELECT title FROM test WHERE id=1")
        title = result.get_one()[0]

        self.assertTrue(isinstance(title, str))

        variable = UnicodeVariable()
        result.set_variable(variable, title)
        self.assertEquals(variable.get(), uni_str)

    def test_unicode_with_default_encoding(self):
        encoding = "utf-8"
        raw_str = "\xc3\xa1\xc3\xa9\xc3\xad\xc3\xb3\xc3\xba"
        uni_str = raw_str.decode(encoding)

        connection = self.database.connect()
        connection.execute("SET client_encoding=?", (encoding,))
        connection.execute("INSERT INTO test VALUES (1, ?)", (raw_str,))

        result = connection.execute("SELECT title FROM test WHERE id=1")
        title = result.get_one()[0]

        self.assertTrue(isinstance(title, str))

        variable = UnicodeVariable()
        result.set_variable(variable, title)
        self.assertEquals(variable.get(), uni_str)

    def test_datetime_with_none(self):
        self.connection.execute("INSERT INTO datetime_test (dt) VALUES (NULL)")
        result = self.connection.execute("SELECT dt FROM datetime_test")
        variable = DateTimeVariable()
        result.set_variable(variable, result.get_one()[0])
        self.assertEquals(variable.get(), None)


class MySQLUnsupportedTest(UnsupportedDatabaseTest, TestHelper):
    
    dbapi_module_name = "psycopg"
    db_module_name = "postgres"
