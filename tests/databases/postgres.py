from datetime import datetime, date, time
import os

from storm.databases.postgres import Postgres
from storm.kinds import UnicodeKind

from tests.databases.base import DatabaseTest
from tests.helper import TestHelper


class PostgresTest(TestHelper, DatabaseTest):

    def setUp(self):
        TestHelper.setUp(self)
        DatabaseTest.setUp(self)

    def tearDown(self):
        DatabaseTest.setUp(self)
        TestHelper.setUp(self)
    
    def is_supported(self):
        return bool(os.environ.get("STORM_POSTGRES_DBNAME"))

    def create_database(self):
        self.database = Postgres(os.environ["STORM_POSTGRES_DBNAME"])

    def create_tables(self):
        self.connection.execute("CREATE TABLE test "
                                "(id SERIAL PRIMARY KEY, title VARCHAR)")
        self.connection.execute("CREATE TABLE datetime_test "
                                "(id SERIAL PRIMARY KEY,"
                                " dt TIMESTAMP, d DATE, t TIME)")

    def test_unicode_with_database_encoding(self):
        encoding = "iso-8859-1"
        raw_str = "\xe1\xe9\xed\xf3\xfa"
        uni_str = raw_str.decode(encoding)

        database = Postgres(os.environ["STORM_POSTGRES_DBNAME"],
                            encoding=encoding)

        connection = database.connect()
        connection.execute("SET client_encoding=?", (encoding,))
        connection.execute("INSERT INTO test VALUES (1, ?)", (raw_str,))

        result = connection.execute("SELECT title FROM test WHERE id=1")
        title = result.get_one()[0]

        self.assertTrue(isinstance(title, str))
        self.assertEquals(result.to_kind(title, UnicodeKind()), uni_str)

    def test_unicode_with_default_encoding(self):
        encoding = "utf-8"
        raw_str = "\xc3\xa1\xc3\xa9\xc3\xad\xc3\xb3\xc3\xba"
        uni_str = raw_str.decode(encoding)

        database = Postgres(os.environ["STORM_POSTGRES_DBNAME"])

        connection = database.connect()
        connection.execute("SET client_encoding=?", (encoding,))
        connection.execute("INSERT INTO test VALUES (1, ?)", (raw_str,))

        result = connection.execute("SELECT title FROM test WHERE id=1")
        title = result.get_one()[0]

        self.assertTrue(isinstance(title, str))
        self.assertEquals(result.to_kind(title, UnicodeKind()), uni_str)
