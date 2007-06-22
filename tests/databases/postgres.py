from datetime import datetime, date, time, timedelta
import os

from storm.databases.postgres import Postgres, compile
from storm.uri import URI
from storm.database import create_database
from storm.variables import UnicodeVariable, DateTimeVariable
from storm.variables import ListVariable, IntVariable, Variable
from storm.expr import Union, Select, Alias, SQLRaw

from tests.databases.base import DatabaseTest, UnsupportedDatabaseTest
from tests.helper import TestHelper, MakePath


class PostgresTest(DatabaseTest, TestHelper):

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
        database = create_database("postgres://un:pw@ht:12/db")
        self.assertTrue(isinstance(database, Postgres))
        self.assertEquals(database._dsn,
                          "dbname=db host=ht port=12 user=un password=pw")

    def test_utf8_client_encoding(self):
        connection = self.database.connect()
        result = connection.execute("SHOW client_encoding")
        encoding = result.get_one()[0]
        self.assertEquals(encoding.upper(), "UTF8")

    def test_unicode(self):
        raw_str = "\xc3\xa1\xc3\xa9\xc3\xad\xc3\xb3\xc3\xba"
        uni_str = raw_str.decode("UTF-8")

        connection = self.database.connect()
        connection.execute("INSERT INTO test VALUES (1, '%s')" % raw_str)

        result = connection.execute("SELECT title FROM test WHERE id=1")
        title = result.get_one()[0]

        self.assertTrue(isinstance(title, unicode))
        self.assertEquals(title, uni_str)

    def test_unicode_array(self):
        raw_str = "\xc3\xa1\xc3\xa9\xc3\xad\xc3\xb3\xc3\xba"
        uni_str = raw_str.decode("UTF-8")

        connection = self.database.connect()
        result = connection.execute("""SELECT '{"%s"}'::TEXT[]""" % raw_str)
        self.assertEquals(result.get_one()[0], [uni_str])
        result = connection.execute("""SELECT ?::TEXT[]""", ([uni_str],))
        self.assertEquals(result.get_one()[0], [uni_str])

    def test_time(self):
        connection = self.database.connect()
        value = time(12, 34)
        result = connection.execute("SELECT ?::TIME", (value,))
        self.assertEquals(result.get_one()[0], value)

    def test_date(self):
        connection = self.database.connect()
        value = date(2007, 6, 22)
        result = connection.execute("SELECT ?::DATE", (value,))
        self.assertEquals(result.get_one()[0], value)

    def test_interval(self):
        connection = self.database.connect()
        value = timedelta(365)
        result = connection.execute("SELECT ?::INTERVAL", (value,))
        self.assertEquals(result.get_one()[0], value)

    def test_datetime_with_none(self):
        self.connection.execute("INSERT INTO datetime_test (dt) VALUES (NULL)")
        result = self.connection.execute("SELECT dt FROM datetime_test")
        variable = DateTimeVariable()
        result.set_variable(variable, result.get_one()[0])
        self.assertEquals(variable.get(), None)

    def test_array_support(self):
        try:
            self.connection.execute("DROP TABLE array_test")
            self.connection.commit()
        except:
            self.connection.rollback()

        self.connection.execute("CREATE TABLE array_test "
                                "(id SERIAL PRIMARY KEY, a INT[])")

        variable = ListVariable(IntVariable)
        variable.set([1,2,3,4])

        statement, params = compile(variable)

        self.connection.execute("INSERT INTO array_test VALUES (1, %s)"
                                % statement, params)

        result = self.connection.execute("SELECT a FROM array_test WHERE id=1")

        array = result.get_one()[0]

        self.assertTrue(isinstance(array, list))

        variable = ListVariable(IntVariable)
        result.set_variable(variable, array)
        self.assertEquals(variable.get(), [1,2,3,4])

    def test_array_support_with_empty(self):
        try:
            self.connection.execute("DROP TABLE array_test")
            self.connection.commit()
        except:
            self.connection.rollback()

        self.connection.execute("CREATE TABLE array_test "
                                "(id SERIAL PRIMARY KEY, a INT[])")

        variable = ListVariable(IntVariable)
        variable.set([])

        statement, params = compile(variable)

        self.connection.execute("INSERT INTO array_test VALUES (1, %s)"
                                % statement, params)

        result = self.connection.execute("SELECT a FROM array_test WHERE id=1")

        array = result.get_one()[0]

        self.assertTrue(isinstance(array, list))

        variable = ListVariable(IntVariable)
        result.set_variable(variable, array)
        self.assertEquals(variable.get(), [])

    def test_expressions_in_union_order_by(self):
        # The following statement breaks in postgres:
        #     SELECT 1 AS id UNION SELECT 1 ORDER BY id+1;
        # With the error:
        #     ORDER BY on a UNION/INTERSECT/EXCEPT result must
        #     be on one of the result columns
        column = SQLRaw("1")
        Alias.auto_counter = 0
        alias = Alias(column, "id")
        expr = Union(Select(alias), Select(column), order_by=alias+1,
                     limit=1, offset=1, all=True)

        statement, parameters = compile(expr)
        self.assertEquals(statement,
                          "SELECT * FROM "
                          "((SELECT 1 AS id) UNION ALL (SELECT 1)) AS _1 "
                          "ORDER BY id+? LIMIT 1 OFFSET 1")
        self.assertEquals(parameters, [Variable(1)])

        result = self.connection.execute(expr)
        self.assertEquals(result.get_one(), (1,))

    def test_expressions_in_union_in_union_order_by(self):
        column = SQLRaw("1")
        alias = Alias(column, "id")
        expr = Union(Select(alias), Select(column), order_by=alias+1,
                     limit=1, offset=1, all=True)
        expr = Union(expr, expr, order_by=alias+1, all=True)
        result = self.connection.execute(expr)
        self.assertEquals(result.get_all(), [(1,), (1,)])


class PostgresUnsupportedTest(UnsupportedDatabaseTest, TestHelper):
    
    dbapi_module_name = "psycopg2"
    db_module_name = "postgres"
