from datetime import datetime, date, time
import os, storm

from storm.databases.postgres import Postgres, compile, parse_array
from storm.uri import URI
from storm.database import create_database
from storm.variables import UnicodeVariable, DateTimeVariable
from storm.variables import ListVariable, IntVariable, Variable
from storm.expr import Union, Select, Alias, SQLRaw, Like, Table

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
        connection.execute("INSERT INTO test VALUES (1, ?)", (uni_str,))

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
        connection.execute("INSERT INTO test VALUES (1, ?)", (uni_str,))

        result = connection.execute("SELECT title FROM test WHERE id=1")
        title = result.get_one()[0]

        self.assertTrue(isinstance(title, str))

        variable = UnicodeVariable()
        result.set_variable(variable, title)
        self.assertEquals(variable.get(), uni_str)

    def test_unicode_with_unicode_data(self):
        # Psycopg can be configured to return unicode objects for
        # string columns (for example, psycopgda does this).
        uni_str = u'\xe1\xe9\xed\xf3\xfa'

        connection = self.database.connect()
        result = connection.execute("SELECT TRUE")

        variable = UnicodeVariable()
        result.set_variable(variable, uni_str)
        self.assertEquals(variable.get(), uni_str)

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

        self.assertTrue(isinstance(array, str))

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

        self.assertTrue(isinstance(array, str))

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

    def test_case_default_like(self):
        try:
            self.connection.execute("DROP TABLE like_case_insensitive_test")
            self.connection.commit()
        except:
            self.connection.rollback()

        self.connection.execute("CREATE TABLE like_case_insensitive_test "
                                "(id SERIAL PRIMARY KEY, description TEXT)")

        self.connection.execute("INSERT INTO like_case_insensitive_test "
                                "(description) VALUES ('hullah')")
        self.connection.execute("INSERT INTO like_case_insensitive_test "
                                "(description) VALUES ('HULLAH')")
        self.connection.commit()

        like = Like(SQLRaw("description"), "%hullah%")
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = self.connection.execute(expr)
        self.assertEquals(result.get_all(), [(1,)])

        like = Like(SQLRaw("description"), "%HULLAH%")
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = self.connection.execute(expr)
        self.assertEquals(result.get_all(), [(2,)])

    def test_case_sensitive_like(self):
        try:
            self.connection.execute("DROP TABLE like_case_insensitive_test")
            self.connection.commit()
        except:
            self.connection.rollback()

        self.connection.execute("CREATE TABLE like_case_insensitive_test "
                                "(id SERIAL PRIMARY KEY, description TEXT)")

        self.connection.execute("INSERT INTO like_case_insensitive_test "
                                "(description) VALUES ('hullah')")
        self.connection.execute("INSERT INTO like_case_insensitive_test "
                                "(description) VALUES ('HULLAH')")
        self.connection.commit()

        like = Like(SQLRaw("description"), "%hullah%", case_sensitive=True)
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = self.connection.execute(expr)
        self.assertEquals(result.get_all(), [(1,)])

        like = Like(SQLRaw("description"), "%HULLAH%", case_sensitive=True)
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = self.connection.execute(expr)
        self.assertEquals(result.get_all(), [(2,)])

    def test_case_insensitive_like(self):
        try:
            self.connection.execute("DROP TABLE like_case_insensitive_test")
            self.connection.commit()
        except:
            self.connection.rollback()

        self.connection.execute("CREATE TABLE like_case_insensitive_test "
                                "(id SERIAL PRIMARY KEY, description TEXT)")

        self.connection.execute("INSERT INTO like_case_insensitive_test "
                                "(description) VALUES ('HULLAH')")

        self.connection.execute("INSERT INTO like_case_insensitive_test "
                                "(description) VALUES ('hullah')")
        self.connection.commit()

        like = Like(SQLRaw("description"), "%hullah%", case_sensitive=False)
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = self.connection.execute(expr)
        self.assertEquals(result.get_all(), [(1,), (2,)])
        like = Like(SQLRaw("description"), "%HULLAH%", case_sensitive=False)
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = self.connection.execute(expr)
        self.assertEquals(result.get_all(), [(1,), (2,)])

class ParseArrayTest(TestHelper):

    def test_parse_array(self):
        data = r'{{meeting,lunch},{ training , "presentation"},"{}","\"",NULL}'
        obj = parse_array(data)
        self.assertEquals(obj,
                          [["meeting", "lunch"],
                           ["training", "presentation"], "{}", '"', None])


class PostgresUnsupportedTest(UnsupportedDatabaseTest, TestHelper):
    
    dbapi_module_name = "psycopg"
    db_module_name = "postgres"
