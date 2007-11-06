#
# Copyright (c) 2006, 2007 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# Storm is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 2.1 of
# the License, or (at your option) any later version.
#
# Storm is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
from datetime import date, time, timedelta
import os

from storm.databases.postgres import Postgres, compile
from storm.uri import URI
from storm.database import create_database
from storm.variables import DateTimeVariable, RawStrVariable
from storm.variables import ListVariable, IntVariable, Variable
from storm.expr import Union, Select, Alias, SQLRaw, State, Sequence, Like

from tests.databases.base import (
    DatabaseTest, DatabaseDisconnectionTest, UnsupportedDatabaseTest)
from tests.databases.proxy import ProxyTCPServer
from tests.helper import TestHelper


class PostgresTest(DatabaseTest, TestHelper):

    def is_supported(self):
        return bool(os.environ.get("STORM_POSTGRES_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_POSTGRES_URI"])

    def create_tables(self):
        self.connection.execute("CREATE TABLE number "
                                "(one INTEGER, two INTEGER, three INTEGER)")
        self.connection.execute("CREATE TABLE test "
                                "(id SERIAL PRIMARY KEY, title VARCHAR)")
        self.connection.execute("CREATE TABLE datetime_test "
                                "(id SERIAL PRIMARY KEY,"
                                " dt TIMESTAMP, d DATE, t TIME)")
        self.connection.execute("CREATE TABLE bin_test "
                                "(id SERIAL PRIMARY KEY, b BYTEA)")
        self.connection.execute("CREATE TABLE like_case_insensitive_test "
                                "(id SERIAL PRIMARY KEY, description TEXT)")

    def drop_tables(self):
        super(PostgresTest, self).drop_tables()
        try:
            self.connection.execute("DROP TABLE like_case_insensitive_test")
            self.connection.commit()
        except:
            self.connection.rollback()

    def create_sample_data(self):
        super(PostgresTest, self).create_sample_data()
        self.connection.execute("INSERT INTO like_case_insensitive_test "
                                "(description) VALUES ('hullah')")
        self.connection.execute("INSERT INTO like_case_insensitive_test "
                                "(description) VALUES ('HULLAH')")
        self.connection.commit()

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

        state = State()
        statement = compile(variable, state)

        self.connection.execute("INSERT INTO array_test VALUES (1, %s)"
                                % statement, state.parameters)

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

        state = State()
        statement = compile(variable, state)

        self.connection.execute("INSERT INTO array_test VALUES (1, %s)"
                                % statement, state.parameters)

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

        state = State()
        statement = compile(expr, state)
        self.assertEquals(statement,
                          'SELECT * FROM '
                          '((SELECT 1 AS id) UNION ALL (SELECT 1)) AS "_1" '
                          'ORDER BY id+? LIMIT 1 OFFSET 1')
        self.assertEquals(state.parameters, [Variable(1)])

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

    def test_sequence(self):
        expr1 = Select(Sequence("test_id_seq"))
        expr2 = "SELECT currval('test_id_seq')"
        value1 = self.connection.execute(expr1).get_one()[0]
        value2 = self.connection.execute(expr2).get_one()[0]
        value3 = self.connection.execute(expr1).get_one()[0]
        self.assertEquals(value1, value2)
        self.assertEquals(value3-value1, 1)

    def test_like_case(self):
        expr = Like("name", "value")
        statement = compile(expr)
        self.assertEquals(statement, "? LIKE ?")
        expr = Like("name", "value", case_sensitive=True)
        statement = compile(expr)
        self.assertEquals(statement, "? LIKE ?")
        expr = Like("name", "value", case_sensitive=False)
        statement = compile(expr)
        self.assertEquals(statement, "? ILIKE ?")

    def test_case_default_like(self):

        like = Like(SQLRaw("description"), "%hullah%")
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = self.connection.execute(expr)
        self.assertEquals(result.get_all(), [(1,)])

        like = Like(SQLRaw("description"), "%HULLAH%")
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = self.connection.execute(expr)
        self.assertEquals(result.get_all(), [(2,)])

    def test_case_sensitive_like(self):

        like = Like(SQLRaw("description"), "%hullah%", case_sensitive=True)
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = self.connection.execute(expr)
        self.assertEquals(result.get_all(), [(1,)])

        like = Like(SQLRaw("description"), "%HULLAH%", case_sensitive=True)
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = self.connection.execute(expr)
        self.assertEquals(result.get_all(), [(2,)])

    def test_case_insensitive_like(self):

        like = Like(SQLRaw("description"), "%hullah%", case_sensitive=False)
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = self.connection.execute(expr)
        self.assertEquals(result.get_all(), [(1,), (2,)])
        like = Like(SQLRaw("description"), "%HULLAH%", case_sensitive=False)
        expr = Select(SQLRaw("id"), like, tables=["like_case_insensitive_test"])
        result = self.connection.execute(expr)
        self.assertEquals(result.get_all(), [(1,), (2,)])

    def test_none_on_string_variable(self):
        """
        Verify that the logic to enforce fix E''-styled strings isn't
        breaking on NULL values.
        """
        variable = RawStrVariable(value=None)
        result = self.connection.execute(Select(variable))
        self.assertEquals(result.get_one(), (None,))


class PostgresUnsupportedTest(UnsupportedDatabaseTest, TestHelper):

    dbapi_module_names = ["psycopg2"]
    db_module_name = "postgres"


class PostgresDisconnectionTest(DatabaseDisconnectionTest, TestHelper):

    def get_uri(self):
        uri_str = os.environ.get("STORM_POSTGRES_HOST_URI")
        if uri_str:
            uri = URI(uri_str)
            if not uri.host:
                raise RuntimeError("The URI in STORM_POSTGRES_HOST_URI "
                                   "must include a host.")
            return uri
        else:
            uri_str = os.environ.get("STORM_POSTGRES_URI")
            if uri_str:
                uri = URI(uri_str)
                if uri.host:
                    return uri
        return None

    def is_supported(self):
        return bool(self.get_uri())

    def create_database_and_proxy(self):
        uri = self.get_uri()
        self.proxy = ProxyTCPServer((uri.host, uri.port or 5432))
        uri.host, uri.port = self.proxy.server_address
        self.database = create_database(uri)
