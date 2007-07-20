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
import time
import os

from storm.exceptions import OperationalError
from storm.databases.sqlite import SQLite
from storm.database import create_database
from storm.uri import URI

from tests.databases.base import DatabaseTest, UnsupportedDatabaseTest
from tests.helper import TestHelper, MakePath


class SQLiteMemoryTest(DatabaseTest, TestHelper):

    helpers = [MakePath]
    
    def get_path(self):
        return ""

    def get_path(self):
        return self.make_path()

    def create_database(self):
        self.database = SQLite(URI("sqlite:%s?timeout=0" % self.get_path()))

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
        database = create_database("sqlite:")
        self.assertTrue(isinstance(database, SQLite))
        self.assertEquals(database._filename, ":memory:")



class SQLiteFileTest(SQLiteMemoryTest):

    def get_path(self):
        return self.make_path()

    def test_wb_create_database(self):
        filename = self.make_path()
        database = create_database("sqlite:%s" % filename)
        self.assertTrue(isinstance(database, SQLite))
        self.assertEquals(database._filename, filename)

    def test_timeout(self):
        database = create_database("sqlite:%s?timeout=0.3" % self.get_path())
        connection1 = database.connect()
        connection2 = database.connect()
        connection1.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        connection1.execute("INSERT INTO test VALUES (1)")
        started = time.time()
        try:
            connection2.execute("INSERT INTO test VALUES (2)")
        except OperationalError:
            self.assertTrue(time.time()-started > 0.3)
        else:
            self.fail("OperationalError not raised")

    def get_connection_pair(self):
        database = create_database("sqlite:%s?timeout=0" % self.get_path())
        connection1 = database.connect()
        connection2 = database.connect()
        connection1.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        connection1.commit()
        return connection1, connection2

    def test_wb_begin_transaction_on_select(self):
        connection1, connection2 = self.get_connection_pair()
        connection1.execute("SELECT * FROM test")
        connection2.execute("INSERT INTO test VALUES (2)")
        self.assertEquals(connection1._in_transaction, True)
        try:
            connection2.commit()
        except OperationalError:
            pass
        else:
            self.fail("OperationalError not raised")

    def test_wb_begin_transaction_on_insert(self):
        connection1, connection2 = self.get_connection_pair()
        connection1.execute("INSERT INTO test VALUES (1)")
        self.assertEquals(connection1._in_transaction, True)
        try:
            connection2.execute("INSERT INTO test VALUES (2)")
        except OperationalError:
            pass
        else:
            self.fail("OperationalError not raised")

    def test_wb_begin_transaction_on_update(self):
        connection1, connection2 = self.get_connection_pair()
        connection1.execute("UPDATE test SET id=1 WHERE NULL")
        self.assertEquals(connection1._in_transaction, True)
        try:
            connection2.execute("INSERT INTO test VALUES (2)")
        except OperationalError:
            pass
        else:
            self.fail("OperationalError not raised")

    def test_wb_begin_transaction_on_replace(self):
        connection1, connection2 = self.get_connection_pair()
        connection1.execute("REPLACE INTO test VALUES (1)")
        self.assertEquals(connection1._in_transaction, True)
        try:
            connection2.execute("INSERT INTO test VALUES (2)")
        except OperationalError:
            pass
        else:
            self.fail("OperationalError not raised")

    def test_wb_begin_transaction_on_delete(self):
        connection1, connection2 = self.get_connection_pair()
        connection1.execute("DELETE FROM test")
        self.assertEquals(connection1._in_transaction, True)
        try:
            connection2.execute("INSERT INTO test VALUES (2)")
        except OperationalError:
            pass
        else:
            self.fail("OperationalError not raised")

    def test_wb_end_transaction_on_commit(self):
        self.connection.execute("SELECT * FROM test")
        self.assertEquals(self.connection._in_transaction, True)
        self.connection.commit()
        self.assertEquals(self.connection._in_transaction, False)

    def test_wb_end_transaction_on_rollback(self):
        self.connection.execute("SELECT * FROM test")
        self.assertEquals(self.connection._in_transaction, True)
        self.connection.rollback()
        self.assertEquals(self.connection._in_transaction, False)

    def test_wb_begin_transaction_again_after_unknown_operation(self):
        connection1, connection2 = self.get_connection_pair()
        connection1.execute("SELECT * FROM test")
        self.assertEquals(connection1._in_transaction, True)
        connection1.execute("PRAGMA encoding")
        self.assertEquals(connection1._in_transaction, False)
        connection1.execute("SELECT * FROM test")
        connection2.execute("INSERT INTO test VALUES (2)")
        try:
            connection2.commit()
        except OperationalError:
            pass
        else:
            self.fail("OperationalError not raised")


class SQLiteUnsupportedTest(UnsupportedDatabaseTest, TestHelper):
    
    dbapi_module_name = "pysqlite2"
    db_module_name = "sqlite"

