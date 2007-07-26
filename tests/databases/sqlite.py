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
import os
import sys

from storm.databases.sqlite import SQLite
from storm.database import create_database
from storm.uri import URI

from tests.databases.base import DatabaseTest, UnsupportedDatabaseTest
from tests.helper import TestHelper, MakePath


class SQLiteTest(DatabaseTest, TestHelper):

    helpers = [MakePath]

    def create_database(self):
        self.database = SQLite(URI("sqlite:" + self.make_path()))

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
        self.database = SQLite(URI("sqlite:"))

    def test_simultaneous_iter(self):
        pass

    def test_wb_create_database(self):
        sqlite = create_database("sqlite:")
        self.assertTrue(isinstance(sqlite, SQLite))
        self.assertEquals(sqlite._filename, ":memory:")


class SQLiteUnsupportedTest(UnsupportedDatabaseTest, TestHelper):
    
    dbapi_module_name = "pysqlite2"
    db_module_name = "sqlite"

    def is_supported(self):
        return sys.version_info[:2] < (2.5)

