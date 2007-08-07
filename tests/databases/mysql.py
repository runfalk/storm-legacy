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
from datetime import datetime, date, time
import os

from storm.databases.mysql import MySQL
from storm.database import create_database

from tests.databases.base import DatabaseTest, UnsupportedDatabaseTest
from tests.helper import TestHelper


class MySQLTest(DatabaseTest, TestHelper):

    supports_microseconds = False

    def is_supported(self):
        return bool(os.environ.get("STORM_MYSQL_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_MYSQL_URI"])

    def create_tables(self):
        self.connection.execute("CREATE TABLE test "
                                "(id INT AUTO_INCREMENT PRIMARY KEY,"
                                " title VARCHAR(50)) ENGINE=InnoDB")
        self.connection.execute("CREATE TABLE datetime_test "
                                "(id INT AUTO_INCREMENT PRIMARY KEY,"
                                " dt TIMESTAMP, d DATE, t TIME) ENGINE=InnoDB")
        self.connection.execute("CREATE TABLE bin_test "
                                "(id INT AUTO_INCREMENT PRIMARY KEY,"
                                " b BLOB) ENGINE=InnoDB")

    def test_wb_create_database(self):
        database = create_database("mysql://un:pw@ht:12/db?unix_socket=us")
        self.assertTrue(isinstance(database, MySQL))
        for key, value in [("db", "db"), ("host", "ht"), ("port", 12),
                           ("user", "un"), ("passwd", "pw"),
                           ("unix_socket", "us")]:
            self.assertEquals(database._connect_kwargs.get(key), value)


class MySQLUnsupportedTest(UnsupportedDatabaseTest, TestHelper):
    
    dbapi_module_names = ["MySQLdb"]
    db_module_name = "mysql"
