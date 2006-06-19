from datetime import datetime, date, time
import os

from storm.databases.mysql import MySQL
from storm.database import create_database

from tests.databases.base import DatabaseTest, UnsupportedDatabaseTest
from tests.helper import TestHelper


class MySQLTest(TestHelper, DatabaseTest):

    supports_microseconds = False

    def setUp(self):
        TestHelper.setUp(self)
        DatabaseTest.setUp(self)

    def tearDown(self):
        DatabaseTest.setUp(self)
        TestHelper.setUp(self)
    
    def is_supported(self):
        return bool(os.environ.get("STORM_MYSQL_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_MYSQL_URI"])

    def create_tables(self):
        self.connection.execute("CREATE TABLE test "
                                "(id INT AUTO_INCREMENT PRIMARY KEY,"
                                " title VARCHAR(50))")
        self.connection.execute("CREATE TABLE datetime_test "
                                "(id INT AUTO_INCREMENT PRIMARY KEY,"
                                " dt TIMESTAMP, d DATE, t TIME)")
        self.connection.execute("CREATE TABLE bin_test "
                                "(id INT AUTO_INCREMENT PRIMARY KEY,"
                                " b BLOB)")

    def test_wb_create_database(self):
        database = create_database("mysql://un:pw@ht:12/db?unix_socket=us")
        self.assertTrue(isinstance(database, MySQL))
        for key, value in [("db", "db"), ("host", "ht"), ("port", 12),
                           ("user", "un"), ("passwd", "pw"),
                           ("unix_socket", "us")]:
            self.assertEquals(database._connect_kwargs.get(key), value)


class MySQLUnsupportedTest(UnsupportedDatabaseTest, TestHelper):
    
    dbapi_module_name = "MySQLdb"
    db_module_name = "mysql"
