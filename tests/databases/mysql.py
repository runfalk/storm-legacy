from datetime import datetime, date, time
import os

from tests.databases.base import DatabaseTest
from tests.helper import TestHelper

from storm.databases.mysql import MySQL


class MySQLTest(TestHelper, DatabaseTest):

    supports_microseconds = False

    def setUp(self):
        TestHelper.setUp(self)
        DatabaseTest.setUp(self)

    def tearDown(self):
        DatabaseTest.setUp(self)
        TestHelper.setUp(self)
    
    def is_supported(self):
        return bool(os.environ.get("STORM_MYSQL_DBNAME"))

    def create_database(self):
        self.database = MySQL(os.environ["STORM_MYSQL_DBNAME"])

    def create_tables(self):
        self.connection.execute("CREATE TABLE test "
                                "(id INT AUTO_INCREMENT PRIMARY KEY,"
                                " title VARCHAR(50))")
        self.connection.execute("CREATE TABLE datetime_test "
                                "(id INT AUTO_INCREMENT PRIMARY KEY,"
                                " dt TIMESTAMP, d DATE, t TIME)")
