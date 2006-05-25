from datetime import datetime, date, time
import os

from tests.databases.base import DatabaseTest

from storm.databases.postgres import Postgres


class PostgresTest(DatabaseTest):
    
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

    def test_datetime(self):
        value = datetime(1977, 4, 5, 12, 34, 56, 78)
        self.connection.execute("INSERT INTO datetime_test (dt) VALUES (?)",
                                (value,))
        result = self.connection.execute("SELECT dt FROM datetime_test")
        self.assertEquals(result.fetch_one()[0], value)

    def test_date(self):
        value = date(1977, 4, 5)
        self.connection.execute("INSERT INTO datetime_test (d) VALUES (?)",
                                (value,))
        result = self.connection.execute("SELECT d FROM datetime_test")
        self.assertEquals(result.fetch_one()[0], value)

    def test_time(self):
        value = time(12, 34, 56, 78)
        self.connection.execute("INSERT INTO datetime_test (t) VALUES (?)",
                                (value,))
        result = self.connection.execute("SELECT t FROM datetime_test")
        self.assertEquals(result.fetch_one()[0], value)

    def test_convert_datetime_none(self):
        value = time(12, 34, 56, 78)
        self.connection.execute("INSERT INTO datetime_test (dt) VALUES (NULL)")
        result = self.connection.execute("SELECT dt FROM datetime_test")
        self.assertEquals(result.fetch_one()[0], None)

del DatabaseTest
