import os

from storm.database import create_database

from tests.store.base import StoreTest
from tests.helper import TestHelper


class PostgresStoreTest(TestHelper, StoreTest):

    def setUp(self):
        TestHelper.setUp(self)
        StoreTest.setUp(self)

    def tearDown(self):
        TestHelper.tearDown(self)
        StoreTest.tearDown(self)

    def is_supported(self):
        return bool(os.environ.get("STORM_POSTGRES_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_POSTGRES_URI"])

    def create_tables(self):
        connection = self.database.connect()
        connection.execute("CREATE TABLE test "
                           "(id SERIAL PRIMARY KEY,"
                           " title VARCHAR DEFAULT 'Default Title')")
        connection.execute("CREATE TABLE other "
                           "(id SERIAL PRIMARY KEY,"
                           " test_id INTEGER,"
                           " other_title VARCHAR)")
        connection.execute("CREATE TABLE bin "
                           "(id SERIAL PRIMARY KEY, bin BYTEA)")
        connection.execute("CREATE TABLE link "
                           "(test_id INTEGER, other_id INTEGER)")
        connection.commit()
