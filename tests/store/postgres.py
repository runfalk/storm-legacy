import os

from storm.databases.postgres import Postgres

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
        return bool(os.environ.get("STORM_POSTGRES_DBNAME"))

    def create_database(self):
        self.database = Postgres(os.environ["STORM_POSTGRES_DBNAME"])

    def create_tables(self):
        connection = self.database.connect()
        connection.execute("CREATE TABLE test "
                           "(id SERIAL PRIMARY KEY,"
                           " title VARCHAR DEFAULT 'Default Title')")
        connection.execute("CREATE TABLE other "
                           "(id SERIAL PRIMARY KEY,"
                           " test_id INTEGER,"
                           " other_title VARCHAR)")
        connection.commit()
