from storm.databases.sqlite import SQLite

from tests.store.base import StoreTest
from tests.helper import TestHelper, MakePath


class SQLiteStoreTest(TestHelper, StoreTest):

    helpers = [MakePath]

    def setUp(self):
        TestHelper.setUp(self)
        StoreTest.setUp(self)

    def tearDown(self):
        TestHelper.tearDown(self)
        StoreTest.tearDown(self)

    def create_database(self):
        self.database = SQLite(self.make_path())

    def create_tables(self):
        connection = self.database.connect()
        connection.execute("CREATE TABLE test "
                           "(id INTEGER PRIMARY KEY,"
                           " title VARCHAR DEFAULT 'Default Title')")
        connection.execute("CREATE TABLE other "
                           "(id INTEGER PRIMARY KEY,"
                           " test_id INTEGER,"
                           " other_title VARCHAR)")
        connection.execute("CREATE TABLE link "
                           "(test_id INTEGER, other_id INTEGER)")
        connection.commit()

    def drop_tables(self):
        pass
