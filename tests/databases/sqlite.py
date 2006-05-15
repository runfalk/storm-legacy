from pysqlite2 import dbapi2 as sqlite

from storm.databases.sqlite import SQLite
from storm.database import *

from tests.helper import TestHelper, MakePath


class SQLiteMemoryTest(TestHelper):

    helpers = [MakePath]
    
    def setUp(self):
        TestHelper.setUp(self)
        self.setup_database()

    def setup_database(self):
        self.database = SQLite()

    def add_sample_data(self, connection):
        connection.execute("CREATE TABLE test (id INT PRIMARY KEY, title VARCHAR)")
        connection.execute("INSERT INTO test VALUES (1, 'Title 1')")
        connection.execute("INSERT INTO test VALUES (2, 'Title 2')")

    def test_create(self):
        self.assertTrue(isinstance(self.database, Database))
        
    def test_connection(self):
        connection = self.database.connect()
        self.assertTrue(isinstance(connection, Connection))

    def test_execute_result(self):
        connection = self.database.connect()
        result = connection.execute("SELECT 1")
        self.assertTrue(isinstance(result, Result))

    def test_execute_params(self):
        connection = self.database.connect()
        result = connection.execute("SELECT 1 WHERE 1=?", (1,))
        self.assertTrue(result.fetch_one())
        result = connection.execute("SELECT 1 WHERE 1=?", (2,))
        self.assertFalse(result.fetch_one())

    def test_fetch_one(self):
        connection = self.database.connect()
        self.add_sample_data(connection)
        result = connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEquals(result.fetch_one(), (1, "Title 1"))

    def test_fetch_all(self):
        connection = self.database.connect()
        self.add_sample_data(connection)
        result = connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEquals(result.fetch_all(), [(1, "Title 1"), (2, "Title 2")])

    def test_iter(self):
        connection = self.database.connect()
        self.add_sample_data(connection)
        result = connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEquals([item for item in result],
                          [(1, "Title 1"), (2, "Title 2")])

class SQLiteFileTest(SQLiteMemoryTest):
    
    def setup_database(self):
        self.database = SQLite(self.make_path())

    # Test iterating over two connections at the same time.

