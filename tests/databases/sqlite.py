from pysqlite2 import dbapi2 as sqlite

from storm.databases.sqlite import SQLite
from storm.expr import Select, Column, Undef
from storm.database import *

from tests.helper import TestHelper, MakePath


class SQLiteMemoryTest(TestHelper):

    helpers = [MakePath]
    
    def setUp(self):
        TestHelper.setUp(self)
        self.setup_database()
        self.connection = self.database.connect()

    def setup_database(self):
        self.database = SQLite()

    def add_sample_data(self):
        self.connection.execute("CREATE TABLE test "
                                "(id INTEGER PRIMARY KEY, title VARCHAR)")
        self.connection.execute("INSERT INTO test VALUES (1, 'Title 1')")
        self.connection.execute("INSERT INTO test VALUES (2, 'Title 2')")

    def test_create(self):
        self.assertTrue(isinstance(self.database, Database))
        
    def test_connection(self):
        self.assertTrue(isinstance(self.connection, Connection))

    def test_execute_result(self):
        result = self.connection.execute("SELECT 1")
        self.assertTrue(isinstance(result, Result))

    def test_execute_params(self):
        result = self.connection.execute("SELECT 1 WHERE 1=?", (1,))
        self.assertTrue(result.fetch_one())
        result = self.connection.execute("SELECT 1 WHERE 1=?", (2,))
        self.assertFalse(result.fetch_one())

    def test_fetch_one(self):
        self.add_sample_data()
        result = self.connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEquals(result.fetch_one(), (1, "Title 1"))

    def test_fetch_all(self):
        self.add_sample_data()
        result = self.connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEquals(result.fetch_all(), [(1, "Title 1"), (2, "Title 2")])

    def test_iter(self):
        self.add_sample_data()
        result = self.connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEquals([item for item in result],
                          [(1, "Title 1"), (2, "Title 2")])

    def test_get_insert_identity(self):
        self.add_sample_data()
        result = self.connection.execute("INSERT INTO test (title) "
                                         "VALUES ('Title 3')")
        expr = result.get_insert_identity((Column("id", "test"),), (Undef,))
        result = self.connection.execute(Select(Column("title", "test"), expr))
        self.assertEquals(result.fetch_one(), ("Title 3",))


class SQLiteFileTest(SQLiteMemoryTest):
    
    def setup_database(self):
        self.database = SQLite(self.make_path())

    def test_simultaneous_iter(self):
        self.add_sample_data()
        result1 = self.connection.execute("SELECT * FROM test "
                                          "ORDER BY id ASC")
        result2 = self.connection.execute("SELECT * FROM test "
                                          "ORDER BY id DESC")
        iter1 = iter(result1)
        iter2 = iter(result2)
        self.assertEquals(iter1.next(), (1, "Title 1"))
        self.assertEquals(iter2.next(), (2, "Title 2"))
        self.assertEquals(iter1.next(), (2, "Title 2"))
        self.assertEquals(iter2.next(), (1, "Title 1"))
        self.assertRaises(StopIteration, iter1.next)
        self.assertRaises(StopIteration, iter2.next)

