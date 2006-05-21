from pysqlite2 import dbapi2 as sqlite

from storm.databases.sqlite import SQLite
from storm.expr import Select, Column, Undef
from storm.database import *

from tests.helper import TestHelper, MakePath


class SQLiteMemoryTest(TestHelper):

    helpers = [MakePath]
    
    def setUp(self):
        TestHelper.setUp(self)
        self.create_sample_data()

    def tearDown(self):
        TestHelper.tearDown(self)
        self.drop_sample_data()

    def create_database(self):
        self.database = SQLite()

    def create_sample_data(self):
        self.create_database()
        self.connection = self.database.connect()
        self.connection.execute("CREATE TABLE test "
                                "(id INTEGER PRIMARY KEY, title VARCHAR)")
        self.connection.execute("INSERT INTO test VALUES (10, 'Title 10')")
        self.connection.execute("INSERT INTO test VALUES (20, 'Title 20')")

    def drop_sample_data(self):
        self.connection.execute("DROP TABLE test")
        self.connection.commit()

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
        result = self.connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEquals(result.fetch_one(), (10, "Title 10"))

    def test_fetch_all(self):
        result = self.connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEquals(result.fetch_all(),
                          [(10, "Title 10"), (20, "Title 20")])

    def test_iter(self):
        result = self.connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEquals([item for item in result],
                          [(10, "Title 10"), (20, "Title 20")])

    def test_get_insert_identity(self):
        result = self.connection.execute("INSERT INTO test (title) "
                                         "VALUES ('Title 30')")
        expr = result.get_insert_identity((Column("id", "test"),), (Undef,))
        result = self.connection.execute(Select(Column("title", "test"), expr))
        self.assertEquals(result.fetch_one(), ("Title 30",))


class SQLiteFileTest(SQLiteMemoryTest):
    
    def setup_database(self):
        self.database = SQLite(self.make_path())

    def test_simultaneous_iter(self):
        result1 = self.connection.execute("SELECT * FROM test "
                                          "ORDER BY id ASC")
        result2 = self.connection.execute("SELECT * FROM test "
                                          "ORDER BY id DESC")
        iter1 = iter(result1)
        iter2 = iter(result2)
        self.assertEquals(iter1.next(), (10, "Title 10"))
        self.assertEquals(iter2.next(), (20, "Title 20"))
        self.assertEquals(iter1.next(), (20, "Title 20"))
        self.assertEquals(iter2.next(), (10, "Title 10"))
        self.assertRaises(StopIteration, iter1.next)
        self.assertRaises(StopIteration, iter2.next)
