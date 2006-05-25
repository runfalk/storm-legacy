from storm.expr import Select, Column, Undef
from storm.database import *

from tests.helper import TestHelper


class DatabaseTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        self.create_database()
        self.create_connection()
        self.drop_tables()
        self.create_tables()
        self.create_sample_data()

    def tearDown(self):
        self.drop_sample_data()
        self.drop_tables()
        self.drop_database()
        TestHelper.tearDown(self)

    def is_supported(self):
        return self.__class__ is not DatabaseTest

    def create_database(self):
        raise NotImplementedError

    def create_connection(self):
        self.connection = self.database.connect()

    def create_tables(self):
        raise NotImplementedError

    def create_sample_data(self):
        self.connection.execute("INSERT INTO test VALUES (10, 'Title 10')")
        self.connection.execute("INSERT INTO test VALUES (20, 'Title 20')")
        self.connection.commit()

    def drop_sample_data(self):
        pass

    def drop_tables(self):
        try:
            self.connection.execute("DROP TABLE test")
            self.connection.commit()
        except:
            self.connection.rollback()
        try:
            self.connection.execute("DROP TABLE datetime_test")
            self.connection.commit()
        except:
            self.connection.rollback()

    def drop_database(self):
        pass

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

    def test_get_insert_identity(self):
        result = self.connection.execute("INSERT INTO test (title) "
                                         "VALUES ('Title 30')")
        primary_key = (Column("id", "test"),)
        primary_values = (Undef,)
        expr = result.get_insert_identity(primary_key, primary_values)
        result = self.connection.execute(Select(Column("title", "test"), expr))
        self.assertEquals(result.fetch_one(), ("Title 30",))

    def test_get_insert_identity_composed(self):
        result = self.connection.execute("INSERT INTO test (title) "
                                         "VALUES ('Title 30')")
        primary_key = (Column("id", "test"), Column("title", "test"))
        primary_values = (Undef, "Title 30")
        expr = result.get_insert_identity(primary_key, primary_values)
        result = self.connection.execute(Select(Column("title", "test"), expr))
        self.assertEquals(result.fetch_one(), ("Title 30",))
