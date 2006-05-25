from datetime import datetime, date, time

from storm.expr import Select, Column, Undef
from storm.kinds import DateTimeKind, DateKind, TimeKind
from storm.database import *


class DatabaseTest(object):

    supports_microseconds = True

    def setUp(self):
        self.create_database()
        self.create_connection()
        self.drop_tables()
        self.create_tables()
        self.create_sample_data()

    def tearDown(self):
        self.drop_sample_data()
        self.drop_tables()
        self.drop_database()

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
        result = self.connection.execute("SELECT 1 FROM (SELECT 1) AS ALIAS "
                                         "WHERE 1=?", (1,))
        self.assertTrue(result.fetch_one())
        result = self.connection.execute("SELECT 1 FROM (SELECT 1) AS ALIAS "
                                         "WHERE 1=?", (2,))
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


    def test_datetime(self):
        value = datetime(1977, 4, 5, 12, 34, 56, 78)
        self.connection.execute("INSERT INTO datetime_test (dt) VALUES (?)",
                                (value,))
        result = self.connection.execute("SELECT dt FROM datetime_test")
        kind = DateTimeKind()
        result_value = kind.from_database(result.fetch_one()[0])
        if not self.supports_microseconds:
            value = value.replace(microsecond=0)
        self.assertEquals(result_value, value)

    def test_date(self):
        value = date(1977, 4, 5)
        self.connection.execute("INSERT INTO datetime_test (d) VALUES (?)",
                                (value,))
        result = self.connection.execute("SELECT d FROM datetime_test")
        kind = DateKind()
        result_value = kind.from_database(result.fetch_one()[0])
        self.assertEquals(result_value, value)

    def test_time(self):
        value = time(12, 34, 56, 78)
        self.connection.execute("INSERT INTO datetime_test (t) VALUES (?)",
                                (value,))
        result = self.connection.execute("SELECT t FROM datetime_test")
        kind = TimeKind()
        result_value = kind.from_database(result.fetch_one()[0])
        if not self.supports_microseconds:
            value = value.replace(microsecond=0)
        self.assertEquals(result_value, value)
