import os
import gc

from storm.database import create_database
from storm.properties import Int, List

from tests.store.base import StoreTest, EmptyResultSetTest
from tests.helper import TestHelper

from tests.helper import run_this


class Lst1(object):
    __table__ = "lst1", "id"
    id = Int()
    ints = List(type=Int())

class Lst2(object):
    __table__ = "lst2", "id"
    id = Int()
    ints = List(type=List(type=Int()))


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
        connection.execute("CREATE TABLE foo "
                           "(id SERIAL PRIMARY KEY,"
                           " title VARCHAR DEFAULT 'Default Title')")
        connection.execute("CREATE TABLE bar "
                           "(id SERIAL PRIMARY KEY,"
                           " foo_id INTEGER, title VARCHAR)")
        connection.execute("CREATE TABLE bin "
                           "(id SERIAL PRIMARY KEY, bin BYTEA)")
        connection.execute("CREATE TABLE link "
                           "(foo_id INTEGER, bar_id INTEGER)")
        connection.execute("CREATE TABLE lst1 "
                           "(id SERIAL PRIMARY KEY, ints INTEGER[])")
        connection.execute("CREATE TABLE lst2 "
                           "(id SERIAL PRIMARY KEY, ints INTEGER[][])")
        connection.commit()

    def drop_tables(self):
        StoreTest.drop_tables(self)
        for table in ["lst1", "lst2"]:
            connection = self.database.connect()
            try:
                connection.execute("DROP TABLE %s" % table)
                connection.commit()
            except:
                connection.rollback()

    def test_list_variable(self):

        lst = Lst1()
        lst.id = 1
        lst.ints = [1,2,3,4]

        self.store.add(lst)

        result = self.store.execute("SELECT ints FROM lst1 WHERE id=1")
        self.assertEquals(result.get_one(), ("{1,2,3,4}",))

        del lst
        gc.collect()

        lst = self.store.find(Lst1, Lst1.ints == [1,2,3,4]).one()
        self.assertTrue(lst)

        lst.ints.append(5)

        result = self.store.execute("SELECT ints FROM lst1 WHERE id=1")
        self.assertEquals(result.get_one(), ("{1,2,3,4,5}",))

    def test_list_variable_nested(self):

        lst = Lst2()
        lst.id = 1
        lst.ints = [[1, 2], [3, 4]]

        self.store.add(lst)

        result = self.store.execute("SELECT ints FROM lst2 WHERE id=1")
        self.assertEquals(result.get_one(), ("{{1,2},{3,4}}",))

        del lst
        gc.collect()

        lst = self.store.find(Lst2, Lst2.ints == [[1,2],[3,4]]).one()
        self.assertTrue(lst)

        lst.ints.append([5, 6])

        result = self.store.execute("SELECT ints FROM lst2 WHERE id=1")
        self.assertEquals(result.get_one(), ("{{1,2},{3,4},{5,6}}",))


class PostgresEmptyResultSetTest(TestHelper, EmptyResultSetTest):

    def setUp(self):
        TestHelper.setUp(self)
        EmptyResultSetTest.setUp(self)

    def tearDown(self):
        TestHelper.tearDown(self)
        EmptyResultSetTest.tearDown(self)

    def is_supported(self):
        return bool(os.environ.get("STORM_POSTGRES_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_POSTGRES_URI"])

    def create_tables(self):
        connection = self.database.connect()
        connection.execute("CREATE TABLE foo "
                           "(id SERIAL PRIMARY KEY,"
                           " title VARCHAR DEFAULT 'Default Title')")
        connection.commit()
