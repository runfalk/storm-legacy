from storm.databases.sqlite import SQLite
from storm.properties import Int, Str
from storm.expr import Asc, Desc
from storm.store import Store

from tests.helper import TestHelper, MakePath


class Class(object):
    __table__ = "test", "id"

    id = Int()
    title = Str()


class StoreTest(TestHelper):

    helpers = [MakePath]

    def setUp(self):
        TestHelper.setUp(self)
        self.filename = self.make_path()
        self.database = SQLite(self.filename)
        self.store = Store(self.database)

        connection = self.database.connect()
        connection.execute("CREATE TABLE test "
                           "(id INT PRIMARY KEY, title VARCHAR)")
        connection.execute("INSERT INTO test VALUES (1, 'Title 9')")
        connection.execute("INSERT INTO test VALUES (2, 'Title 8')")
        connection.execute("INSERT INTO test VALUES (4, 'Title 7')")
        connection.commit()

    def test_get(self):
        obj = self.store.get(Class, 1)
        self.assertEquals(obj.id, 1)
        self.assertEquals(obj.title, "Title 9")

        obj = self.store.get(Class, 2)
        self.assertEquals(obj.id, 2)
        self.assertEquals(obj.title, "Title 8")

        obj = self.store.get(Class, 3)
        self.assertEquals(obj, None)

    def test_get_tuple(self):
        class Class(object):
            __table__ = "test", ("title", "id")
            id = Int()
            title = Str()
        obj = self.store.get(Class, ("Title 9", 1))
        self.assertEquals(obj.id, 1)
        self.assertEquals(obj.title, "Title 9")

        obj = self.store.get(Class, ("Title 8", 1))
        self.assertEquals(obj, None)

    def test_of(self):
        obj = self.store.get(Class, 1)
        self.assertEquals(Store.of(obj), self.store)

    def test_find_iter(self):
        result = self.store.find(Class)
        lst = [(obj.id, obj.title) for obj in result]
        lst.sort()
        self.assertEquals(lst, [(1, "Title 9"),
                                (2, "Title 8"),
                                (4, "Title 7")])

    def test_find_order_by(self, *args):
        result = self.store.find(Class).order_by(Class.title)
        lst = [(obj.id, obj.title) for obj in result]
        self.assertEquals(lst, [(4, "Title 7"),
                                (2, "Title 8"),
                                (1, "Title 9")])

    def test_find_order_asc(self, *args):
        result = self.store.find(Class).order_by(Asc(Class.title))
        lst = [(obj.id, obj.title) for obj in result]
        self.assertEquals(lst, [(4, "Title 7"),
                                (2, "Title 8"),
                                (1, "Title 9")])

    def test_find_order_desc(self, *args):
        result = self.store.find(Class).order_by(Desc(Class.title))
        lst = [(obj.id, obj.title) for obj in result]
        self.assertEquals(lst, [(1, "Title 9"),
                                (2, "Title 8"),
                                (4, "Title 7")])

    def test_find_one(self, *args):
        obj = self.store.find(Class).order_by(Class.title).one()
        self.assertEquals(obj.id, 4)
        self.assertEquals(obj.title, "Title 7")

        obj = self.store.find(Class).order_by(Class.id).one()
        self.assertEquals(obj.id, 1)
        self.assertEquals(obj.title, "Title 9")

    def test_find_count(self):
        self.assertEquals(self.store.find(Class).count(), 3)

    def test_find_max(self):
        self.assertEquals(self.store.find(Class).max(Class.id), 4)

    def test_find_min(self):
        self.assertEquals(self.store.find(Class).min(Class.id), 1)

    def test_find_avg(self):
        self.assertEquals(int(self.store.find(Class).avg(Class.id)), 2)

    def test_find_sum(self):
        self.assertEquals(int(self.store.find(Class).sum(Class.id)), 7)

#    def test_add(self):
#        obj = Class()        
#        obj.id = 3
#        obj.title = "Title 1"
#
#        self.store.add(obj)
#
#        connection = self.database.connect()
#        connection.execute("SELECT * FROM test DATABASE")
