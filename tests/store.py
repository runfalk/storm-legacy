import gc

from storm.databases.sqlite import SQLite
from storm.properties import ObjectInfo
from storm.database import Result
from storm.properties import Int, Str
from storm.expr import Asc, Desc, Select
from storm.store import *

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

    def get_items(self):
        # Bypass the store to avoid flushing.
        connection = self.store._connection
        result = connection.execute("SELECT * FROM test DATABASE ORDER BY id")
        return list(result)

    def get_committed_items(self):
        connection = self.database.connect()
        result = connection.execute("SELECT * FROM test DATABASE ORDER BY id")
        return list(result)

    def test_execute(self):
        result = self.store.execute("SELECT 1")
        self.assertTrue(isinstance(result, Result))
        self.assertEquals(result.fetch_one(), (1,))
        
        result = self.store.execute("SELECT 1", noresult=True)
        self.assertEquals(result, None)

    def test_execute_params(self):
        result = self.store.execute("SELECT ?", [1])
        self.assertTrue(isinstance(result, Result))
        self.assertEquals(result.fetch_one(), (1,))

    def test_execute_flushes(self):
        obj = self.store.get(Class, 1)
        obj.title = "New Title"
        
        result = self.store.execute("SELECT title FROM test WHERE id=1")
        self.assertEquals(result.fetch_one(), ("New Title",))

    def test_get(self):
        obj = self.store.get(Class, 1)
        self.assertEquals(obj.id, 1)
        self.assertEquals(obj.title, "Title 9")

        obj = self.store.get(Class, 2)
        self.assertEquals(obj.id, 2)
        self.assertEquals(obj.title, "Title 8")

        obj = self.store.get(Class, 3)
        self.assertEquals(obj, None)

    def test_get_from_cache(self):
        obj = self.store.get(Class, 1)
        self.assertTrue(self.store.get(Class, 1) is obj)

    def test_cache_cleanup(self):
        obj = self.store.get(Class, 1)
        obj.taint = True

        del obj
        gc.collect()

        obj = self.store.get(Class, 1)
        self.assertFalse(getattr(obj, "taint", False))

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
        self.assertEquals(Store.of(Class()), None)
        self.assertEquals(Store.of(object()), None)

    def test_find_iter(self):
        result = self.store.find(Class)

        lst = [(obj.id, obj.title) for obj in result]
        lst.sort()
        self.assertEquals(lst, [(1, "Title 9"),
                                (2, "Title 8"),
                                (4, "Title 7")])

    def test_find_from_cache(self):
        obj = self.store.get(Class, 1)
        self.assertTrue(self.store.find(Class, id=1).one() is obj)

    def test_find_expr(self):
        result = self.store.find(Class, Class.id == 2,
                                 Class.title == "Title 8")
        self.assertEquals([(obj.id, obj.title) for obj in result],
                          [(2, "Title 8")])

        result = self.store.find(Class, Class.id == 1,
                                 Class.title == "Title 8")
        self.assertEquals([(obj.id, obj.title) for obj in result], [])

    def test_find_keywords(self):
        result = self.store.find(Class, id=2, title="Title 8")
        self.assertEquals([(obj.id, obj.title) for obj in result],
                          [(2, "Title 8")])

        result = self.store.find(Class, id=1, title="Title 8")
        self.assertEquals([(obj.id, obj.title) for obj in result], [])

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

        obj = self.store.find(Class, id=3).one()
        self.assertEquals(obj, None)

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

    def test_find_remove(self, *args):
        self.store.find(Class, Class.id == 2).remove()
        result = self.store.find(Class)
        lst = [(obj.id, obj.title) for obj in result]
        lst.sort()
        self.assertEquals(lst, [(1, "Title 9"),
                                (4, "Title 7")])

    def test_add_commit(self):
        obj = Class()
        obj.id = 3
        obj.title = "Title 6"

        self.store.add(obj)

        self.assertEquals(self.get_committed_items(),
                          [(1, "Title 9"), (2, "Title 8"), (4, "Title 7")])

        self.store.commit()

        self.assertEquals(self.get_committed_items(),
                          [(1, "Title 9"), (2, "Title 8"), (3, "Title 6"),
                           (4, "Title 7")])

    def test_add_rollback_commit(self):
        obj = Class()
        obj.id = 3
        obj.title = "Title 6"

        self.store.add(obj)
        self.store.rollback()

        self.assertEquals(self.store.get(Class, 3), None)

        self.assertEquals(self.get_items(),
                          [(1, "Title 9"), (2, "Title 8"), (4, "Title 7")])
        
        self.store.commit()

        self.assertEquals(self.get_committed_items(),
                          [(1, "Title 9"), (2, "Title 8"), (4, "Title 7")])

    def test_add_get(self):
        obj = Class()
        obj.id = 3
        obj.title = "Title 6"

        self.store.add(obj)

        get_obj = self.store.get(Class, 3)

        self.assertEquals(get_obj.id, 3)
        self.assertEquals(get_obj.title, "Title 6")

        self.assertTrue(get_obj is obj)

    def test_add_find(self):
        obj = Class()
        obj.id = 3
        obj.title = "Title 6"

        self.store.add(obj)

        get_obj = self.store.find(Class, Class.id == 3).one()

        self.assertEquals(get_obj.id, 3)
        self.assertEquals(get_obj.title, "Title 6")

        self.assertTrue(get_obj is obj)

    def test_add_twice(self):
        obj = Class()
        obj.id = 3
        obj.title = "Title 3"

        self.store.add(obj)
        self.store.add(obj)

    def test_add_twice_to_wrong_store(self):
        obj = Class()
        obj.id = 3
        obj.title = "Title 3"

        self.store.add(obj)
        
        self.assertRaises(StoreError, Store(self.database).add, obj)

    def test_remove_commit(self):
        obj = self.store.get(Class, 2)

        self.store.remove(obj)

        self.assertEquals(Store.of(obj), self.store)

        self.store.flush()
        
        self.assertEquals(Store.of(obj), None)

        self.assertEquals(self.get_items(),
                          [(1, "Title 9"), (4, "Title 7")])
        self.assertEquals(self.get_committed_items(),
                          [(1, "Title 9"), (2, "Title 8"), (4, "Title 7")])

        self.store.commit()

        self.assertEquals(self.get_committed_items(),
                          [(1, "Title 9"), (4, "Title 7")])

    def test_remove_rollback_update(self):
        obj = self.store.get(Class, 2)

        self.store.remove(obj)
        self.store.rollback()
        
        obj.title = "Title 2"

        self.store.flush()

        self.assertEquals(self.get_items(),
                          [(1, "Title 9"), (2, "Title 2"), (4, "Title 7")])

    def test_remove_add_update(self):
        obj = self.store.get(Class, 2)

        self.store.remove(obj)
        self.store.add(obj)
        
        obj.title = "Title 2"

        self.store.flush()

        self.assertEquals(self.get_items(),
                          [(1, "Title 9"), (2, "Title 2"), (4, "Title 7")])

    def test_remove_flush_add_update(self):
        obj = self.store.get(Class, 2)

        self.store.remove(obj)
        self.store.flush()
        self.store.add(obj)
        
        obj.title = "Title 2"

        self.store.flush()

        self.assertEquals(self.get_items(),
                          [(1, "Title 9"), (2, "Title 2"), (4, "Title 7")])

    def test_remove_from_wrong_store(self):
        obj = self.store.get(Class, 1)
        self.assertRaises(StoreError, Store(self.database).remove, obj)

    def test_wb_remove_flush_update_is_dirty(self):
        obj = self.store.get(Class, 2)

        self.store.remove(obj)
        self.store.flush()
        
        obj.title = "Title 2"

        self.assertFalse(id(obj) in self.store._dirty)

    def test_update_flush_commit(self):
        obj = self.store.get(Class, 2)

        obj.title = "Title 2"

        self.assertEquals(self.get_items(),
                          [(1, "Title 9"), (2, "Title 8"), (4, "Title 7")])
        self.assertEquals(self.get_committed_items(),
                          [(1, "Title 9"), (2, "Title 8"), (4, "Title 7")])

        self.store.flush()

        self.assertEquals(self.get_items(),
                          [(1, "Title 9"), (2, "Title 2"), (4, "Title 7")])
        self.assertEquals(self.get_committed_items(),
                          [(1, "Title 9"), (2, "Title 8"), (4, "Title 7")])

        self.store.commit()

        self.assertEquals(self.get_committed_items(),
                          [(1, "Title 9"), (2, "Title 2"), (4, "Title 7")])

    def test_update_commit(self):

        obj = self.store.get(Class, 2)

        obj.title = "Title 2"

        self.assertEquals(self.get_items(),
                          [(1, "Title 9"), (2, "Title 8"), (4, "Title 7")])
        self.assertEquals(self.get_committed_items(),
                          [(1, "Title 9"), (2, "Title 8"), (4, "Title 7")])

        # Does it work without flush?

        self.store.commit()

        self.assertEquals(self.get_committed_items(),
                          [(1, "Title 9"), (2, "Title 2"), (4, "Title 7")])

    def test_update_primary_key(self):
        obj = self.store.get(Class, 2)

        obj.id = 8

        self.store.commit()

        self.assertEquals(self.get_committed_items(),
                          [(1, "Title 9"), (4, "Title 7"), (8, "Title 8")])

        # Update twice to see if the notion of primary key for the
        # existent object was updated as well.
        
        obj.id = 18

        self.store.commit()

        self.assertEquals(self.get_committed_items(),
                          [(1, "Title 9"), (4, "Title 7"), (18, "Title 8")])

        self.assertTrue(self.store.get(Class, 18) is obj)
        self.assertTrue(self.store.get(Class, 2) is None)
        self.assertTrue(self.store.get(Class, 8) is None)

    def test_update_primary_key_exchange(self):
        obj1 = self.store.get(Class, 1)
        obj2 = self.store.get(Class, 2)

        obj1.id = 3
        self.store.flush()
        obj2.id = 1
        self.store.flush()
        obj1.id = 2

        self.assertTrue(self.store.get(Class, 2) is obj1)
        self.assertTrue(self.store.get(Class, 1) is obj2)

        self.store.commit()

        self.assertEquals(self.get_committed_items(),
                          [(1, "Title 8"), (2, "Title 9"), (4, "Title 7")])

    def test_update_flush_flush(self):
        obj = self.store.get(Class, 2)

        obj.title = "Title 2"

        self.store.flush()

        # If changes get committed even with the notification disabled,
        # it means the dirty flag isn't being cleared.

        ObjectInfo(obj).set_change_notification(None)

        obj.title = "Title 12"

        self.store.flush()

        self.assertEquals(self.get_items(),
                          [(1, "Title 9"), (2, "Title 2"), (4, "Title 7")])

    def test_update_find(self):
        obj = self.store.get(Class, 2)
        obj.title = "Title 2"
        self.assertTrue(self.store.find(Class, Class.title == "Title 2").one())

    def test_update_get(self):
        obj = self.store.get(Class, 2)
        obj.id = 3
        self.assertTrue(self.store.get(Class, 3))

    def test_add_update(self):
        obj = Class()
        obj.id = 3
        obj.title = "Title 6"

        self.store.add(obj)

        obj.title = "Title 3"

        self.store.flush()

        self.assertEquals(self.get_items(),
                          [(1, "Title 9"), (2, "Title 8"), (3, "Title 3"),
                           (4, "Title 7")])

    def test_add_remove_add(self):
        obj = Class()
        obj.id = 3
        obj.title = "Title 6"

        self.store.add(obj)

        self.store.remove(obj)

        self.assertEquals(Store.of(obj), None)

        obj.title = "Title 3"

        self.store.add(obj)

        obj.id = 6

        self.store.flush()

        self.assertEquals(self.get_items(),
                          [(1, "Title 9"), (2, "Title 8"), (4, "Title 7"),
                           (6, "Title 3")])

        self.assertTrue(self.store.get(Class, 6) is obj)

    def test_wb_add_remove_add(self):
        obj = Class()
        obj.id = 3
        obj.title = "Title 6"

        self.store.add(obj)

        self.assertTrue(id(obj) in self.store._dirty)
        self.assertTrue(Store.of(obj) is self.store)

        self.store.remove(obj)

        self.assertTrue(id(obj) not in self.store._dirty)
        self.assertTrue(Store.of(obj) is None)

        self.store.add(obj)

        self.assertTrue(id(obj) in self.store._dirty)
        self.assertTrue(Store.of(obj) is self.store)

    def test_wb_update_remove_add(self):
        obj = self.store.get(Class, 1)
        obj.title = "Title 1"

        self.store.remove(obj)
        self.store.add(obj)

        self.assertTrue(id(obj) in self.store._dirty)

    def test_sub_class(self):
        class SubClass(Class):
            title = Int()

        obj = self.store.get(Class, 1)
        obj.title = 9

        sup_obj = self.store.get(Class, 1)
        sub_obj = self.store.get(SubClass, 1)

        self.assertEquals(sup_obj.id, 1)
        self.assertEquals(sup_obj.title, "9")
        self.assertEquals(sub_obj.id, 1)
        self.assertEquals(sub_obj.title, 9)

    def test_join(self):
        self.store.execute("CREATE TABLE other "
                           "(id INT PRIMARY KEY, other_title VARCHAR)")

        class Other(object):
            __table__ = "other", "id"
            id = Int()
            other_title = Str()

        other = Other()
        other.id = 3
        other.other_title = "Title 9"

        self.store.add(other)

        # Add another object with the same title to ensure DISTINCT
        # is in place.
        other = Other()
        other.id = 4
        other.other_title = "Title 9"

        self.store.add(other)

        result = self.store.find(Class, Class.title == Other.other_title)

        self.assertEquals([(obj.id, obj.title) for obj in result],
                          [(1, "Title 9")])

    def test_sub_select(self):
        obj = self.store.find(Class, Class.id == Select("1")).one()
        self.assertTrue(obj)
        self.assertEquals(obj.id, 1)
        self.assertEquals(obj.title, "Title 9")

