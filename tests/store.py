import gc
import os

from storm.databases.sqlite import SQLite
from storm.databases.postgres import Postgres

from storm.properties import get_obj_info
from storm.references import Reference
from storm.database import Result
from storm.properties import Int, Str
from storm.expr import Asc, Desc, Select
from storm.store import *

from tests.helper import *


class Test(object):
    __table__ = "test", "id"

    id = Int()
    title = Str()

class Other(object):
    __table__ = "other", "id"
    id = Int()
    test_id = Int()
    other_title = Str()
    test = Reference(Test, test_id)


class StoreTest(TestHelper):

    helpers = [MakePath]

    def setUp(self):
        TestHelper.setUp(self)
        self.create_database()
        self.drop_tables()
        self.create_tables()
        self.create_sample_data()
        self.create_store()

    def tearDown(self):
        self.drop_store()
        self.drop_sample_data()
        self.drop_tables()
        self.drop_database()
        TestHelper.tearDown(self)

    def create_database(self):
        raise NotImplementedError

    def create_tables(self):
        raise NotImplementedError

    def create_sample_data(self):
        connection = self.database.connect()
        connection.execute("INSERT INTO test VALUES (10, 'Title 30')")
        connection.execute("INSERT INTO test VALUES (20, 'Title 20')")
        connection.execute("INSERT INTO test VALUES (30, 'Title 10')")
        connection.execute("INSERT INTO other VALUES (100, 10, 'Title 300')")
        connection.execute("INSERT INTO other VALUES (200, 20, 'Title 200')")
        connection.execute("INSERT INTO other VALUES (300, 30, 'Title 100')")
        connection.commit()

    def create_store(self):
        self.store = Store(self.database)

    def drop_store(self):
        self.store.rollback()

    def drop_sample_data(self):
        pass

    def drop_tables(self):
        connection = self.database.connect()
        try:
            connection.execute("DROP TABLE test")
            connection.execute("DROP TABLE other")
            connection.commit()
        except:
            connection.rollback()

    def drop_database(self):
        pass

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
        obj = self.store.get(Test, 10)
        obj.title = "New Title"

        result = self.store.execute("SELECT title FROM test WHERE id=10")
        self.assertEquals(result.fetch_one(), ("New Title",))

    def test_get(self):
        obj = self.store.get(Test, 10)
        self.assertEquals(obj.id, 10)
        self.assertEquals(obj.title, "Title 30")

        obj = self.store.get(Test, 20)
        self.assertEquals(obj.id, 20)
        self.assertEquals(obj.title, "Title 20")

        obj = self.store.get(Test, 40)
        self.assertEquals(obj, None)

    def test_get_cached(self):
        obj = self.store.get(Test, 10)
        self.assertTrue(self.store.get(Test, 10) is obj)

    def test_wb_get_cached_doesnt_need_connection(self):
        obj = self.store.get(Test, 10)
        connection = self.store._connection
        self.store._connection = None
        self.store.get(Test, 10)
        self.store._connection = connection

    def test_cache_cleanup(self):
        obj = self.store.get(Test, 10)
        obj.taint = True

        del obj
        gc.collect()

        obj = self.store.get(Test, 10)
        self.assertFalse(getattr(obj, "taint", False))

    def test_get_tuple(self):
        class Test(object):
            __table__ = "test", ("title", "id")
            id = Int()
            title = Str()
        obj = self.store.get(Test, ("Title 30", 10))
        self.assertEquals(obj.id, 10)
        self.assertEquals(obj.title, "Title 30")

        obj = self.store.get(Test, ("Title 20", 10))
        self.assertEquals(obj, None)

    def test_of(self):
        obj = self.store.get(Test, 10)
        self.assertEquals(Store.of(obj), self.store)
        self.assertEquals(Store.of(Test()), None)
        self.assertEquals(Store.of(object()), None)

    def test_find_iter(self):
        result = self.store.find(Test)

        lst = [(obj.id, obj.title) for obj in result]
        lst.sort()
        self.assertEquals(lst, [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_find_from_cache(self):
        obj = self.store.get(Test, 10)
        self.assertTrue(self.store.find(Test, id=10).one() is obj)

    def test_find_expr(self):
        result = self.store.find(Test, Test.id == 20,
                                 Test.title == "Title 20")
        self.assertEquals([(obj.id, obj.title) for obj in result], [
                          (20, "Title 20"),
                         ])

        result = self.store.find(Test, Test.id == 10,
                                 Test.title == "Title 20")
        self.assertEquals([(obj.id, obj.title) for obj in result], [
                         ])

    def test_find_keywords(self):
        result = self.store.find(Test, id=20, title="Title 20")
        self.assertEquals([(obj.id, obj.title) for obj in result], [
                          (20, "Title 20")
                         ])

        result = self.store.find(Test, id=10, title="Title 20")
        self.assertEquals([(obj.id, obj.title) for obj in result], [
                         ])

    def test_find_order_by(self, *args):
        result = self.store.find(Test).order_by(Test.title)
        lst = [(obj.id, obj.title) for obj in result]
        self.assertEquals(lst, [
                          (30, "Title 10"),
                          (20, "Title 20"),
                          (10, "Title 30"),
                         ])

    def test_find_order_asc(self, *args):
        result = self.store.find(Test).order_by(Asc(Test.title))
        lst = [(obj.id, obj.title) for obj in result]
        self.assertEquals(lst, [
                          (30, "Title 10"),
                          (20, "Title 20"),
                          (10, "Title 30"),
                         ])

    def test_find_order_desc(self, *args):
        result = self.store.find(Test).order_by(Desc(Test.title))
        lst = [(obj.id, obj.title) for obj in result]
        self.assertEquals(lst, [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_find_one(self, *args):
        obj = self.store.find(Test).order_by(Test.title).one()
        self.assertEquals(obj.id, 30)
        self.assertEquals(obj.title, "Title 10")

        obj = self.store.find(Test).order_by(Test.id).one()
        self.assertEquals(obj.id, 10)
        self.assertEquals(obj.title, "Title 30")

        obj = self.store.find(Test, id=40).one()
        self.assertEquals(obj, None)

    def test_find_count(self):
        self.assertEquals(self.store.find(Test).count(), 3)

    def test_find_max(self):
        self.assertEquals(self.store.find(Test).max(Test.id), 30)

    def test_find_min(self):
        self.assertEquals(self.store.find(Test).min(Test.id), 10)

    def test_find_avg(self):
        self.assertEquals(int(self.store.find(Test).avg(Test.id)), 20)

    def test_find_sum(self):
        self.assertEquals(int(self.store.find(Test).sum(Test.id)), 60)

    def test_find_remove(self, *args):
        self.store.find(Test, Test.id == 20).remove()
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (30, "Title 10"),
                         ])

    def test_add_commit(self):
        obj = Test()
        obj.id = 40
        obj.title = "Title 40"

        self.store.add(obj)

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                          (40, "Title 40"),
                         ])

    def test_add_rollback_commit(self):
        obj = Test()
        obj.id = 40
        obj.title = "Title 40"

        self.store.add(obj)
        self.store.rollback()

        self.assertEquals(self.store.get(Test, 3), None)

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])
        
        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_add_get(self):
        obj = Test()
        obj.id = 40
        obj.title = "Title 40"

        self.store.add(obj)

        old_obj = obj

        obj = self.store.get(Test, 40)

        self.assertEquals(obj.id, 40)
        self.assertEquals(obj.title, "Title 40")

        self.assertTrue(obj is old_obj)

    def test_add_find(self):
        obj = Test()
        obj.id = 40
        obj.title = "Title 40"

        self.store.add(obj)

        old_obj = obj

        obj = self.store.find(Test, Test.id == 40).one()

        self.assertEquals(obj.id, 40)
        self.assertEquals(obj.title, "Title 40")

        self.assertTrue(obj is old_obj)

    def test_add_twice(self):
        obj = Test()
        self.store.add(obj)
        self.assertRaises(StoreError, self.store.add, obj)

    def test_add_loaded(self):
        obj = self.store.get(Test, 10)
        self.assertRaises(StoreError, self.store.add, obj)

    def test_add_twice_to_wrong_store(self):
        obj = Test()
        self.store.add(obj)
        self.assertRaises(StoreError, Store(self.database).add, obj)

    def test_remove_commit(self):
        obj = self.store.get(Test, 20)
        self.store.remove(obj)
        self.assertEquals(Store.of(obj), self.store)
        self.store.flush()
        self.assertEquals(Store.of(obj), self.store)

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (30, "Title 10"),
                         ])
        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.store.commit()
        self.assertEquals(Store.of(obj), None)

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (30, "Title 10"),
                         ])

    def test_remove_rollback_update(self):
        obj = self.store.get(Test, 20)

        self.store.remove(obj)
        self.store.rollback()
        
        obj.title = "Title 200"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_remove_flush_rollback_update(self):
        obj = self.store.get(Test, 20)

        self.store.remove(obj)
        self.store.flush()
        self.store.rollback()

        obj.title = "Title 200"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_remove_add_update(self):
        obj = self.store.get(Test, 20)

        self.store.remove(obj)
        self.store.add(obj)
        
        obj.title = "Title 200"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_remove_flush_add_update(self):
        obj = self.store.get(Test, 20)

        self.store.remove(obj)
        self.store.flush()
        self.store.add(obj)
        
        obj.title = "Title 200"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_remove_twice(self):
        obj = self.store.get(Test, 10)
        self.store.remove(obj)
        self.assertRaises(StoreError, self.store.remove, obj)

    def test_remove_unknown(self):
        obj = Test()
        self.assertRaises(StoreError, self.store.remove, obj)

    def test_remove_from_wrong_store(self):
        obj = self.store.get(Test, 20)
        self.assertRaises(StoreError, Store(self.database).remove, obj)

    def test_wb_remove_flush_update_isnt_dirty(self):
        obj = self.store.get(Test, 20)
        self.store.remove(obj)
        self.store.flush()
        
        obj.title = "Title 200"

        self.assertTrue(id(obj) not in self.store._dirty)

    def test_wb_remove_rollback_isnt_dirty_or_ghost(self):
        obj = self.store.get(Test, 20)
        self.store.remove(obj)
        self.store.rollback()

        self.assertTrue(id(obj) not in self.store._dirty)
        self.assertTrue(id(obj) not in self.store._ghosts)

    def test_wb_remove_flush_rollback_isnt_dirty_or_ghost(self):
        obj = self.store.get(Test, 20)
        self.store.remove(obj)
        self.store.flush()
        self.store.rollback()

        self.assertTrue(id(obj) not in self.store._dirty)
        self.assertTrue(id(obj) not in self.store._ghosts)

    def test_add_rollback_not_in_store(self):
        obj = Test()
        obj.id = 40
        obj.title = "Title 40"

        self.store.add(obj)
        self.store.rollback()

        self.assertEquals(Store.of(obj), None)

    def test_update_flush_commit(self):
        obj = self.store.get(Test, 20)
        obj.title = "Title 200"

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])
        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])
        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_update_commit(self):
        obj = self.store.get(Test, 20)
        obj.title = "Title 200"

        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_update_commit_twice(self):
        obj = self.store.get(Test, 20)
        obj.title = "Title 200"
        self.store.commit()
        obj.title = "Title 2000"
        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 2000"),
                          (30, "Title 10"),
                         ])

    def test_update_primary_key(self):
        obj = self.store.get(Test, 20)
        obj.id = 25

        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (25, "Title 20"),
                          (30, "Title 10"),
                         ])

        # Update twice to see if the notion of primary key for the
        # existent object was updated as well.
        
        obj.id = 27

        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (27, "Title 20"),
                          (30, "Title 10"),
                         ])

        # Ensure only the right ones are there.

        self.assertTrue(self.store.get(Test, 27) is obj)
        self.assertTrue(self.store.get(Test, 25) is None)
        self.assertTrue(self.store.get(Test, 20) is None)

    def test_update_primary_key_exchange(self):
        obj1 = self.store.get(Test, 10)
        obj2 = self.store.get(Test, 30)

        obj1.id = 40
        self.store.flush()
        obj2.id = 10
        self.store.flush()
        obj1.id = 30

        self.assertTrue(self.store.get(Test, 30) is obj1)
        self.assertTrue(self.store.get(Test, 10) is obj2)

        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 10"),
                          (20, "Title 20"),
                          (30, "Title 30"),
                         ])

    def test_wb_update_flush_flush(self):
        obj = self.store.get(Test, 20)
        obj.title = "Title 200"

        self.store.flush()

        # If changes get committed even with the notification disabled,
        # it means the dirty flag isn't being cleared.

        self.store._disable_change_notification(obj)

        obj.title = "Title 2000"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_update_find(self):
        obj = self.store.get(Test, 20)
        obj.title = "Title 200"
        
        result = self.store.find(Test, Test.title == "Title 200")

        self.assertTrue(result.one() is obj)

    def test_update_get(self):
        obj = self.store.get(Test, 20)
        obj.id = 200

        self.assertTrue(self.store.get(Test, 200) is obj)

    def test_add_update(self):
        obj = Test()
        obj.id = 40
        obj.title = "Title 40"

        self.store.add(obj)

        obj.title = "Title 400"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                          (40, "Title 400"),
                         ])

    def test_add_remove_add(self):
        obj = Test()
        obj.id = 40
        obj.title = "Title 40"

        self.store.add(obj)
        self.store.remove(obj)

        self.assertEquals(Store.of(obj), self.store)

        obj.title = "Title 400"

        self.store.add(obj)

        obj.id = 400

        self.store.commit()

        self.assertEquals(Store.of(obj), self.store)

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                          (400, "Title 400"),
                         ])

        self.assertTrue(self.store.get(Test, 400) is obj)

    def test_wb_add_remove_add(self):
        obj = Test()
        self.store.add(obj)
        self.assertTrue(id(obj) in self.store._dirty)
        self.store.remove(obj)
        self.assertTrue(id(obj) not in self.store._dirty)
        self.store.add(obj)
        self.assertTrue(id(obj) in self.store._dirty)
        self.assertTrue(Store.of(obj) is self.store)

    def test_wb_update_remove_add(self):
        obj = self.store.get(Test, 20)
        obj.title = "Title 200"

        self.store.remove(obj)
        self.store.add(obj)

        self.assertTrue(id(obj) in self.store._dirty)

    def test_sub_class(self):
        class SubTest(Test):
            title = Int()

        obj = self.store.get(Test, 20)
        obj.title = "20"

        obj1 = self.store.get(Test, 20)
        obj2 = self.store.get(SubTest, 20)

        self.assertEquals(obj1.id, 20)
        self.assertEquals(obj1.title, "20")
        self.assertEquals(obj2.id, 20)
        self.assertEquals(obj2.title, 20)

    def test_join(self):

        class Other(object):
            __table__ = "other", "id"
            id = Int()
            other_title = Str()

        other = Other()
        other.id = 40
        other.other_title = "Title 20"

        self.store.add(other)

        # Add another object with the same title to ensure DISTINCT
        # is in place.

        other = Other()
        other.id = 400
        other.other_title = "Title 20"

        self.store.add(other)

        result = self.store.find(Test, Test.title == Other.other_title)

        self.assertEquals([(obj.id, obj.title) for obj in result], [
                          (20, "Title 20")
                         ])

    def test_sub_select(self):
        obj = self.store.find(Test, Test.id == Select("20")).one()
        self.assertTrue(obj)
        self.assertEquals(obj.id, 20)
        self.assertEquals(obj.title, "Title 20")

    def test_attribute_rollback(self):
        obj = self.store.get(Test, 20)
        obj.some_attribute = 1
        self.store.rollback()
        self.assertEquals(getattr(obj, "some_attribute", None), None)

    def test_attribute_rollback_after_commit(self):
        obj = self.store.get(Test, 20)
        obj.some_attribute = 1
        self.store.commit()
        obj.some_attribute = 2
        self.store.rollback()
        self.assertEquals(getattr(obj, "some_attribute", None), 1)

    def test_cache_has_improper_object(self):
        obj = self.store.get(Test, 20)
        self.store.remove(obj)
        self.store.commit()

        self.store.execute("INSERT INTO test VALUES (20, 'Title 20')")

        self.assertTrue(self.store.get(Test, 20) is not obj)

    def test_cache_has_improper_object_readded(self):
        obj = self.store.get(Test, 20)
        self.store.remove(obj)

        self.store.flush()

        old_obj = obj # Keep a reference.

        obj = Test()
        obj.id = 20
        obj.title = "Readded"
        self.store.add(obj)

        self.store.commit()

        self.assertTrue(self.store.get(Test, 20) is obj)

    def test__load__(self):

        loaded = []

        class MyTest(Test):
            def __init__(self):
                loaded.append("NO!")
            def __load__(self):
                loaded.append((self.id, self.title))
                self.title = "Title 200"
                self.some_attribute = 1

        obj = self.store.get(MyTest, 20)

        self.assertEquals(loaded, [(20, "Title 20")])
        self.assertEquals(obj.title, "Title 200")
        self.assertEquals(obj.some_attribute, 1)

        obj.some_attribute = 2

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

        self.store.rollback()

        self.assertEquals(obj.title, "Title 20")
        self.assertEquals(getattr(obj, "some_attribute", None), 1)

    def test_retrieve_default_primary_key(self):
        obj = Test()
        obj.title = "Title 40"
        self.store.add(obj)
        self.store.flush()
        self.assertNotEquals(obj.id, None)
        self.assertTrue(self.store.get(Test, obj.id) is obj)

    def test_retrieve_default_value(self):
        obj = Test()
        obj.id = 40
        self.store.add(obj)
        self.store.flush()
        self.assertEquals(obj.title, "Default Title")

    def test_wb_remove_prop_not_dirty(self):
        obj = self.store.get(Test, 20)
        del obj.title
        self.assertTrue(id(obj) not in self.store._dirty)

    def test_flush_with_removed_prop(self):
        obj = self.store.get(Test, 20)
        del obj.title
        self.store.flush()
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_flush_with_removed_prop_forced_dirty(self):
        obj = self.store.get(Test, 20)
        del obj.title
        obj.id = 40
        obj.id = 20
        self.store.flush()
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_flush_with_removed_prop_really_dirty(self):
        obj = self.store.get(Test, 20)
        del obj.title
        obj.id = 25
        self.store.flush()
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (25, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_reference(self):
        other = self.store.get(Other, 100)
        self.assertTrue(other.test)
        self.assertEquals(other.test.title, "Title 30")

    def test_new_object_reference(self):
        other = Other()
        other.id = 400
        other.title = "Title 400"
        other.test_id = 10

        self.assertEquals(other.test, None)

        self.store.add(other)

        self.assertTrue(other.test)
        self.assertEquals(other.test.title, "Title 30")

    def test_set_object_reference(self):
        other = self.store.get(Other, 100)
        self.assertEquals(other.test.id, 10)
        test = self.store.get(Test, 30)
        other.test = test
        self.assertEquals(other.test.id, 30)
        result = self.store.execute("SELECT test_id FROM other WHERE id=100")
        self.assertEquals(result.fetch_one(), (30,))

    def test_object_reference_on_added(self):
        obj = Test()
        obj.title = "Title 40"

        other = Other()
        other.id = 400
        other.test = obj
        other.other_title = "Title 400"

        self.assertEquals(other.test.id, None)
        self.assertEquals(other.test.title, "Title 40")

        self.store.add(obj)
        self.store.add(other)

        self.store.flush()

        self.assertTrue(other.test.id)
        self.assertEquals(other.test.title, "Title 40")

        result = self.store.execute("SELECT test.title FROM test, other "
                                    "WHERE other.id=400 AND "
                                    "test.id = other.test_id")
        self.assertEquals(result.fetch_one(), ("Title 40",))

    def test_object_reference_on_added_composed_key(self):
        class Test(object):
            __table__ = "test", ("id", "title")
            id = Int()
            title = Str()

        class Other(object):
            __table__ = "other", "id"
            id = Int()
            test_id = Int()
            other_title = Str()
            test = Reference(Test, (test_id, other_title))

        obj = Test()
        obj.title = "Title 40"

        other = Other()
        other.id = 400
        other.test = obj

        self.assertEquals(other.test.id, None)
        self.assertEquals(other.test.title, "Title 40")
        self.assertEquals(other.other_title, "Title 40")

        self.store.add(obj)
        self.store.add(other)

        self.store.flush()

        self.assertTrue(other.test.id)
        self.assertEquals(other.test.title, "Title 40")

        result = self.store.execute("SELECT test.title FROM test, other "
                                    "WHERE other.id=400 AND "
                                    "test.id = other.test_id")
        self.assertEquals(result.fetch_one(), ("Title 40",))

    def test_object_reference_on_added_unlink_on_flush(self):
        obj = Test()
        obj.title = "Title 40"

        other = Other()
        other.id = 400
        other.test = obj
        other.other_title = "Title 400"

        self.store.add(obj)
        self.store.add(other)

        obj.id = 40
        self.assertEquals(other.test_id, 40)
        obj.id = 50
        self.assertEquals(other.test_id, 50)
        obj.id = 60
        self.assertEquals(other.test_id, 60)

        self.store.flush()

        obj.id = 70
        self.assertEquals(other.test_id, 60)

    def test_object_reference_on_added_unlink_on_flush(self):
        obj = Test()
        obj.title = "Title 40"

        other = Other()
        other.id = 400
        other.other_title = "Title 400"
        other.test = obj

        self.store.add(obj)
        self.store.add(other)

        obj.id = 40
        self.assertEquals(other.test_id, 40)
        obj.id = 50
        self.assertEquals(other.test_id, 50)
        obj.id = 60
        self.assertEquals(other.test_id, 60)

        self.store.flush()

        obj.id = 70
        self.assertEquals(other.test_id, 60)

    def test_object_reference_on_two_added(self):
        obj1 = Test()
        obj1.title = "Title 40"
        obj2 = Test()
        obj2.title = "Title 40"

        other = Other()
        other.id = 400
        other.other_title = "Title 400"
        other.test = obj1
        other.test = obj2

        self.store.add(obj1)
        self.store.add(obj2)
        self.store.add(other)

        obj1.id = 40
        self.assertEquals(other.test_id, None)
        obj2.id = 50
        self.assertEquals(other.test_id, 50)

    def test_object_reference_on_added_and_changed_manually(self):
        obj = Test()
        obj.title = "Title 40"

        other = Other()
        other.id = 400
        other.other_title = "Title 400"
        other.test = obj

        self.store.add(obj)
        self.store.add(other)

        other.test_id = 40
        obj.id = 50
        self.assertEquals(other.test_id, 40)

    def test_object_reference_on_added_composed_key_changed_manually(self):
        class Test(object):
            __table__ = "test", ("id", "title")
            id = Int()
            title = Str()

        class Other(object):
            __table__ = "other", "id"
            id = Int()
            test_id = Int()
            other_title = Str()
            test = Reference(Test, (test_id, other_title))

        obj = Test()
        obj.title = "Title 40"

        other = Other()
        other.id = 400
        other.test = obj

        self.store.add(obj)
        self.store.add(other)

        other.other_title = "Title 50"

        self.assertEquals(other.test, None)

        obj.id = 40

        self.assertEquals(other.test_id, None)


class SQLiteStoreTest(StoreTest):

    helpers = [MakePath]

    def create_database(self):
        self.database = SQLite(self.make_path())

    def create_tables(self):
        connection = self.database.connect()
        connection.execute("CREATE TABLE test "
                           "(id INTEGER PRIMARY KEY,"
                           " title VARCHAR DEFAULT 'Default Title')")
        connection.execute("CREATE TABLE other "
                           "(id INT PRIMARY KEY,"
                           " test_id INTEGER,"
                           " other_title VARCHAR)")
        connection.commit()

    def drop_tables(self):
        pass


class PostgresStoreTest(StoreTest):

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

del StoreTest
