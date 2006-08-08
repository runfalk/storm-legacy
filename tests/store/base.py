import gc

from storm.properties import get_obj_info
from storm.references import Reference, ReferenceSet
from storm.database import Result
from storm.properties import Int, Str, Unicode, Property, Pickle
from storm.expr import Asc, Desc, Select, Func
from storm.variables import Variable, UnicodeVariable, IntVariable
from storm.exceptions import *
from storm.store import *

from tests.helper import run_this


class Foo(object):
    __table__ = "foo", "id"
    id = Int()
    title = Unicode()

class Bar(object):
    __table__ = "bar", "id"
    id = Int()
    title = Unicode()
    foo_id = Int()
    foo = Reference(foo_id, Foo.id)

class Blob(object):
    __table__ = "bin", "id"
    id = Int()
    bin = Str()

class Link(object):
    __table__ = "link", ("foo_id", "bar_id")
    foo_id = Int()
    bar_id = Int()

class FooRef(Foo):
    bar = Reference(Foo.id, Bar.foo_id)

class FooRefSet(Foo):
    bars = ReferenceSet(Foo.id, Bar.foo_id)

class FooRefSetOrderID(Foo):
    bars = ReferenceSet(Foo.id, Bar.foo_id, order_by=Bar.id)

class FooRefSetOrderTitle(Foo):
    bars = ReferenceSet(Foo.id, Bar.foo_id, order_by=Bar.title)

class FooIndRefSet(Foo):
    bars = ReferenceSet(Foo.id, Link.foo_id, Link.bar_id, Bar.id)

class FooIndRefSetOrderID(Foo):
    bars = ReferenceSet(Foo.id, Link.foo_id, Link.bar_id, Bar.id,
                        order_by=Bar.id)

class FooIndRefSetOrderTitle(Foo):
    bars = ReferenceSet(Foo.id, Link.foo_id, Link.bar_id, Bar.id,
                        order_by=Bar.title)


class DecorateVariable(Variable):

    def _parse_get(self, value, to_db):
        return u"to_%s(%s)" % (to_db and "db" or "py", value)

    def _parse_set(self, value, from_db):
        return u"from_%s(%s)" % (from_db and "db" or "py", value)


class FooVariable(Foo):
    title = Property(None, cls=DecorateVariable)


class Proxy(object):

    def __init__(self, obj):
        self._obj = obj

Proxy.__object_info = property(lambda self: self._obj.__object_info)


class StoreTest(object):

    def setUp(self):
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

    def create_database(self):
        raise NotImplementedError

    def create_tables(self):
        raise NotImplementedError

    def create_sample_data(self):
        connection = self.database.connect()
        connection.execute("INSERT INTO foo VALUES (10, 'Title 30')")
        connection.execute("INSERT INTO foo VALUES (20, 'Title 20')")
        connection.execute("INSERT INTO foo VALUES (30, 'Title 10')")
        connection.execute("INSERT INTO bar VALUES (100, 10, 'Title 300')")
        connection.execute("INSERT INTO bar VALUES (200, 20, 'Title 200')")
        connection.execute("INSERT INTO bar VALUES (300, 30, 'Title 100')")
        connection.execute("INSERT INTO bin VALUES (10, 'Blob 30')")
        connection.execute("INSERT INTO bin VALUES (20, 'Blob 20')")
        connection.execute("INSERT INTO bin VALUES (30, 'Blob 10')")
        connection.execute("INSERT INTO link VALUES (10, 100)")
        connection.execute("INSERT INTO link VALUES (10, 200)")
        connection.execute("INSERT INTO link VALUES (10, 300)")
        connection.execute("INSERT INTO link VALUES (20, 100)")
        connection.execute("INSERT INTO link VALUES (20, 200)")
        connection.execute("INSERT INTO link VALUES (30, 300)")
        connection.commit()

    def create_store(self):
        self.store = Store(self.database)

    def drop_store(self):
        self.store.rollback()

        # Closing the store is needed because testcase objects are all
        # instantiated at once, and thus connections are kept open.
        self.store.close()

    def drop_sample_data(self):
        pass

    def drop_tables(self):
        for table in ["foo", "bar", "bin", "link"]:
            connection = self.database.connect()
            try:
                connection.execute("DROP TABLE %s" % table)
                connection.commit()
            except:
                connection.rollback()

    def drop_database(self):
        pass

    def get_items(self):
        # Bypass the store to avoid flushing.
        connection = self.store._connection
        result = connection.execute("SELECT * FROM foo ORDER BY id")
        return list(result)

    def get_committed_items(self):
        connection = self.database.connect()
        result = connection.execute("SELECT * FROM foo ORDER BY id")
        return list(result)


    def test_execute(self):
        result = self.store.execute("SELECT 1")
        self.assertTrue(isinstance(result, Result))
        self.assertEquals(result.get_one(), (1,))
        
        result = self.store.execute("SELECT 1", noresult=True)
        self.assertEquals(result, None)

    def test_execute_params(self):
        result = self.store.execute("SELECT ?", [1])
        self.assertTrue(isinstance(result, Result))
        self.assertEquals(result.get_one(), (1,))

    def test_execute_flushes(self):
        foo = self.store.get(Foo, 10)
        foo.title = "New Title"

        result = self.store.execute("SELECT title FROM foo WHERE id=10")
        self.assertEquals(result.get_one(), ("New Title",))

    def test_close(self):
        store = Store(self.database)
        store.close()
        self.assertRaises(ClosedError, store.execute, "SELECT 1")

    def test_get(self):
        foo = self.store.get(Foo, 10)
        self.assertEquals(foo.id, 10)
        self.assertEquals(foo.title, "Title 30")

        foo = self.store.get(Foo, 20)
        self.assertEquals(foo.id, 20)
        self.assertEquals(foo.title, "Title 20")

        foo = self.store.get(Foo, 40)
        self.assertEquals(foo, None)

    def test_get_cached(self):
        foo = self.store.get(Foo, 10)
        self.assertTrue(self.store.get(Foo, 10) is foo)

    def test_wb_get_cached_doesnt_need_connection(self):
        foo = self.store.get(Foo, 10)
        connection = self.store._connection
        self.store._connection = None
        self.store.get(Foo, 10)
        self.store._connection = connection

    def test_cache_cleanup(self):
        foo = self.store.get(Foo, 10)
        foo.taint = True

        del foo
        gc.collect()

        foo = self.store.get(Foo, 10)
        self.assertFalse(getattr(foo, "taint", False))

    def test_get_tuple(self):
        class Foo(object):
            __table__ = "foo", ("title", "id")
            id = Int()
            title = Unicode()
        foo = self.store.get(Foo, ("Title 30", 10))
        self.assertEquals(foo.id, 10)
        self.assertEquals(foo.title, "Title 30")

        foo = self.store.get(Foo, ("Title 20", 10))
        self.assertEquals(foo, None)

    def test_of(self):
        foo = self.store.get(Foo, 10)
        self.assertEquals(Store.of(foo), self.store)
        self.assertEquals(Store.of(Foo()), None)
        self.assertEquals(Store.of(object()), None)

    def test_find_iter(self):
        result = self.store.find(Foo)

        lst = [(foo.id, foo.title) for foo in result]
        lst.sort()
        self.assertEquals(lst, [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_find_from_cache(self):
        foo = self.store.get(Foo, 10)
        self.assertTrue(self.store.find(Foo, id=10).one() is foo)

    def test_find_expr(self):
        result = self.store.find(Foo, Foo.id == 20,
                                 Foo.title == "Title 20")
        self.assertEquals([(foo.id, foo.title) for foo in result], [
                          (20, "Title 20"),
                         ])

        result = self.store.find(Foo, Foo.id == 10,
                                 Foo.title == "Title 20")
        self.assertEquals([(foo.id, foo.title) for foo in result], [
                         ])

    def test_find_str(self):
        # find() doesn't support non-Expr instances.
        self.assertRaises(UnsupportedError,
                          self.store.find, Foo, "foo.id == 20")

    def test_find_keywords(self):
        result = self.store.find(Foo, id=20, title="Title 20")
        self.assertEquals([(foo.id, foo.title) for foo in result], [
                          (20, "Title 20")
                         ])

        result = self.store.find(Foo, id=10, title="Title 20")
        self.assertEquals([(foo.id, foo.title) for foo in result], [
                         ])

    def test_find_order_by(self, *args):
        result = self.store.find(Foo).order_by(Foo.title)
        lst = [(foo.id, foo.title) for foo in result]
        self.assertEquals(lst, [
                          (30, "Title 10"),
                          (20, "Title 20"),
                          (10, "Title 30"),
                         ])

    def test_find_order_asc(self, *args):
        result = self.store.find(Foo).order_by(Asc(Foo.title))
        lst = [(foo.id, foo.title) for foo in result]
        self.assertEquals(lst, [
                          (30, "Title 10"),
                          (20, "Title 20"),
                          (10, "Title 30"),
                         ])

    def test_find_order_desc(self, *args):
        result = self.store.find(Foo).order_by(Desc(Foo.title))
        lst = [(foo.id, foo.title) for foo in result]
        self.assertEquals(lst, [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_find_index(self):
        foo = self.store.find(Foo).order_by(Foo.title)[0]
        self.assertEquals(foo.id, 30)
        self.assertEquals(foo.title, "Title 10")

        foo = self.store.find(Foo).order_by(Foo.title)[1]
        self.assertEquals(foo.id, 20)
        self.assertEquals(foo.title, "Title 20")

        foo = self.store.find(Foo).order_by(Foo.title)[2]
        self.assertEquals(foo.id, 10)
        self.assertEquals(foo.title, "Title 30")

        foo = self.store.find(Foo).order_by(Foo.title)[1:][1]
        self.assertEquals(foo.id, 10)
        self.assertEquals(foo.title, "Title 30")

        result = self.store.find(Foo).order_by(Foo.title)
        self.assertRaises(IndexError, result.__getitem__, 3)

    def test_find_slice(self):
        result = self.store.find(Foo).order_by(Foo.title)[1:2]
        lst = [(foo.id, foo.title) for foo in result]
        self.assertEquals(lst,
                          [(20, "Title 20")])

    def test_find_slice_offset(self):
        result = self.store.find(Foo).order_by(Foo.title)[1:]
        lst = [(foo.id, foo.title) for foo in result]
        self.assertEquals(lst, 
                          [(20, "Title 20"),
                           (10, "Title 30")])

    def test_find_slice_offset_any(self):
        foo = self.store.find(Foo).order_by(Foo.title)[1:].any()
        self.assertEquals(foo.id, 20)
        self.assertEquals(foo.title, "Title 20")

    def test_find_slice_offset_one(self):
        foo = self.store.find(Foo).order_by(Foo.title)[1:2].one()
        self.assertEquals(foo.id, 20)
        self.assertEquals(foo.title, "Title 20")

    def test_find_slice_offset_first(self):
        foo = self.store.find(Foo).order_by(Foo.title)[1:].first()
        self.assertEquals(foo.id, 20)
        self.assertEquals(foo.title, "Title 20")

    def test_find_slice_offset_last(self):
        foo = self.store.find(Foo).order_by(Foo.title)[1:].last()
        self.assertEquals(foo.id, 10)
        self.assertEquals(foo.title, "Title 30")

    def test_find_slice_limit(self):
        result = self.store.find(Foo).order_by(Foo.title)[:2]
        lst = [(foo.id, foo.title) for foo in result]
        self.assertEquals(lst, 
                          [(30, "Title 10"),
                           (20, "Title 20")])

    def test_find_slice_limit_last(self):
        result = self.store.find(Foo).order_by(Foo.title)[:2]
        self.assertRaises(UnsupportedError, result.last)

    def test_find_slice_slice(self):
        result = self.store.find(Foo).order_by(Foo.title)[0:2][1:3]
        lst = [(foo.id, foo.title) for foo in result]
        self.assertEquals(lst,
                          [(20, "Title 20")])

        result = self.store.find(Foo).order_by(Foo.title)[:2][1:3]
        lst = [(foo.id, foo.title) for foo in result]
        self.assertEquals(lst,
                          [(20, "Title 20")])

        result = self.store.find(Foo).order_by(Foo.title)[1:3][0:1]
        lst = [(foo.id, foo.title) for foo in result]
        self.assertEquals(lst,
                          [(20, "Title 20")])

        result = self.store.find(Foo).order_by(Foo.title)[1:3][:1]
        lst = [(foo.id, foo.title) for foo in result]
        self.assertEquals(lst,
                          [(20, "Title 20")])

        result = self.store.find(Foo).order_by(Foo.title)[5:5][1:1]
        lst = [(foo.id, foo.title) for foo in result]
        self.assertEquals(lst, [])

    def test_find_slice_order_by(self):
        result = self.store.find(Foo)[2:]
        self.assertRaises(UnsupportedError, result.order_by, None)

        result = self.store.find(Foo)[:2]
        self.assertRaises(UnsupportedError, result.order_by, None)

    def test_find_slice_remove(self):
        result = self.store.find(Foo)[2:]
        self.assertRaises(UnsupportedError, result.remove)

        result = self.store.find(Foo)[:2]
        self.assertRaises(UnsupportedError, result.remove)

    def test_find_any(self, *args):
        foo = self.store.find(Foo).order_by(Foo.title).any()
        self.assertEquals(foo.id, 30)
        self.assertEquals(foo.title, "Title 10")

        foo = self.store.find(Foo).order_by(Foo.id).any()
        self.assertEquals(foo.id, 10)
        self.assertEquals(foo.title, "Title 30")

        foo = self.store.find(Foo, id=40).any()
        self.assertEquals(foo, None)

    def test_find_first(self, *args):
        self.assertRaises(UnorderedError, self.store.find(Foo).first)

        foo = self.store.find(Foo).order_by(Foo.title).first()
        self.assertEquals(foo.id, 30)
        self.assertEquals(foo.title, "Title 10")

        foo = self.store.find(Foo).order_by(Foo.id).first()
        self.assertEquals(foo.id, 10)
        self.assertEquals(foo.title, "Title 30")

        foo = self.store.find(Foo, id=40).order_by(Foo.id).first()
        self.assertEquals(foo, None)

    def test_find_last(self, *args):
        self.assertRaises(UnorderedError, self.store.find(Foo).last)

        foo = self.store.find(Foo).order_by(Foo.title).last()
        self.assertEquals(foo.id, 10)
        self.assertEquals(foo.title, "Title 30")

        foo = self.store.find(Foo).order_by(Foo.id).last()
        self.assertEquals(foo.id, 30)
        self.assertEquals(foo.title, "Title 10")

        foo = self.store.find(Foo, id=40).order_by(Foo.id).last()
        self.assertEquals(foo, None)

    def test_find_last_desc(self, *args):
        foo = self.store.find(Foo).order_by(Desc(Foo.title)).last()
        self.assertEquals(foo.id, 30)
        self.assertEquals(foo.title, "Title 10")

        foo = self.store.find(Foo).order_by(Asc(Foo.id)).last()
        self.assertEquals(foo.id, 30)
        self.assertEquals(foo.title, "Title 10")

    def test_find_one(self, *args):
        self.assertRaises(NotOneError, self.store.find(Foo).one)

        foo = self.store.find(Foo, id=10).one()
        self.assertEquals(foo.id, 10)
        self.assertEquals(foo.title, "Title 30")

        foo = self.store.find(Foo, id=40).one()
        self.assertEquals(foo, None)

    def test_find_count(self):
        self.assertEquals(self.store.find(Foo).count(), 3)

    def test_find_max(self):
        self.assertEquals(self.store.find(Foo).max(Foo.id), 30)

    def test_find_max_unicode(self):
        title = self.store.find(Foo).max(Foo.title)
        self.assertEquals(title, "Title 30")
        self.assertTrue(isinstance(title, unicode))

    def test_find_min(self):
        self.assertEquals(self.store.find(Foo).min(Foo.id), 10)

    def test_find_min_unicode(self):
        title = self.store.find(Foo).min(Foo.title)
        self.assertEquals(title, "Title 10")
        self.assertTrue(isinstance(title, unicode))

    def test_find_avg(self):
        self.assertEquals(self.store.find(Foo).avg(Foo.id), 20)

    def test_find_avg_float(self):
        foo = Foo()
        foo.id = 15
        foo.title = "Title 15"
        self.store.add(foo)
        self.assertEquals(self.store.find(Foo).avg(Foo.id), 18.75)

    def test_find_sum(self):
        self.assertEquals(self.store.find(Foo).sum(Foo.id), 60)

    def test_find_max_order_by(self):
        """Interaction between order by and aggregation shouldn't break."""
        result = self.store.find(Foo)
        self.assertEquals(result.order_by(Foo.id).max(Foo.id), 30)

    def test_find_values(self):
        values = self.store.find(Foo).order_by(Foo.id).values(Foo.id)
        self.assertEquals(list(values), [10, 20, 30])

        values = self.store.find(Foo).order_by(Foo.id).values(Foo.title)
        values = list(values)
        self.assertEquals(values, ["Title 30", "Title 20", "Title 10"])
        self.assertEquals([type(value) for value in values],
                          [unicode, unicode, unicode])

    def test_find_multiple_values(self):
        result = self.store.find(Foo).order_by(Foo.id)
        values = result.values(Foo.id, Foo.title)
        self.assertEquals(list(values),
                          [(10, "Title 30"),
                           (20, "Title 20"),
                           (30, "Title 10")])

    def test_find_values_with_no_arguments(self):
        result = self.store.find(Foo).order_by(Foo.id)
        self.assertRaises(TypeError, result.values().next)

    def test_find_slice_values(self):
        values = self.store.find(Foo).order_by(Foo.id)[1:2].values(Foo.id)
        self.assertEquals(list(values), [20])

    def test_find_remove(self):
        self.store.find(Foo, Foo.id == 20).remove()
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (30, "Title 10"),
                         ])

    def test_find_cached(self):
        foo = self.store.get(Foo, 20)
        bar = self.store.get(Bar, 200)
        self.assertTrue(foo)
        self.assertTrue(bar)
        self.assertEquals(self.store.find(Foo).cached(), [foo])

    def test_find_cached_where(self):
        foo1 = self.store.get(Foo, 10)
        foo2 = self.store.get(Foo, 20)
        bar = self.store.get(Bar, 200)
        self.assertTrue(foo1)
        self.assertTrue(foo2)
        self.assertTrue(bar)
        self.assertEquals(self.store.find(Foo, title="Title 20").cached(),
                          [foo2])


    def test_add_commit(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"

        self.store.add(foo)

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
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"

        self.store.add(foo)
        self.store.rollback()

        self.assertEquals(self.store.get(Foo, 3), None)

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
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"

        self.store.add(foo)

        old_foo = foo

        foo = self.store.get(Foo, 40)

        self.assertEquals(foo.id, 40)
        self.assertEquals(foo.title, "Title 40")

        self.assertTrue(foo is old_foo)

    def test_add_find(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"

        self.store.add(foo)

        old_foo = foo

        foo = self.store.find(Foo, Foo.id == 40).one()

        self.assertEquals(foo.id, 40)
        self.assertEquals(foo.title, "Title 40")

        self.assertTrue(foo is old_foo)

    def test_add_twice(self):
        foo = Foo()
        self.store.add(foo)
        self.store.add(foo)
        self.assertEquals(Store.of(foo), self.store)

    def test_add_loaded(self):
        foo = self.store.get(Foo, 10)
        self.store.add(foo)
        self.assertEquals(Store.of(foo), self.store)

    def test_add_twice_to_wrong_store(self):
        foo = Foo()
        self.store.add(foo)
        self.assertRaises(WrongStoreError, Store(self.database).add, foo)

    def test_add_checkpoints(self):
        bar = Bar()
        self.store.add(bar)

        bar.id = 400
        bar.title = "Title 400"
        bar.foo_id = 40

        self.store.flush()
        self.store.execute("UPDATE bar SET title='Title 500' "
                           "WHERE id=400")
        bar.foo_id = 400

        # When not checkpointing, this flush will set title again.
        self.store.flush()
        self.store.reload(bar)
        self.assertEquals(bar.title, "Title 500")

    def test_new(self):
        class MyFoo(Foo):
            def __init__(self, id, title):
                self.id = id
                self.title = title

        foo = self.store.new(MyFoo, 40, title="Title 40")
        
        self.assertEquals(type(foo), MyFoo)
        self.assertEquals(foo.id, 40)
        self.assertEquals(foo.title, "Title 40")
        self.assertEquals(Store.of(foo), self.store)

    def test_remove_commit(self):
        foo = self.store.get(Foo, 20)
        self.store.remove(foo)
        self.assertEquals(Store.of(foo), self.store)
        self.store.flush()
        self.assertEquals(Store.of(foo), self.store)

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
        self.assertEquals(Store.of(foo), None)

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (30, "Title 10"),
                         ])

    def test_remove_rollback_update(self):
        foo = self.store.get(Foo, 20)

        self.store.remove(foo)
        self.store.rollback()
        
        foo.title = "Title 200"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_remove_flush_rollback_update(self):
        foo = self.store.get(Foo, 20)

        self.store.remove(foo)
        self.store.flush()
        self.store.rollback()

        foo.title = "Title 200"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_remove_add_update(self):
        foo = self.store.get(Foo, 20)

        self.store.remove(foo)
        self.store.add(foo)
        
        foo.title = "Title 200"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_remove_flush_add_update(self):
        foo = self.store.get(Foo, 20)

        self.store.remove(foo)
        self.store.flush()
        self.store.add(foo)
        
        foo.title = "Title 200"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_remove_twice(self):
        foo = self.store.get(Foo, 10)
        self.store.remove(foo)
        self.store.remove(foo)

    def test_remove_unknown(self):
        foo = Foo()
        self.assertRaises(WrongStoreError, self.store.remove, foo)

    def test_remove_from_wrong_store(self):
        foo = self.store.get(Foo, 20)
        self.assertRaises(WrongStoreError, Store(self.database).remove, foo)

    def test_wb_remove_flush_update_isnt_dirty(self):
        foo = self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        self.store.remove(foo)
        self.store.flush()
        
        foo.title = "Title 200"

        self.assertTrue(obj_info not in self.store._dirty)

    def test_wb_remove_rollback_isnt_dirty_or_ghost(self):
        foo = self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        self.store.remove(foo)
        self.store.rollback()

        self.assertTrue(obj_info not in self.store._dirty)
        self.assertTrue(obj_info not in self.store._ghosts)

    def test_wb_remove_flush_rollback_isnt_dirty_or_ghost(self):
        foo = self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        self.store.remove(foo)
        self.store.flush()
        self.store.rollback()

        self.assertTrue(obj_info not in self.store._dirty)
        self.assertTrue(obj_info not in self.store._ghosts)

    def test_add_rollback_not_in_store(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"

        self.store.add(foo)
        self.store.rollback()

        self.assertEquals(Store.of(foo), None)

    def test_update_flush_commit(self):
        foo = self.store.get(Foo, 20)
        foo.title = "Title 200"

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

    def test_update_flush_reload_rollback(self):
        foo = self.store.get(Foo, 20)
        foo.title = "Title 200"
        self.store.flush()
        self.store.reload(foo)
        self.store.rollback()
        self.assertEquals(foo.title, "Title 20")

    def test_update_commit(self):
        foo = self.store.get(Foo, 20)
        foo.title = "Title 200"

        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_update_commit_twice(self):
        foo = self.store.get(Foo, 20)
        foo.title = "Title 200"
        self.store.commit()
        foo.title = "Title 2000"
        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 2000"),
                          (30, "Title 10"),
                         ])

    def test_update_checkpoints(self):
        bar = self.store.get(Bar, 200)
        bar.title = "Title 400"
        self.store.flush()
        self.store.execute("UPDATE bar SET title='Title 500' "
                           "WHERE id=200")
        bar.foo_id = 40
        # When not checkpointing, this flush will set title again.
        self.store.flush()
        self.store.reload(bar)
        self.assertEquals(bar.title, "Title 500")

    def test_update_primary_key(self):
        foo = self.store.get(Foo, 20)
        foo.id = 25

        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (25, "Title 20"),
                          (30, "Title 10"),
                         ])

        # Update twice to see if the notion of primary key for the
        # existent object was updated as well.
        
        foo.id = 27

        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (27, "Title 20"),
                          (30, "Title 10"),
                         ])

        # Ensure only the right ones are there.

        self.assertTrue(self.store.get(Foo, 27) is foo)
        self.assertTrue(self.store.get(Foo, 25) is None)
        self.assertTrue(self.store.get(Foo, 20) is None)

    def test_update_primary_key_exchange(self):
        foo1 = self.store.get(Foo, 10)
        foo2 = self.store.get(Foo, 30)

        foo1.id = 40
        self.store.flush()
        foo2.id = 10
        self.store.flush()
        foo1.id = 30

        self.assertTrue(self.store.get(Foo, 30) is foo1)
        self.assertTrue(self.store.get(Foo, 10) is foo2)

        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 10"),
                          (20, "Title 20"),
                          (30, "Title 30"),
                         ])

    def test_wb_update_not_dirty_after_flush(self):
        foo = self.store.get(Foo, 20)
        foo.title = "Title 200"

        self.store.flush()

        # If changes get committed even with the notification disabled,
        # it means the dirty flag isn't being cleared.

        self.store._disable_change_notification(get_obj_info(foo))

        foo.title = "Title 2000"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_update_find(self):
        foo = self.store.get(Foo, 20)
        foo.title = "Title 200"
        
        result = self.store.find(Foo, Foo.title == "Title 200")

        self.assertTrue(result.one() is foo)

    def test_update_get(self):
        foo = self.store.get(Foo, 20)
        foo.id = 200

        self.assertTrue(self.store.get(Foo, 200) is foo)

    def test_add_update(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"

        self.store.add(foo)

        foo.title = "Title 400"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                          (40, "Title 400"),
                         ])

    def test_add_remove_add(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"

        self.store.add(foo)
        self.store.remove(foo)

        self.assertEquals(Store.of(foo), self.store)

        foo.title = "Title 400"

        self.store.add(foo)

        foo.id = 400

        self.store.commit()

        self.assertEquals(Store.of(foo), self.store)

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                          (400, "Title 400"),
                         ])

        self.assertTrue(self.store.get(Foo, 400) is foo)

    def test_wb_add_remove_add(self):
        foo = Foo()
        obj_info = get_obj_info(foo)
        self.store.add(foo)
        self.assertTrue(obj_info in self.store._dirty)
        self.store.remove(foo)
        self.assertTrue(obj_info not in self.store._dirty)
        self.store.add(foo)
        self.assertTrue(obj_info in self.store._dirty)
        self.assertTrue(Store.of(foo) is self.store)

    def test_wb_update_remove_add(self):
        foo = self.store.get(Foo, 20)
        foo.title = "Title 200"

        obj_info = get_obj_info(foo)

        self.store.remove(foo)
        self.store.add(foo)

        self.assertTrue(obj_info in self.store._dirty)

    def test_sub_class(self):
        class SubFoo(Foo):
            title = Int()

        foo = self.store.get(Foo, 20)
        foo.title = "20"

        foo1 = self.store.get(Foo, 20)
        foo2 = self.store.get(SubFoo, 20)

        self.assertEquals(foo1.id, 20)
        self.assertEquals(foo1.title, "20")
        self.assertEquals(foo2.id, 20)
        self.assertEquals(foo2.title, 20)

    def test_join(self):

        class Bar(object):
            __table__ = "bar", "id"
            id = Int()
            title = Unicode()

        bar = Bar()
        bar.id = 40
        bar.title = "Title 20"

        self.store.add(bar)

        # Add anbar object with the same title to ensure DISTINCT
        # is in place.

        bar = Bar()
        bar.id = 400
        bar.title = "Title 20"

        self.store.add(bar)

        result = self.store.find(Foo, Foo.title == Bar.title)

        self.assertEquals([(foo.id, foo.title) for foo in result], [
                          (20, "Title 20")
                         ])

    def test_sub_select(self):
        foo = self.store.find(Foo, Foo.id == Select("20")).one()
        self.assertTrue(foo)
        self.assertEquals(foo.id, 20)
        self.assertEquals(foo.title, "Title 20")

    def test_attribute_rollback(self):
        foo = self.store.get(Foo, 20)
        foo.some_attribute = 1
        self.store.rollback()
        self.assertEquals(getattr(foo, "some_attribute", None), None)

    def test_attribute_rollback_after_commit(self):
        foo = self.store.get(Foo, 20)
        foo.some_attribute = 1
        self.store.commit()
        foo.some_attribute = 2
        self.store.rollback()
        self.assertEquals(getattr(foo, "some_attribute", None), 1)

    def test_cache_has_improper_object(self):
        foo = self.store.get(Foo, 20)
        self.store.remove(foo)
        self.store.commit()

        self.store.execute("INSERT INTO foo VALUES (20, 'Title 20')")

        self.assertTrue(self.store.get(Foo, 20) is not foo)

    def test_cache_has_improper_object_readded(self):
        foo = self.store.get(Foo, 20)
        self.store.remove(foo)

        self.store.flush()

        old_foo = foo # Keep a reference.

        foo = Foo()
        foo.id = 20
        foo.title = "Readded"
        self.store.add(foo)

        self.store.commit()

        self.assertTrue(self.store.get(Foo, 20) is foo)

    def test__load__(self):

        loaded = []

        class MyFoo(Foo):
            def __init__(self):
                loaded.append("NO!")
            def __load__(self):
                loaded.append((self.id, self.title))
                self.title = "Title 200"
                self.some_attribute = 1

        foo = self.store.get(MyFoo, 20)

        self.assertEquals(loaded, [(20, "Title 20")])
        self.assertEquals(foo.title, "Title 200")
        self.assertEquals(foo.some_attribute, 1)

        foo.some_attribute = 2

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

        self.store.rollback()

        self.assertEquals(foo.title, "Title 20")
        self.assertEquals(getattr(foo, "some_attribute", None), 1)

    def test_retrieve_default_primary_key(self):
        foo = Foo()
        foo.title = "Title 40"
        self.store.add(foo)
        self.store.flush()
        self.assertNotEquals(foo.id, None)
        self.assertTrue(self.store.get(Foo, foo.id) is foo)

    def test_retrieve_default_value(self):
        foo = Foo()
        foo.id = 40
        self.store.add(foo)
        self.store.flush()
        self.assertEquals(foo.title, "Default Title")

    def test_wb_remove_prop_not_dirty(self):
        foo = self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        del foo.title
        self.assertTrue(obj_info not in self.store._dirty)

    def test_flush_with_removed_prop(self):
        foo = self.store.get(Foo, 20)
        del foo.title
        self.store.flush()
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_flush_with_removed_prop_forced_dirty(self):
        foo = self.store.get(Foo, 20)
        del foo.title
        foo.id = 40
        foo.id = 20
        self.store.flush()
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_flush_with_removed_prop_really_dirty(self):
        foo = self.store.get(Foo, 20)
        del foo.title
        foo.id = 25
        self.store.flush()
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (25, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_reload(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("UPDATE foo SET title='Title 40' WHERE id=20")
        self.assertEquals(foo.title, "Title 20")
        self.store.reload(foo)
        self.assertEquals(foo.title, "Title 40")

    def test_reload_not_changed(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("UPDATE foo SET title='Title 40' WHERE id=20")
        self.store.reload(foo)
        for variable in get_obj_info(foo).variables.values():
            self.assertFalse(variable.has_changed())

    def test_reload_new(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"
        self.assertRaises(WrongStoreError, self.store.reload, foo)

    def test_reload_new_unflushed(self):
        foo = Foo()
        foo.id = 40
        foo.title = "Title 40"
        self.store.add(foo)
        self.assertRaises(NotFlushedError, self.store.reload, foo)

    def test_reload_removed(self):
        foo = self.store.get(Foo, 20)
        self.store.remove(foo)
        self.store.flush()
        self.assertRaises(WrongStoreError, self.store.reload, foo)

    def test_reload_unknown(self):
        foo = self.store.get(Foo, 20)
        store = Store(self.database)
        self.assertRaises(WrongStoreError, store.reload, foo)

    def test_wb_reload_not_dirty(self):
        foo = self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        foo.title = "Title 40"
        self.store.reload(foo)
        self.assertTrue(obj_info not in self.store._dirty)

    def test_find_set_empty(self):
        self.store.find(Foo, title="Title 20").set()
        foo = self.store.get(Foo, 20)
        self.assertEquals(foo.title, "Title 20")

    def test_find_set(self):
        self.store.find(Foo, title="Title 20").set(title="Title 40")
        foo = self.store.get(Foo, 20)
        self.assertEquals(foo.title, "Title 40")

    def test_find_set_column(self):
        self.store.find(Bar, title="Title 200").set(foo_id=Bar.id)
        bar = self.store.get(Bar, 200)
        self.assertEquals(bar.foo_id, 200)

    def test_find_set_expr(self):
        self.store.find(Foo, title="Title 20").set(Foo.title == "Title 40")
        foo = self.store.get(Foo, 20)
        self.assertEquals(foo.title, "Title 40")

    def test_find_set_none(self):
        self.store.find(Foo, title="Title 20").set(title=None)
        foo = self.store.get(Foo, 20)
        self.assertEquals(foo.title, None)

    def test_find_set_expr_column(self):
        self.store.find(Bar, id=200).set(Bar.foo_id == Bar.id)
        bar = self.store.get(Bar, 200)
        self.assertEquals(bar.id, 200)
        self.assertEquals(bar.foo_id, 200)

    def test_find_set_on_cached(self):
        foo1 = self.store.get(Foo, 20)
        foo2 = self.store.get(Foo, 30)
        self.store.find(Foo, id=20).set(id=40)
        self.assertEquals(foo1.id, 40)
        self.assertEquals(foo2.id, 30)

    def test_find_set_expr_on_cached(self):
        bar = self.store.get(Bar, 200)
        self.store.find(Bar, id=200).set(Bar.foo_id == Bar.id)
        self.assertEquals(bar.id, 200)
        self.assertEquals(bar.foo_id, 200)

    def test_find_set_none_on_cached(self):
        foo = self.store.get(Foo, 20)
        self.store.find(Foo, title="Title 20").set(title=None)
        self.assertEquals(foo.title, None)

    def test_find_set_on_cached_but_removed(self):
        foo1 = self.store.get(Foo, 20)
        foo2 = self.store.get(Foo, 30)
        self.store.remove(foo1)
        self.store.find(Foo, id=20).set(id=40)
        self.assertEquals(foo1.id, 20)
        self.assertEquals(foo2.id, 30)

    def test_find_set_on_cached_unsupported_python_expr(self):
        foo1 = self.store.get(Foo, 20)
        foo2 = self.store.get(Foo, 30)
        self.store.find(Foo, Foo.id == Select("20")).set(title="Title 40")
        self.assertEquals(foo1.title, "Title 40")
        self.assertEquals(foo2.title, "Title 10")

    def test_find_set_expr_unsupported(self):
        result = self.store.find(Foo, title="Title 20")
        self.assertRaises(SetError, result.set, Foo.title > "Title 40")

    def test_find_set_expr_unsupported_2(self):
        result = self.store.find(Foo, title="Title 20")
        self.assertRaises(SetError, result.set, Foo.title == Func())

    def test_wb_find_set_checkpoints(self):
        bar = self.store.get(Bar, 200)
        self.store.find(Bar, id=200).set(title="Title 400")
        self.store._connection.execute("UPDATE bar SET "
                                       "title='Title 500' "
                                       "WHERE id=200")
        # When not checkpointing, this flush will set title again.
        self.store.flush()
        self.store.reload(bar)
        self.assertEquals(bar.title, "Title 500")

    def test_reference(self):
        bar = self.store.get(Bar, 100)
        self.assertTrue(bar.foo)
        self.assertEquals(bar.foo.title, "Title 30")

    def test_reference_break_on_local_diverged(self):
        bar = self.store.get(Bar, 100)
        self.assertTrue(bar.foo)
        bar.foo_id = 40
        self.assertEquals(bar.foo, None)

    def test_reference_break_on_remote_diverged(self):
        bar = self.store.get(Bar, 100)
        bar.foo.id = 40
        self.assertEquals(bar.foo, None)

    def test_reference_on_non_primary_key(self):
        self.store.execute("INSERT INTO bar VALUES (400, 40, 'Title 30')")
        class MyBar(Bar):
            foo = Reference(Bar.title, Foo.title)

        bar = self.store.get(Bar, 400)
        self.assertEquals(bar.title, "Title 30")
        self.assertEquals(bar.foo, None)

        mybar = self.store.get(MyBar, 400)
        self.assertEquals(mybar.title, "Title 30")
        self.assertNotEquals(mybar.foo, None)
        self.assertEquals(mybar.foo.id, 10)
        self.assertEquals(mybar.foo.title, "Title 30")

    def test_new_reference(self):
        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo_id = 10

        self.assertEquals(bar.foo, None)

        self.store.add(bar)

        self.assertTrue(bar.foo)
        self.assertEquals(bar.foo.title, "Title 30")

    def test_set_reference(self):
        bar = self.store.get(Bar, 100)
        self.assertEquals(bar.foo.id, 10)
        foo = self.store.get(Foo, 30)
        bar.foo = foo
        self.assertEquals(bar.foo.id, 30)
        result = self.store.execute("SELECT foo_id FROM bar WHERE id=100")
        self.assertEquals(result.get_one(), (30,))

    def test_reference_on_added(self):
        foo = Foo()
        foo.title = "Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo
        self.store.add(bar)

        self.assertEquals(bar.foo.id, None)
        self.assertEquals(bar.foo.title, "Title 40")


        self.store.flush()

        self.assertTrue(bar.foo.id)
        self.assertEquals(bar.foo.title, "Title 40")

        result = self.store.execute("SELECT foo.title FROM foo, bar "
                                    "WHERE bar.id=400 AND "
                                    "foo.id = bar.foo_id")
        self.assertEquals(result.get_one(), ("Title 40",))

    def test_reference_set_none(self):
        foo = Foo()
        foo.title = "Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo
        bar.foo = None
        bar.foo = None # Twice to make sure it doesn't blow up.
        self.store.add(bar)

        self.store.flush()

        self.assertEquals(type(bar.id), int)
        self.assertEquals(foo.id, None)

    def test_reference_on_added_composed_key(self):
        class Bar(object):
            __table__ = "bar", "id"
            id = Int()
            foo_id = Int()
            title = Unicode()
            foo = Reference((foo_id, title), (Foo.id, Foo.title))

        foo = Foo()
        foo.title = "Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.foo = foo
        self.store.add(bar)

        self.assertEquals(bar.foo.id, None)
        self.assertEquals(bar.foo.title, "Title 40")
        self.assertEquals(bar.title, "Title 40")

        self.store.flush()

        self.assertTrue(bar.foo.id)
        self.assertEquals(bar.foo.title, "Title 40")

        result = self.store.execute("SELECT foo.title FROM foo, bar "
                                    "WHERE bar.id=400 AND "
                                    "foo.id = bar.foo_id")
        self.assertEquals(result.get_one(), ("Title 40",))

    def test_reference_on_added_unlink_on_flush(self):
        foo = Foo()
        foo.title = "Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.foo = foo
        bar.title = "Title 400"
        self.store.add(bar)

        foo.id = 40
        self.assertEquals(bar.foo_id, 40)
        foo.id = 50
        self.assertEquals(bar.foo_id, 50)
        foo.id = 60
        self.assertEquals(bar.foo_id, 60)

        self.store.flush()

        foo.id = 70
        self.assertEquals(bar.foo_id, 60)

    def test_reference_on_added_unlink_on_flush(self):
        foo = Foo()
        foo.title = "Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo
        self.store.add(bar)

        foo.id = 40
        self.assertEquals(bar.foo_id, 40)
        foo.id = 50
        self.assertEquals(bar.foo_id, 50)
        foo.id = 60
        self.assertEquals(bar.foo_id, 60)

        self.store.flush()

        foo.id = 70
        self.assertEquals(bar.foo_id, 60)

    def test_reference_on_two_added(self):
        foo1 = Foo()
        foo1.title = "Title 40"
        foo2 = Foo()
        foo2.title = "Title 40"
        self.store.add(foo1)
        self.store.add(foo2)

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo1
        bar.foo = foo2
        self.store.add(bar)

        foo1.id = 40
        self.assertEquals(bar.foo_id, None)
        foo2.id = 50
        self.assertEquals(bar.foo_id, 50)

    def test_reference_on_added_and_changed_manually(self):
        foo = Foo()
        foo.title = "Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo
        self.store.add(bar)

        bar.foo_id = 40
        foo.id = 50
        self.assertEquals(bar.foo_id, 40)

    def test_reference_on_added_composed_key_changed_manually(self):
        class Bar(object):
            __table__ = "bar", "id"
            id = Int()
            foo_id = Int()
            title = Str()
            foo = Reference((foo_id, title), (Foo.id, Foo.title))

        foo = Foo()
        foo.title = "Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.foo = foo
        self.store.add(bar)

        bar.title = "Title 50"

        self.assertEquals(bar.foo, None)

        foo.id = 40

        self.assertEquals(bar.foo_id, None)

    def test_reference_on_added_no_local_store(self):
        foo = Foo()
        foo.title = "Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo

        self.assertEquals(Store.of(bar), self.store)
        self.assertEquals(Store.of(foo), self.store)

    def test_reference_on_added_no_remote_store(self):
        foo = Foo()
        foo.title = "Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        self.store.add(bar)

        bar.foo = foo

        self.assertEquals(Store.of(bar), self.store)
        self.assertEquals(Store.of(foo), self.store)

    def test_reference_on_added_no_store(self):
        foo = Foo()
        foo.title = "Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo

        self.store.add(bar)

        self.assertEquals(Store.of(bar), self.store)
        self.assertEquals(Store.of(foo), self.store)

        self.store.flush()

        self.assertEquals(type(bar.foo_id), int)

    def test_reference_on_added_no_store_2(self):
        foo = Foo()
        foo.title = "Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo

        self.store.add(foo)

        self.assertEquals(Store.of(bar), self.store)
        self.assertEquals(Store.of(foo), self.store)

        self.store.flush()

        self.assertEquals(type(bar.foo_id), int)

    def test_reference_on_added_wrong_store(self):
        store = Store(self.database)

        foo = Foo()
        foo.title = "Title 40"
        store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        self.store.add(bar)

        self.assertRaises(WrongStoreError, setattr, bar, "foo", foo)

    def test_reference_on_added_no_store_unlink_before_adding(self):
        foo1 = Foo()
        foo1.title = "Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"
        bar.foo = foo1
        bar.foo = None

        self.store.add(bar)

        store = Store(self.database)
        store.add(foo1)

        self.assertEquals(Store.of(bar), self.store)
        self.assertEquals(Store.of(foo1), store)

    def test_back_reference(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        foo = self.store.get(MyFoo, 10)
        self.assertTrue(foo.bar)
        self.assertEquals(foo.bar.title, "Title 300")

    def test_back_reference_setting(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = "Title 400"
        self.store.add(bar)

        foo = MyFoo()
        foo.bar = bar
        foo.title = "Title 40"
        self.store.add(foo)

        self.store.flush()

        self.assertTrue(foo.id)
        self.assertEquals(bar.foo_id, foo.id)

        result = self.store.execute("SELECT bar.title "
                                    "FROM foo, bar "
                                    "WHERE foo.id = bar.foo_id AND "
                                    "foo.title = 'Title 40'")
        self.assertEquals(result.get_one(), ("Title 400",))

    def test_back_reference_setting_changed_manually(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = "Title 400"
        self.store.add(bar)

        foo = MyFoo()
        foo.bar = bar
        foo.title = "Title 40"
        self.store.add(foo)

        foo.id = 40

        self.assertEquals(foo.bar, bar)

        self.store.flush()

        self.assertEquals(foo.id, 40)
        self.assertEquals(bar.foo_id, 40)

        result = self.store.execute("SELECT bar.title "
                                    "FROM foo, bar "
                                    "WHERE foo.id = bar.foo_id AND "
                                    "foo.title = 'Title 40'")
        self.assertEquals(result.get_one(), ("Title 400",))

    def test_back_reference_on_added_no_store(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = "Title 400"

        foo = MyFoo()
        foo.bar = bar
        foo.title = "Title 40"

        self.store.add(bar)

        self.assertEquals(Store.of(bar), self.store)
        self.assertEquals(Store.of(foo), self.store)

        self.store.flush()

        self.assertEquals(type(bar.foo_id), int)

    def test_back_reference_on_added_no_store_2(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = "Title 400"

        foo = MyFoo()
        foo.bar = bar
        foo.title = "Title 40"

        self.store.add(foo)

        self.assertEquals(Store.of(bar), self.store)
        self.assertEquals(Store.of(foo), self.store)

        self.store.flush()

        self.assertEquals(type(bar.foo_id), int)

    def add_reference_set_bar_400(self):
        bar = Bar()
        bar.id = 400
        bar.foo_id = 20
        bar.title = "Title 100"
        self.store.add(bar)

    def test_reference_set(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSet, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.foo_id, bar.title))
        items.sort()

        self.assertEquals(items, [
                          (200, 20, "Title 200"),
                          (400, 20, "Title 100"),
                         ])

    def test_reference_set_with_added(self):
        bar1 = Bar()
        bar1.id = 400
        bar1.title = "Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = "Title 500"

        foo = FooRefSet()
        foo.title = "Title 40"
        foo.bars.add(bar1)
        foo.bars.add(bar2)

        self.store.add(foo)

        self.assertEquals(foo.id, None)
        self.assertEquals(bar1.foo_id, None)
        self.assertEquals(bar2.foo_id, None)
        self.assertEquals(list(foo.bars.order_by(Bar.id)),
                          [bar1, bar2])
        self.assertEquals(type(foo.id), int)
        self.assertEquals(foo.id, bar1.foo_id)
        self.assertEquals(foo.id, bar2.foo_id)

    def test_reference_set_composed(self):
        self.add_reference_set_bar_400()

        bar = self.store.get(Bar, 400)
        bar.title = "Title 20"

        class FooRefSetComposed(Foo):
            bars = ReferenceSet((Foo.id, Foo.title),
                                (Bar.foo_id, Bar.title))

        foo = self.store.get(FooRefSetComposed, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.foo_id, bar.title))

        self.assertEquals(items, [
                          (400, 20, "Title 20"),
                         ])

        bar = self.store.get(Bar, 200)
        bar.title = "Title 20"

        del items[:]
        for bar in foo.bars:
            items.append((bar.id, bar.foo_id, bar.title))
        items.sort()

        self.assertEquals(items, [
                          (200, 20, "Title 20"),
                          (400, 20, "Title 20"),
                         ])

    def test_reference_set_find(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSet, 20)

        items = []
        for bar in foo.bars.find():
            items.append((bar.id, bar.foo_id, bar.title))
        items.sort()

        self.assertEquals(items, [
                          (200, 20, "Title 200"),
                          (400, 20, "Title 100"),
                         ])

        # Notice that there's another item with this title in the base,
        # which isn't part of the reference.

        objects = list(foo.bars.find(Bar.title == "Title 100"))
        self.assertEquals(len(objects), 1)
        self.assertTrue(objects[0] is bar)

        objects = list(foo.bars.find(title="Title 100"))
        self.assertEquals(len(objects), 1)
        self.assertTrue(objects[0] is bar)

    def test_reference_set_clear(self):
        foo = self.store.get(FooRefSet, 20)
        foo.bars.clear()
        self.assertEquals(list(foo.bars), [])

    def test_reference_set_clear_cached(self):
        foo = self.store.get(FooRefSet, 20)
        bar = self.store.get(Bar, 200)
        self.assertEquals(bar.foo_id, 20)
        foo.bars.clear()
        self.assertEquals(bar.foo_id, None)

    def test_reference_set_clear_where(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSet, 20)
        foo.bars.clear(Bar.id > 200)

        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        self.assertEquals(items, [
                          (200, 20, "Title 200"),
                         ])

        bar = self.store.get(Bar, 400)
        bar.foo_id = 20

        foo.bars.clear(id=200)

        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        self.assertEquals(items, [
                          (400, 20, "Title 100"),
                         ])

    def test_reference_set_count(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSet, 20)

        self.assertEquals(foo.bars.count(), 2)

    def test_reference_set_order_by(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSet, 20)

        items = []
        for bar in foo.bars.order_by(Bar.id):
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEquals(items, [
                          (200, 20, "Title 200"),
                          (400, 20, "Title 100"),
                         ])

        del items[:]
        for bar in foo.bars.order_by(Bar.title):
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEquals(items, [
                          (400, 20, "Title 100"),
                          (200, 20, "Title 200"),
                         ])

    def test_reference_set_default_order_by(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSetOrderID, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEquals(items, [
                          (200, 20, "Title 200"),
                          (400, 20, "Title 100"),
                         ])

        items = []
        for bar in foo.bars.find():
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEquals(items, [
                          (200, 20, "Title 200"),
                          (400, 20, "Title 100"),
                         ])

        foo = self.store.get(FooRefSetOrderTitle, 20)

        del items[:]
        for bar in foo.bars:
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEquals(items, [
                          (400, 20, "Title 100"),
                          (200, 20, "Title 200"),
                         ])

        del items[:]
        for bar in foo.bars.find():
            items.append((bar.id, bar.foo_id, bar.title))
        self.assertEquals(items, [
                          (400, 20, "Title 100"),
                          (200, 20, "Title 200"),
                         ])

    def test_reference_set_first_last(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSetOrderID, 20)
        self.assertEquals(foo.bars.first().id, 200)
        self.assertEquals(foo.bars.last().id, 400)

        foo = self.store.get(FooRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.first().id, 400)
        self.assertEquals(foo.bars.last().id, 200)

        foo = self.store.get(FooRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.first(Bar.id > 400), None)
        self.assertEquals(foo.bars.last(Bar.id > 400), None)

        foo = self.store.get(FooRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.first(Bar.id < 400).id, 200)
        self.assertEquals(foo.bars.last(Bar.id < 400).id, 200)

        foo = self.store.get(FooRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.first(id=200).id, 200)
        self.assertEquals(foo.bars.last(id=200).id, 200)

        foo = self.store.get(FooRefSet, 20)
        self.assertRaises(UnorderedError, foo.bars.first)
        self.assertRaises(UnorderedError, foo.bars.last)

    def test_reference_set_any(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSetOrderID, 20)
        self.assertEquals(foo.bars.any().id, 200)

        foo = self.store.get(FooRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.any().id, 400)

        foo = self.store.get(FooRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.any(Bar.id > 400), None)

        foo = self.store.get(FooRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.any(Bar.id < 400).id, 200)

        foo = self.store.get(FooRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.any(id=200).id, 200)

        foo = self.store.get(FooRefSet, 20)
        self.assertTrue(foo.bars.any().id in [200, 400])

    def test_reference_set_remove(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSet, 20)
        for bar in foo.bars:
            foo.bars.remove(bar)

        self.assertEquals(bar.foo_id, None)
        self.assertEquals(list(foo.bars), [])

    def test_reference_set_add(self):
        bar = Bar()
        bar.id = 400
        bar.title = "Title 100"

        foo = self.store.get(FooRefSet, 20)
        foo.bars.add(bar)

        self.assertEquals(bar.foo_id, 20)
        self.assertEquals(Store.of(bar), self.store)

    def test_reference_set_add_no_store(self):
        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"

        foo = FooRefSet()
        foo.title = "Title 40"
        foo.bars.add(bar)

        self.store.add(foo)

        self.assertEquals(Store.of(foo), self.store)
        self.assertEquals(Store.of(bar), self.store)

        self.store.flush()

        self.assertEquals(type(bar.foo_id), int)

    def test_reference_set_add_no_store_2(self):
        bar = Bar()
        bar.id = 400
        bar.title = "Title 400"

        foo = FooRefSet()
        foo.title = "Title 40"
        foo.bars.add(bar)

        self.store.add(bar)

        self.assertEquals(Store.of(foo), self.store)
        self.assertEquals(Store.of(bar), self.store)

        self.store.flush()

        self.assertEquals(type(bar.foo_id), int)

    def test_reference_set_add_no_store_unlink_after_adding(self):
        bar1 = Bar()
        bar1.id = 400
        bar1.title = "Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = "Title 500"

        foo = FooRefSet()
        foo.title = "Title 40"
        foo.bars.add(bar1)
        foo.bars.add(bar2)
        foo.bars.remove(bar1)

        self.store.add(foo)

        store = Store(self.database)
        store.add(bar1)

        self.assertEquals(Store.of(foo), self.store)
        self.assertEquals(Store.of(bar1), store)
        self.assertEquals(Store.of(bar2), self.store)

    def test_reference_set_values(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSetOrderID, 20)

        values = list(foo.bars.values(Bar.id, Bar.foo_id, Bar.title))
        self.assertEquals(values, [
                          (200, 20, "Title 200"),
                          (400, 20, "Title 100"),
                         ])
        

    def test_indirect_reference_set(self):
        foo = self.store.get(FooIndRefSet, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEquals(items, [
                          (100, "Title 300"),
                          (200, "Title 200"),
                         ])

    def test_indirect_reference_set_with_added(self):
        bar1 = Bar()
        bar1.id = 400
        bar1.title = "Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = "Title 500"
        self.store.add(bar1)
        self.store.add(bar2)

        foo = FooIndRefSet()
        foo.title = "Title 40"
        foo.bars.add(bar1)
        foo.bars.add(bar2)

        self.assertEquals(foo.id, None)

        self.store.add(foo)

        self.assertEquals(foo.id, None)
        self.assertEquals(bar1.foo_id, None)
        self.assertEquals(bar2.foo_id, None)
        self.assertEquals(list(foo.bars.order_by(Bar.id)),
                          [bar1, bar2])
        self.assertEquals(type(foo.id), int)
        self.assertEquals(type(bar1.id), int)
        self.assertEquals(type(bar2.id), int)

    def test_indirect_reference_set_find(self):
        foo = self.store.get(FooIndRefSet, 20)

        items = []
        for bar in foo.bars.find(Bar.title == "Title 300"):
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEquals(items, [
                          (100, "Title 300"),
                         ])

    def test_indirect_reference_set_clear(self):
        foo = self.store.get(FooIndRefSet, 20)
        foo.bars.clear()
        self.assertEquals(list(foo.bars), [])

    def test_indirect_reference_set_clear_where(self):
        foo = self.store.get(FooIndRefSet, 20)
        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        self.assertEquals(items, [
                          (100, 10, "Title 300"),
                          (200, 20, "Title 200"),
                         ])

        foo = self.store.get(FooIndRefSet, 30)
        foo.bars.clear(Bar.id < 300)
        foo.bars.clear(id=200)

        foo = self.store.get(FooIndRefSet, 20)
        foo.bars.clear(Bar.id < 200)

        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        self.assertEquals(items, [
                          (200, 20, "Title 200"),
                         ])

        foo.bars.clear(id=200)

        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        self.assertEquals(items, [])

    def test_indirect_reference_set_count(self):
        foo = self.store.get(FooIndRefSet, 20)
        self.assertEquals(foo.bars.count(), 2)

    def test_indirect_reference_set_order_by(self):
        foo = self.store.get(FooIndRefSet, 20)

        items = []
        for bar in foo.bars.order_by(Bar.title):
            items.append((bar.id, bar.title))

        self.assertEquals(items, [
                          (200, "Title 200"),
                          (100, "Title 300"),
                         ])

        del items[:]
        for bar in foo.bars.order_by(Bar.id):
            items.append((bar.id, bar.title))

        self.assertEquals(items, [
                          (100, "Title 300"),
                          (200, "Title 200"),
                         ])

    def test_indirect_reference_set_default_order_by(self):
        foo = self.store.get(FooIndRefSetOrderTitle, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        self.assertEquals(items, [
                          (200, "Title 200"),
                          (100, "Title 300"),
                         ])

        del items[:]
        for bar in foo.bars.find():
            items.append((bar.id, bar.title))
        self.assertEquals(items, [
                          (200, "Title 200"),
                          (100, "Title 300"),
                         ])

        foo = self.store.get(FooIndRefSetOrderID, 20)

        del items[:]
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        self.assertEquals(items, [
                          (100, "Title 300"),
                          (200, "Title 200"),
                         ])

        del items[:]
        for bar in foo.bars.find():
            items.append((bar.id, bar.title))
        self.assertEquals(items, [
                          (100, "Title 300"),
                          (200, "Title 200"),
                         ])

    def test_indirect_reference_set_first_last(self):
        foo = self.store.get(FooIndRefSetOrderID, 20)
        self.assertEquals(foo.bars.first().id, 100)
        self.assertEquals(foo.bars.last().id, 200)

        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.first().id, 200)
        self.assertEquals(foo.bars.last().id, 100)

        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.first(Bar.id > 200), None)
        self.assertEquals(foo.bars.last(Bar.id > 200), None)

        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.first(Bar.id < 200).id, 100)
        self.assertEquals(foo.bars.last(Bar.id < 200).id, 100)

        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.first(id=200).id, 200)
        self.assertEquals(foo.bars.last(id=200).id, 200)

        foo = self.store.get(FooIndRefSet, 20)
        self.assertRaises(UnorderedError, foo.bars.first)
        self.assertRaises(UnorderedError, foo.bars.last)

    def test_indirect_reference_set_any(self):
        foo = self.store.get(FooIndRefSetOrderID, 20)
        self.assertEquals(foo.bars.any().id, 100)

        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.any().id, 200)

        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.any(Bar.id > 200), None)

        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.any(Bar.id < 200).id, 100)

        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        self.assertEquals(foo.bars.any(id=200).id, 200)

        foo = self.store.get(FooIndRefSet, 20)
        self.assertTrue(foo.bars.any().id in [100, 200])

    def test_indirect_reference_set_add(self):
        foo = self.store.get(FooIndRefSet, 20)
        bar = self.store.get(Bar, 300)

        foo.bars.add(bar)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEquals(items, [
                          (100, "Title 300"),
                          (200, "Title 200"),
                          (300, "Title 100"),
                         ])

    def test_indirect_reference_set_remove(self):
        foo = self.store.get(FooIndRefSet, 20)
        bar = self.store.get(Bar, 200)

        foo.bars.remove(bar)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEquals(items, [
                          (100, "Title 300"),
                         ])

    def test_indirect_reference_set_add_remove(self):
        foo = self.store.get(FooIndRefSet, 20)
        bar = self.store.get(Bar, 300)

        foo.bars.add(bar)
        foo.bars.remove(bar)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEquals(items, [
                          (100, "Title 300"),
                          (200, "Title 200"),
                         ])

    def test_indirect_reference_set_add_remove_with_added(self):
        foo = FooIndRefSet()
        foo.id = 40
        bar1 = Bar()
        bar1.id = 400
        bar1.title = "Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = "Title 500"
        self.store.add(foo)
        self.store.add(bar1)
        self.store.add(bar2)

        foo.bars.add(bar1)
        foo.bars.add(bar2)
        foo.bars.remove(bar1)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEquals(items, [
                          (500, "Title 500"),
                         ])

    def test_indirect_reference_set_with_added_no_store(self):
        bar1 = Bar()
        bar1.id = 400
        bar1.title = "Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = "Title 500"

        foo = FooIndRefSet()
        foo.title = "Title 40"

        foo.bars.add(bar1)
        foo.bars.add(bar2)

        self.store.add(bar1)

        self.assertEquals(Store.of(foo), self.store)
        self.assertEquals(Store.of(bar1), self.store)
        self.assertEquals(Store.of(bar2), self.store)

        self.assertEquals(foo.id, None)
        self.assertEquals(bar1.foo_id, None)
        self.assertEquals(bar2.foo_id, None)

        self.assertEquals(list(foo.bars.order_by(Bar.id)),
                          [bar1, bar2])

    def test_indirect_reference_set_values(self):
        foo = self.store.get(FooIndRefSetOrderID, 20)

        values = list(foo.bars.values(Bar.id, Bar.foo_id, Bar.title))
        self.assertEquals(values, [
                          (100, 10, "Title 300"),
                          (200, 20, "Title 200"),
                         ])

    def test_references_raise_nostore(self):
        foo1 = FooRefSet()
        foo2 = FooIndRefSet()

        self.assertRaises(NoStoreError, foo1.bars.__iter__)
        self.assertRaises(NoStoreError, foo2.bars.__iter__)
        self.assertRaises(NoStoreError, foo1.bars.find)
        self.assertRaises(NoStoreError, foo2.bars.find)
        self.assertRaises(NoStoreError, foo1.bars.order_by)
        self.assertRaises(NoStoreError, foo2.bars.order_by)
        self.assertRaises(NoStoreError, foo1.bars.count)
        self.assertRaises(NoStoreError, foo2.bars.count)
        self.assertRaises(NoStoreError, foo1.bars.clear)
        self.assertRaises(NoStoreError, foo2.bars.clear)
        self.assertRaises(NoStoreError, foo2.bars.remove, object())

    def test_flush_order(self):
        foo1 = Foo()
        foo2 = Foo()
        foo3 = Foo()
        foo4 = Foo()
        foo5 = Foo()

        for i, foo in enumerate([foo1, foo2, foo3, foo4, foo5]):
            foo.title = "Object %d" % (i+1)
            self.store.add(foo)

        self.store.add_flush_order(foo2, foo4)
        self.store.add_flush_order(foo4, foo1)
        self.store.add_flush_order(foo1, foo3)
        self.store.add_flush_order(foo3, foo5)
        self.store.add_flush_order(foo5, foo2)
        self.store.add_flush_order(foo5, foo2)

        self.assertRaises(OrderLoopError, self.store.flush)

        self.store.remove_flush_order(foo5, foo2)

        self.assertRaises(OrderLoopError, self.store.flush)

        self.store.remove_flush_order(foo5, foo2)

        self.store.flush()

        self.assertTrue(foo2.id < foo4.id)
        self.assertTrue(foo4.id < foo1.id)
        self.assertTrue(foo1.id < foo3.id)
        self.assertTrue(foo3.id < foo5.id)

    def test_variable_filter_on_load(self):
        foo = self.store.get(FooVariable, 20)
        self.assertEquals(foo.title, "to_py(from_db(Title 20))")

    def test_variable_filter_on_update(self):
        foo = self.store.get(FooVariable, 20)
        foo.title = "Title 20"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "to_db(from_py(Title 20))"),
                          (30, "Title 10"),
                         ])

    def test_variable_filter_on_update_unchanged(self):
        foo = self.store.get(FooVariable, 20)
        self.store.flush()
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_variable_filter_on_insert(self):
        foo = FooVariable()
        foo.id = 40
        foo.title = "Title 40"

        self.store.add(foo)
        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                          (40, "to_db(from_py(Title 40))"),
                         ])

    def test_variable_filter_on_missing_values(self):
        foo = FooVariable()
        foo.id = 40

        self.store.add(foo)
        self.store.flush()

        self.assertEquals(foo.title, "to_py(from_db(Default Title))")

    def test_variable_filter_on_set(self):
        foo = FooVariable()
        self.store.find(FooVariable, id=20).set(title="Title 20")

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "to_db(from_py(Title 20))"),
                          (30, "Title 10"),
                         ])

    def test_variable_filter_on_set_expr(self):
        foo = FooVariable()
        result = self.store.find(FooVariable, id=20)
        result.set(FooVariable.title == "Title 20")

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "to_db(from_py(Title 20))"),
                          (30, "Title 10"),
                         ])

    def test_wb_result_set_variable(self):
        Result = self.store._connection._result_factory

        class MyResult(Result):
            def set_variable(self, variable, value):
                if variable.__class__ is UnicodeVariable:
                    variable.set(u"set_variable(%s)" % value)
                elif variable.__class__ is IntVariable:
                    variable.set(value+1)
                else:
                    variable.set(value)

        self.store._connection._result_factory = MyResult
        try:
            foo = self.store.get(Foo, 20)
        finally:
            self.store._connection._result_factory = Result

        self.assertEquals(foo.id, 21)
        self.assertEquals(foo.title, "set_variable(Title 20)")

    def test_default(self):
        class MyFoo(Foo):
            title = Str(default="Some default value")

        foo = MyFoo()
        self.store.add(foo)
        self.store.flush()

        result = self.store.execute("SELECT title FROM foo WHERE id=?",
                                    (foo.id,))
        self.assertEquals(result.get_one(), ("Some default value",))

        self.assertEquals(foo.title, "Some default value")

    def test_default_factory(self):
        class MyFoo(Foo):
            title = Str(default_factory=lambda:"Some default value")

        foo = MyFoo()
        self.store.add(foo)
        self.store.flush()

        result = self.store.execute("SELECT title FROM foo WHERE id=?",
                                    (foo.id,))
        self.assertEquals(result.get_one(), ("Some default value",))

        self.assertEquals(foo.title, "Some default value")

    def test_pickle_kind(self):
        class MyBlob(Blob):
            bin = Pickle()

        blob = self.store.get(Blob, 20)
        blob.bin = "\x80\x02}q\x01U\x01aK\x01s."
        self.store.flush()

        blob = self.store.get(MyBlob, 20)
        self.assertEquals(blob.bin["a"], 1)

    def test_unhashable_object(self):

        class DictFoo(Foo, dict):
            pass

        foo = self.store.get(DictFoo, 20)
        foo["a"] = 1

        self.assertEquals(foo.items(), [("a", 1)])

        new_obj = DictFoo()
        new_obj.id = 40
        new_obj.title = "My Title"

        self.store.add(new_obj)
        self.store.commit()

        self.assertTrue(self.store.get(DictFoo, 40) is new_obj)

    def test_proxy(self):
        foo = self.store.get(Foo, 20)
        proxy = Proxy(foo)
        self.store.remove(proxy)
        self.store.flush()
        self.assertEquals(self.store.get(Foo, 20), None)

    def test_rollback_loaded_and_still_in_cached(self):
        # Explore problem found on interaction between caching, commits,
        # and rollbacks.
        foo1 = self.store.get(Foo, 20)
        self.store.commit()
        self.store.rollback()
        foo2 = self.store.get(Foo, 20)
        self.assertTrue(foo1 is foo2)
