# -*- coding: utf-8 -*-
#
# Copyright (c) 2006, 2007 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# Storm is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 2.1 of
# the License, or (at your option) any later version.
#
# Storm is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
import decimal
import gc

from storm.references import Reference, ReferenceSet, Proxy
from storm.database import Result
from storm.properties import Int, Float, RawStr, Unicode, Property, Pickle
from storm.properties import PropertyPublisherMeta, Decimal
from storm.expr import Asc, Desc, Select, Func, LeftJoin, SQL
from storm.variables import Variable, UnicodeVariable, IntVariable
from storm.info import get_obj_info, ClassAlias
from storm.exceptions import *
from storm.store import *

from tests.info import Wrapper
from tests.helper import run_this


class Foo(object):
    __storm_table__ = "foo"
    id = Int(primary=True)
    title = Unicode()

class Bar(object):
    __storm_table__ = "bar"
    id = Int(primary=True)
    title = Unicode()
    foo_id = Int()
    foo = Reference(foo_id, Foo.id)

class Blob(object):
    __storm_table__ = "bin"
    id = Int(primary=True)
    bin = RawStr()

class Link(object):
    __storm_table__ = "link"
    __storm_primary__ = "foo_id", "bar_id"
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

class BarProxy(object):
    __storm_table__ = "bar"
    id = Int(primary=True)
    title = Unicode()
    foo_id = Int()
    foo = Reference(foo_id, Foo.id)
    foo_title = Proxy(foo, Foo.title)

class Money(object):
    __storm_table__ = "money"
    id = Int(primary=True)
    value = Decimal()


class DecorateVariable(Variable):

    def parse_get(self, value, to_db):
        return u"to_%s(%s)" % (to_db and "db" or "py", value)

    def parse_set(self, value, from_db):
        return u"from_%s(%s)" % (from_db and "db" or "py", value)


class FooVariable(Foo):
    title = Property(variable_class=DecorateVariable)


class StoreTest(object):

    def setUp(self):
        self.store = None
        self.stores = []
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
        connection.execute("INSERT INTO money VALUES (10, '12.3455')")

        connection.commit()

    def create_store(self):
        store = Store(self.database)
        self.stores.append(store)
        if self.store is None:
            self.store = store
        return store

    def drop_store(self):
        for store in self.stores:
            store.rollback()

            # Closing the store is needed because testcase objects are all
            # instantiated at once, and thus connections are kept open.
            store.close()

    def drop_sample_data(self):
        pass

    def drop_tables(self):
        for table in ["foo", "bar", "bin", "link", "money"]:
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

    def get_cache(self, store):
        # We don't offer a public API for this just yet.
        return store._cache

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
        foo.title = u"New Title"

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
        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        foo = self.store.get(Foo, 10)
        foo.taint = True

        del foo
        gc.collect()

        foo = self.store.get(Foo, 10)
        self.assertFalse(getattr(foo, "taint", False))

    def test_add_returns_object(self):
        """
        Store.add() returns the object passed to it.  This allows this
        kind of code:

        thing = Thing()
        store.add(thing)
        return thing

        to be simplified as:

        return store.add(Thing())
        """
        foo = Foo()
        self.assertEquals(self.store.add(foo), foo)

    def test_add_and_stop_referencing(self):
        # After adding an object, no references should be needed in
        # python for it still to be added to the database.
        foo = Foo()
        foo.title = u"live"
        self.store.add(foo)

        del foo
        gc.collect()

        self.assertTrue(self.store.find(Foo, title=u"live").one())

    def test_obj_info_with_deleted_object(self):
        # Let's try to put Storm in trouble by killing the object
        # while still holding a reference to the obj_info.

        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        class MyFoo(Foo):
            loaded = False
            def __storm_loaded__(self):
                self.loaded = True

        foo = self.store.get(MyFoo, 20)
        foo.tainted = True
        obj_info = get_obj_info(foo)

        del foo
        gc.collect()

        self.assertEquals(obj_info.get_obj(), None)

        foo = self.store.find(MyFoo, id=20).one()
        self.assertTrue(foo)
        self.assertFalse(getattr(foo, "tainted", False))

        # The object was rebuilt, so the loaded hook must have run.
        self.assertTrue(foo.loaded)

    def test_obj_info_with_deleted_object_with_get(self):
        # Same thing, but using get rather than find.

        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        foo = self.store.get(Foo, 20)
        foo.tainted = True
        obj_info = get_obj_info(foo)

        del foo
        gc.collect()

        self.assertEquals(obj_info.get_obj(), None)

        foo = self.store.get(Foo, 20)
        self.assertTrue(foo)
        self.assertFalse(getattr(foo, "tainted", False))

    def test_delete_object_when_obj_info_is_dirty(self):
        """Object should stay in memory if dirty."""

        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        foo = self.store.get(Foo, 20)
        foo.title = u"Changed"
        foo.tainted = True
        obj_info = get_obj_info(foo)

        del foo
        gc.collect()

        self.assertTrue(obj_info.get_obj())

    def test_get_tuple(self):
        class MyFoo(Foo):
            __storm_primary__ = "title", "id"
        foo = self.store.get(MyFoo, (u"Title 30", 10))
        self.assertEquals(foo.id, 10)
        self.assertEquals(foo.title, "Title 30")

        foo = self.store.get(MyFoo, (u"Title 20", 10))
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
                                 Foo.title == u"Title 20")
        self.assertEquals([(foo.id, foo.title) for foo in result], [
                          (20, "Title 20"),
                         ])

        result = self.store.find(Foo, Foo.id == 10,
                                 Foo.title == u"Title 20")
        self.assertEquals([(foo.id, foo.title) for foo in result], [
                         ])

    def test_find_sql(self):
        foo = self.store.find(Foo, SQL("foo.id = 20")).one()
        self.assertEquals(foo.title, "Title 20")

    def test_find_str(self):
        foo = self.store.find(Foo, "foo.id = 20").one()
        self.assertEquals(foo.title, "Title 20")

    def test_find_keywords(self):
        result = self.store.find(Foo, id=20, title=u"Title 20")
        self.assertEquals([(foo.id, foo.title) for foo in result], [
                          (20, u"Title 20")
                         ])

        result = self.store.find(Foo, id=10, title=u"Title 20")
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

    def test_find_default_order_asc(self):
        class MyFoo(Foo):
            __storm_order__ = "title"

        result = self.store.find(MyFoo)
        lst = [(foo.id, foo.title) for foo in result]
        self.assertEquals(lst, [
                          (30, "Title 10"),
                          (20, "Title 20"),
                          (10, "Title 30"),
                         ])

    def test_find_default_order_desc(self):
        class MyFoo(Foo):
            __storm_order__ = "-title"

        result = self.store.find(MyFoo)
        lst = [(foo.id, foo.title) for foo in result]
        self.assertEquals(lst, [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_find_default_order_with_tuple(self):
        class MyLink(Link):
            __storm_order__ = ("foo_id", "-bar_id")

        result = self.store.find(MyLink)
        lst = [(link.foo_id, link.bar_id) for link in result]
        self.assertEquals(lst, [
                          (10, 300),
                          (10, 200),
                          (10, 100),
                          (20, 200),
                          (20, 100),
                          (30, 300),
                         ])

    def test_find_default_order_with_tuple_and_expr(self):
        class MyLink(Link):
            __storm_order__ = ("foo_id", Desc(Link.bar_id))

        result = self.store.find(MyLink)
        lst = [(link.foo_id, link.bar_id) for link in result]
        self.assertEquals(lst, [
                          (10, 300),
                          (10, 200),
                          (10, 100),
                          (20, 200),
                          (20, 100),
                          (30, 300),
                         ])

    def test_find__add_select_also(self):
        result = self.store.find(Foo)
        result.config(distinct=True)
        result._add_select_also(SQL('lower(foo.title) AS lower_title'))
        result.order_by('lower_title')
        lst = [foo.title for foo in result]
        self.assertEquals(lst, ["Title 10", "Title 20", "Title 30"])

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
        self.assertRaises(FeatureError, result.last)

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
        self.assertRaises(FeatureError, result.order_by, None)

        result = self.store.find(Foo)[:2]
        self.assertRaises(FeatureError, result.order_by, None)

    def test_find_slice_remove(self):
        result = self.store.find(Foo)[2:]
        self.assertRaises(FeatureError, result.remove)

        result = self.store.find(Foo)[:2]
        self.assertRaises(FeatureError, result.remove)

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

    def test_find_count_column(self):
        self.assertEquals(self.store.find(Link).count(Link.foo_id), 6)

    def test_find_count_column_distinct(self):
        count = self.store.find(Link).count(Link.foo_id, distinct=True)
        self.assertEquals(count, 3)

    def test_find_max(self):
        self.assertEquals(self.store.find(Foo).max(Foo.id), 30)

    def test_find_max_expr(self):
        self.assertEquals(self.store.find(Foo).max(Foo.id + 1), 31)

    def test_find_max_unicode(self):
        title = self.store.find(Foo).max(Foo.title)
        self.assertEquals(title, "Title 30")
        self.assertTrue(isinstance(title, unicode))

    def test_find_min(self):
        self.assertEquals(self.store.find(Foo).min(Foo.id), 10)

    def test_find_min_expr(self):
        self.assertEquals(self.store.find(Foo).min(Foo.id - 1), 9)

    def test_find_min_unicode(self):
        title = self.store.find(Foo).min(Foo.title)
        self.assertEquals(title, "Title 10")
        self.assertTrue(isinstance(title, unicode))

    def test_find_avg(self):
        self.assertEquals(self.store.find(Foo).avg(Foo.id), 20)

    def test_find_avg_expr(self):
        self.assertEquals(self.store.find(Foo).avg(Foo.id + 10), 30)

    def test_find_avg_float(self):
        foo = Foo()
        foo.id = 15
        foo.title = u"Title 15"
        self.store.add(foo)
        self.assertEquals(self.store.find(Foo).avg(Foo.id), 18.75)

    def test_find_sum(self):
        self.assertEquals(self.store.find(Foo).sum(Foo.id), 60)

    def test_find_sum_expr(self):
        self.assertEquals(self.store.find(Foo).sum(Foo.id * 2), 120)

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
        self.assertRaises(FeatureError, result.values().next)

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
        self.assertEquals(self.store.find(Foo, title=u"Title 20").cached(),
                          [foo2])

    def test_find_cached_invalidated(self):
        foo = self.store.get(Foo, 20)
        self.store.invalidate(foo)
        self.assertEquals(self.store.find(Foo).cached(), [foo])

    def test_find_cached_invalidated_and_deleted(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("DELETE FROM foo WHERE id=20")
        self.store.invalidate(foo)
        # Do not look for the primary key (id), since it's able to get
        # it without touching the database. Use the title instead.
        self.assertEquals(self.store.find(Foo, title=u"Title 20").cached(), [])

    def test_find_cached_with_info_alive_and_object_dead(self):
        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        foo = self.store.get(Foo, 20)
        foo.tainted = True
        obj_info = get_obj_info(foo)
        del foo
        gc.collect()
        cached = self.store.find(Foo).cached()
        self.assertEquals(len(cached), 1)
        foo = self.store.get(Foo, 20)
        self.assertFalse(hasattr(foo, "tainted"))

    def test_using_find_join(self):
        bar = self.store.get(Bar, 100)
        bar.foo_id = None

        tables = self.store.using(Foo, LeftJoin(Bar, Bar.foo_id == Foo.id))
        result = tables.find(Bar).order_by(Foo.id, Bar.id)
        lst = [bar and (bar.id, bar.title) for bar in result]
        self.assertEquals(lst, [
                          None,
                          (200, u"Title 200"),
                          (300, u"Title 100"),
                         ])

    def test_using_find_with_strings(self):
        foo = self.store.using("foo").find(Foo, id=10).one()
        self.assertEquals(foo.title, "Title 30")

        foo = self.store.using("foo", "bar").find(Foo, id=10).any()
        self.assertEquals(foo.title, "Title 30")

    def test_using_find_join_with_strings(self):
        bar = self.store.get(Bar, 100)
        bar.foo_id = None

        tables = self.store.using(LeftJoin("foo", "bar",
                                           "bar.foo_id = foo.id"))
        result = tables.find(Bar).order_by(Foo.id, Bar.id)
        lst = [bar and (bar.id, bar.title) for bar in result]
        self.assertEquals(lst, [
                          None,
                          (200, u"Title 200"),
                          (300, u"Title 100"),
                         ])

    def test_find_tuple(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None

        result = self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        result = result.order_by(Foo.id)
        lst = [(foo and (foo.id, foo.title), bar and (bar.id, bar.title))
               for (foo, bar) in result]
        self.assertEquals(lst, [
                          ((10, u"Title 30"), (100, u"Title 300")),
                          ((30, u"Title 10"), (300, u"Title 100")),
                         ])

    def test_find_tuple_using(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None

        tables = self.store.using(Foo, LeftJoin(Bar, Bar.foo_id == Foo.id))
        result = tables.find((Foo, Bar)).order_by(Foo.id)
        lst = [(foo and (foo.id, foo.title), bar and (bar.id, bar.title))
               for (foo, bar) in result]
        self.assertEquals(lst, [
                          ((10, u"Title 30"), (100, u"Title 300")),
                          ((20, u"Title 20"), None),
                          ((30, u"Title 10"), (300, u"Title 100")),
                         ])

    def test_find_tuple_using_skip_when_none(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None

        tables = self.store.using(Foo,
                                  LeftJoin(Bar, Bar.foo_id == Foo.id),
                                  LeftJoin(Link, Link.bar_id == Bar.id))
        result = tables.find((Bar, Link)).order_by(Foo.id, Bar.id, Link.foo_id)
        lst = [(bar and (bar.id, bar.title),
                link and (link.bar_id, link.foo_id))
               for (bar, link) in result]
        self.assertEquals(lst, [
                          ((100, u"Title 300"), (100, 10)),
                          ((100, u"Title 300"), (100, 20)),
                          (None, None),
                          ((300, u"Title 100"), (300, 10)),
                          ((300, u"Title 100"), (300, 30)),
                         ])

    def test_find_tuple_any(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None

        result = self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        foo, bar = result.order_by(Foo.id).any()
        self.assertEquals(foo.id, 10)
        self.assertEquals(foo.title, u"Title 30")
        self.assertEquals(bar.id, 100)
        self.assertEquals(bar.title, u"Title 300")

    def test_find_tuple_first(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None

        result = self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        foo, bar = result.order_by(Foo.id).first()
        self.assertEquals(foo.id, 10)
        self.assertEquals(foo.title, u"Title 30")
        self.assertEquals(bar.id, 100)
        self.assertEquals(bar.title, u"Title 300")

    def test_find_tuple_last(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None

        result = self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        foo, bar = result.order_by(Foo.id).last()
        self.assertEquals(foo.id, 30)
        self.assertEquals(foo.title, u"Title 10")
        self.assertEquals(bar.id, 300)
        self.assertEquals(bar.title, u"Title 100")

    def test_find_tuple_first(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None

        result = self.store.find((Foo, Bar),
                                 Bar.foo_id == Foo.id, Foo.id == 10)
        foo, bar = result.order_by(Foo.id).one()
        self.assertEquals(foo.id, 10)
        self.assertEquals(foo.title, u"Title 30")
        self.assertEquals(bar.id, 100)
        self.assertEquals(bar.title, u"Title 300")

    def test_find_tuple_count(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None
        result = self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        self.assertEquals(result.count(), 2)

    def test_find_tuple_remove(self):
        result = self.store.find((Foo, Bar))
        self.assertRaises(FeatureError, result.remove)

    def test_find_tuple_set(self):
        result = self.store.find((Foo, Bar))
        self.assertRaises(FeatureError, result.set, title=u"Title 40")

    def test_find_tuple_kwargs(self):
        self.assertRaises(FeatureError,
                          self.store.find, (Foo, Bar), title=u"Title 10")

    def test_find_tuple_cached(self):
        result = self.store.find((Foo, Bar))
        self.assertRaises(FeatureError, result.cached)

    def test_find_expr(self):
        result = self.store.find(Foo.title)
        self.assertEquals(sorted(result),
                          [u"Title 10", u"Title 20", u"Title 30"])

    def test_find_tuple_expr(self):
        result = self.store.find((Foo, Bar.title), Bar.foo_id == Foo.id)
        result.order_by(Foo.id)
        self.assertEquals([(foo.id, foo.title, bar_title)
                           for foo, bar_title in result],
                           [(10, u'Title 30', u'Title 300'),
                            (20, u'Title 20', u'Title 200'),
                            (30, u'Title 10', u'Title 100')])

    def test_find_using_cached(self):
        result = self.store.using(Foo, Bar).find(Foo)
        self.assertRaises(FeatureError, result.cached)

    def test_get_does_not_validate(self):
        def validator(object, attr, value):
            self.fail("validator called with arguments (%r, %r, %r)" %
                      (object, attr, value))

        class Foo(object):
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode(validator=validator)

        foo = self.store.get(Foo, 10)
        self.assertEqual(foo.title, "Title 30")

    def test_get_does_not_validate_default_value(self):
        def validator(object, attr, value):
            self.fail("validator called with arguments (%r, %r, %r)" %
                      (object, attr, value))

        class Foo(object):
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode(validator=validator, default=u"default value")

        foo = self.store.get(Foo, 10)
        self.assertEqual(foo.title, "Title 30")

    def test_find_does_not_validate(self):
        def validator(object, attr, value):
            self.fail("validator called with arguments (%r, %r, %r)" %
                      (object, attr, value))

        class Foo(object):
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode(validator=validator)

        foo = self.store.find(Foo, Foo.id == 10).one()
        self.assertEqual(foo.title, "Title 30")

    def test_add_commit(self):
        foo = Foo()
        foo.id = 40
        foo.title = u"Title 40"

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
        foo.title = u"Title 40"

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
        foo.title = u"Title 40"

        self.store.add(foo)

        old_foo = foo

        foo = self.store.get(Foo, 40)

        self.assertEquals(foo.id, 40)
        self.assertEquals(foo.title, "Title 40")

        self.assertTrue(foo is old_foo)

    def test_add_find(self):
        foo = Foo()
        foo.id = 40
        foo.title = u"Title 40"

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
        bar.title = u"Title 400"
        bar.foo_id = 40

        self.store.flush()
        self.store.execute("UPDATE bar SET title='Title 500' "
                           "WHERE id=400")
        bar.foo_id = 400

        # When not checkpointing, this flush will set title again.
        self.store.flush()
        self.store.reload(bar)
        self.assertEquals(bar.title, "Title 500")

    def test_add_completely_undefined(self):
        foo = Foo()
        self.store.add(foo)
        self.store.flush()
        self.assertEquals(type(foo.id), int)
        self.assertEquals(foo.title, u"Default Title")

    def test_remove_commit(self):
        foo = self.store.get(Foo, 20)
        self.store.remove(foo)
        self.assertEquals(Store.of(foo), self.store)
        self.store.flush()
        self.assertEquals(Store.of(foo), None)

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

        foo.title = u"Title 200"

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

        foo.title = u"Title 200"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_remove_add_update(self):
        foo = self.store.get(Foo, 20)

        self.store.remove(foo)
        self.store.add(foo)

        foo.title = u"Title 200"

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

        foo.title = u"Title 200"

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

        foo.title = u"Title 200"

        self.assertTrue(obj_info not in self.store._dirty)

    def test_wb_remove_rollback_isnt_dirty(self):
        foo = self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        self.store.remove(foo)
        self.store.rollback()

        self.assertTrue(obj_info not in self.store._dirty)

    def test_wb_remove_flush_rollback_isnt_dirty(self):
        foo = self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        self.store.remove(foo)
        self.store.flush()
        self.store.rollback()

        self.assertTrue(obj_info not in self.store._dirty)

    def test_add_rollback_not_in_store(self):
        foo = Foo()
        foo.id = 40
        foo.title = u"Title 40"

        self.store.add(foo)
        self.store.rollback()

        self.assertEquals(Store.of(foo), None)

    def test_update_flush_commit(self):
        foo = self.store.get(Foo, 20)
        foo.title = u"Title 200"

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
        foo.title = u"Title 200"
        self.store.flush()
        self.store.reload(foo)
        self.store.rollback()
        self.assertEquals(foo.title, "Title 20")

    def test_update_commit(self):
        foo = self.store.get(Foo, 20)
        foo.title = u"Title 200"

        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_update_commit_twice(self):
        foo = self.store.get(Foo, 20)
        foo.title = u"Title 200"
        self.store.commit()
        foo.title = u"Title 2000"
        self.store.commit()

        self.assertEquals(self.get_committed_items(), [
                          (10, "Title 30"),
                          (20, "Title 2000"),
                          (30, "Title 10"),
                         ])

    def test_update_checkpoints(self):
        bar = self.store.get(Bar, 200)
        bar.title = u"Title 400"
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
        foo.title = u"Title 200"

        self.store.flush()

        # If changes get committed even with the notification disabled,
        # it means the dirty flag isn't being cleared.

        self.store._disable_change_notification(get_obj_info(foo))

        foo.title = u"Title 2000"

        self.store.flush()

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 200"),
                          (30, "Title 10"),
                         ])

    def test_update_find(self):
        foo = self.store.get(Foo, 20)
        foo.title = u"Title 200"

        result = self.store.find(Foo, Foo.title == u"Title 200")

        self.assertTrue(result.one() is foo)

    def test_update_get(self):
        foo = self.store.get(Foo, 20)
        foo.id = 200

        self.assertTrue(self.store.get(Foo, 200) is foo)

    def test_add_update(self):
        foo = Foo()
        foo.id = 40
        foo.title = u"Title 40"

        self.store.add(foo)

        foo.title = u"Title 400"

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
        foo.title = u"Title 40"

        self.store.add(foo)
        self.store.remove(foo)

        self.assertEquals(Store.of(foo), None)

        foo.title = u"Title 400"

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
        foo.title = u"Title 200"

        obj_info = get_obj_info(foo)

        self.store.remove(foo)
        self.store.add(foo)

        self.assertTrue(obj_info in self.store._dirty)

    def test_commit_autoreloads(self):
        foo = self.store.get(Foo, 20)
        self.assertEquals(foo.title, "Title 20")
        self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.assertEquals(foo.title, "Title 20")
        self.store.commit()
        self.assertEquals(foo.title, "New Title")

    def test_commit_invalidates(self):
        foo = self.store.get(Foo, 20)
        self.assertTrue(foo)
        self.store.execute("DELETE FROM foo WHERE id=20")
        self.assertEquals(self.store.get(Foo, 20), foo)
        self.store.commit()
        self.assertEquals(self.store.get(Foo, 20), None)

    def test_rollback_autoreloads(self):
        foo = self.store.get(Foo, 20)
        self.assertEquals(foo.title, "Title 20")
        self.store.rollback()
        self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.assertEquals(foo.title, "New Title")

    def test_rollback_invalidates(self):
        foo = self.store.get(Foo, 20)
        self.assertTrue(foo)
        self.assertEquals(self.store.get(Foo, 20), foo)
        self.store.rollback()
        self.store.execute("DELETE FROM foo WHERE id=20")
        self.assertEquals(self.store.get(Foo, 20), None)

    def test_sub_class(self):
        class SubFoo(Foo):
            id = Float(primary=True)

        foo1 = self.store.get(Foo, 20)
        foo2 = self.store.get(SubFoo, 20)

        self.assertEquals(foo1.id, 20)
        self.assertEquals(foo2.id, 20)
        self.assertEquals(type(foo1.id), int)
        self.assertEquals(type(foo2.id), float)

    def test_join(self):

        class Bar(object):
            __storm_table__ = "bar"
            id = Int(primary=True)
            title = Unicode()

        bar = Bar()
        bar.id = 40
        bar.title = u"Title 20"

        self.store.add(bar)

        # Add anbar object with the same title to ensure DISTINCT
        # is in place.

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 20"

        self.store.add(bar)

        result = self.store.find(Foo, Foo.title == Bar.title)

        self.assertEquals([(foo.id, foo.title) for foo in result], [
                          (20, "Title 20"),
                          (20, "Title 20"),
                         ])

    def test_join_distinct(self):

        class Bar(object):
            __storm_table__ = "bar"
            id = Int(primary=True)
            title = Unicode()

        bar = Bar()
        bar.id = 40
        bar.title = u"Title 20"

        self.store.add(bar)

        # Add a bar object with the same title to ensure DISTINCT
        # is in place.

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 20"

        self.store.add(bar)

        result = self.store.find(Foo, Foo.title == Bar.title)
        result.config(distinct=True)

        # Make sure that it won't unset it, and that it's returning itself.
        config = result.config()

        self.assertEquals([(foo.id, foo.title) for foo in result], [
                          (20, "Title 20"),
                         ])

    def test_sub_select(self):
        foo = self.store.find(Foo, Foo.id == Select(SQL("20"))).one()
        self.assertTrue(foo)
        self.assertEquals(foo.id, 20)
        self.assertEquals(foo.title, "Title 20")

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
        foo.title = u"Readded"
        self.store.add(foo)

        self.store.commit()

        self.assertTrue(self.store.get(Foo, 20) is foo)

    def test_loaded_hook(self):

        loaded = []

        class MyFoo(Foo):
            def __init__(self):
                loaded.append("NO!")
            def __storm_loaded__(self):
                loaded.append((self.id, self.title))
                self.title = u"Title 200"
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
        self.assertEquals(foo.some_attribute, 2)

    def test_flush_hook(self):

        class MyFoo(Foo):
            counter = 0
            def __storm_pre_flush__(self):
                if self.counter == 0:
                    self.title = u"Flushing: %s" % self.title
                self.counter += 1

        foo = self.store.get(MyFoo, 20)

        self.assertEquals(foo.title, "Title 20")
        self.store.flush()
        self.assertEquals(foo.title, "Title 20") # It wasn't dirty.
        foo.title = u"Something"
        self.store.flush()
        self.assertEquals(foo.title, "Flushing: Something")

        # It got in the database, because it was flushed *twice* (the
        # title was changed after flushed, and thus the object got dirty
        # again).
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Flushing: Something"),
                          (30, "Title 10"),
                         ])

        # This shouldn't do anything, because the object is clean again.
        foo.counter = 0
        self.store.flush()
        self.assertEquals(foo.title, "Flushing: Something")

    def test_flush_hook_all(self):

        class MyFoo(Foo):
            def __storm_pre_flush__(self):
                other = [foo1, foo2][foo1 is self]
                other.title = u"Changed in hook: " + other.title

        foo1 = self.store.get(MyFoo, 10)
        foo2 = self.store.get(MyFoo, 20)
        foo1.title = u"Changed"
        self.store.flush()

        self.assertEquals(foo1.title, "Changed in hook: Changed")
        self.assertEquals(foo2.title, "Changed in hook: Title 20")

    def test_flushed_hook(self):

        class MyFoo(Foo):
            done = False
            def __storm_flushed__(self):
                if not self.done:
                    self.done = True
                    self.title = u"Flushed: %s" % self.title

        foo = self.store.get(MyFoo, 20)

        self.assertEquals(foo.title, "Title 20")
        self.store.flush()
        self.assertEquals(foo.title, "Title 20") # It wasn't dirty.
        foo.title = u"Something"
        self.store.flush()
        self.assertEquals(foo.title, "Flushed: Something")

        # It got in the database, because it was flushed *twice* (the
        # title was changed after flushed, and thus the object got dirty
        # again).
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Flushed: Something"),
                          (30, "Title 10"),
                         ])

        # This shouldn't do anything, because the object is clean again.
        foo.done = False
        self.store.flush()
        self.assertEquals(foo.title, "Flushed: Something")

    def test_retrieve_default_primary_key(self):
        foo = Foo()
        foo.title = u"Title 40"
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

    def test_retrieve_null_when_no_default(self):
        bar = Bar()
        bar.id = 400
        self.store.add(bar)
        self.store.flush()
        self.assertEquals(bar.title, None)

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

    def test_wb_block_implicit_flushes(self):
        # Make sure calling store.flush() will fail.
        def flush():
            raise RuntimeError("Flush called")
        self.store.flush = flush

        # The following operations do not call flush.
        self.store.block_implicit_flushes()
        foo = self.store.get(Foo, 20)
        foo = self.store.find(Foo, Foo.id == 20).one()
        self.store.execute("SELECT title FROM foo WHERE id = 20")

        self.store.unblock_implicit_flushes()
        self.assertRaises(RuntimeError, self.store.get, Foo, 20)

    def test_wb_block_implicit_flushes_is_recursive(self):
        # Make sure calling store.flush() will fail.
        def flush():
            raise RuntimeError("Flush called")
        self.store.flush = flush

        self.store.block_implicit_flushes()
        self.store.block_implicit_flushes()
        self.store.unblock_implicit_flushes()
        # implicit flushes are still blocked, until unblock() is called again.
        foo = self.store.get(Foo, 20)
        self.store.unblock_implicit_flushes()
        self.assertRaises(RuntimeError, self.store.get, Foo, 20)

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
        foo.title = u"Title 40"
        self.assertRaises(WrongStoreError, self.store.reload, foo)

    def test_reload_new_unflushed(self):
        foo = Foo()
        foo.id = 40
        foo.title = u"Title 40"
        self.store.add(foo)
        self.assertRaises(NotFlushedError, self.store.reload, foo)

    def test_reload_removed(self):
        foo = self.store.get(Foo, 20)
        self.store.remove(foo)
        self.store.flush()
        self.assertRaises(WrongStoreError, self.store.reload, foo)

    def test_reload_unknown(self):
        foo = self.store.get(Foo, 20)
        store = self.create_store()
        self.assertRaises(WrongStoreError, store.reload, foo)

    def test_wb_reload_not_dirty(self):
        foo = self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        foo.title = u"Title 40"
        self.store.reload(foo)
        self.assertTrue(obj_info not in self.store._dirty)

    def test_find_set_empty(self):
        self.store.find(Foo, title=u"Title 20").set()
        foo = self.store.get(Foo, 20)
        self.assertEquals(foo.title, "Title 20")

    def test_find_set(self):
        self.store.find(Foo, title=u"Title 20").set(title=u"Title 40")
        foo = self.store.get(Foo, 20)
        self.assertEquals(foo.title, "Title 40")

    def test_find_set_column(self):
        self.store.find(Bar, title=u"Title 200").set(foo_id=Bar.id)
        bar = self.store.get(Bar, 200)
        self.assertEquals(bar.foo_id, 200)

    def test_find_set_expr(self):
        self.store.find(Foo, title=u"Title 20").set(Foo.title == u"Title 40")
        foo = self.store.get(Foo, 20)
        self.assertEquals(foo.title, "Title 40")

    def test_find_set_none(self):
        self.store.find(Foo, title=u"Title 20").set(title=None)
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
        self.store.find(Foo, title=u"Title 20").set(title=None)
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
        self.store.find(Foo, Foo.id == Select(SQL("20"))).set(title=u"Title 40")
        self.assertEquals(foo1.title, "Title 40")
        self.assertEquals(foo2.title, "Title 10")

    def test_find_set_expr_unsupported(self):
        result = self.store.find(Foo, title=u"Title 20")
        self.assertRaises(FeatureError, result.set, Foo.title > u"Title 40")

    def test_find_set_expr_unsupported_2(self):
        result = self.store.find(Foo, title=u"Title 20")
        self.assertRaises(FeatureError, result.set, Foo.title == Func("func"))

    def test_find_set_expr_unsupported_autoreloads(self):
        bar1 = self.store.get(Bar, 200)
        bar2 = self.store.get(Bar, 300)
        self.store.find(Bar, id=Select(SQL("200"))).set(title=u"Title 400")
        bar1_vars = get_obj_info(bar1).variables
        bar2_vars = get_obj_info(bar2).variables
        self.assertEquals(bar1_vars[Bar.title].get_lazy(), AutoReload)
        self.assertEquals(bar2_vars[Bar.title].get_lazy(), AutoReload)
        self.assertEquals(bar1_vars[Bar.foo_id].get_lazy(), None)
        self.assertEquals(bar2_vars[Bar.foo_id].get_lazy(), None)
        self.assertEquals(bar1.title, "Title 400")
        self.assertEquals(bar2.title, "Title 100")

    def test_wb_find_set_checkpoints(self):
        bar = self.store.get(Bar, 200)
        self.store.find(Bar, id=200).set(title=u"Title 400")
        self.store._connection.execute("UPDATE bar SET "
                                       "title='Title 500' "
                                       "WHERE id=200")
        # When not checkpointing, this flush will set title again.
        self.store.flush()
        self.store.reload(bar)
        self.assertEquals(bar.title, "Title 500")

    def test_find_set_with_info_alive_and_object_dead(self):
        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        foo = self.store.get(Foo, 20)
        foo.tainted = True
        obj_info = get_obj_info(foo)
        del foo
        gc.collect()
        self.store.find(Foo, title=u"Title 20").set(title=u"Title 40")
        foo = self.store.get(Foo, 20)
        self.assertFalse(hasattr(foo, "tainted"))
        self.assertEquals(foo.title, "Title 40")

    def test_reference(self):
        bar = self.store.get(Bar, 100)
        self.assertTrue(bar.foo)
        self.assertEquals(bar.foo.title, "Title 30")

    def test_reference_explicitly_with_wrapper(self):
        bar = self.store.get(Bar, 100)
        foo = Bar.foo.__get__(Wrapper(bar))
        self.assertTrue(foo)
        self.assertEquals(foo.title, "Title 30")

    def test_reference_break_on_local_diverged(self):
        bar = self.store.get(Bar, 100)
        self.assertTrue(bar.foo)
        bar.foo_id = 40
        self.assertEquals(bar.foo, None)

    def test_reference_break_on_remote_diverged(self):
        bar = self.store.get(Bar, 100)
        bar.foo.id = 40
        self.assertEquals(bar.foo, None)

    def test_reference_break_on_local_diverged_by_lazy(self):
        bar = self.store.get(Bar, 100)
        self.assertEquals(bar.foo.id, 10)
        bar.foo_id = SQL("20")
        self.assertEquals(bar.foo.id, 20)

    def test_reference_break_on_remote_diverged_by_lazy(self):
        class MyBar(Bar):
            pass
        MyBar.foo = Reference(MyBar.title, Foo.title)
        bar = self.store.get(MyBar, 100)
        bar.title = u"Title 30"
        self.store.flush()
        self.assertEquals(bar.foo.id, 10)
        bar.foo.title = SQL("'Title 40'")
        self.assertEquals(bar.foo, None)
        self.assertEquals(self.store.find(Foo, title=u"Title 30").one(), None)
        self.assertEquals(self.store.get(Foo, 10).title, u"Title 40")

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
        bar.title = u"Title 400"
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

    def test_set_reference_explicitly_with_wrapper(self):
        bar = self.store.get(Bar, 100)
        self.assertEquals(bar.foo.id, 10)
        foo = self.store.get(Foo, 30)
        Bar.foo.__set__(Wrapper(bar), Wrapper(foo))
        self.assertEquals(bar.foo.id, 30)
        result = self.store.execute("SELECT foo_id FROM bar WHERE id=100")
        self.assertEquals(result.get_one(), (30,))

    def test_reference_assign_remote_key(self):
        bar = self.store.get(Bar, 100)
        self.assertEquals(bar.foo.id, 10)
        bar.foo = 30
        self.assertEquals(bar.foo_id, 30)
        self.assertEquals(bar.foo.id, 30)
        result = self.store.execute("SELECT foo_id FROM bar WHERE id=100")
        self.assertEquals(result.get_one(), (30,))

    def test_reference_on_added(self):
        foo = Foo()
        foo.title = u"Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
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

    def test_reference_on_added_with_autoreload_key(self):
        foo = Foo()
        foo.title = u"Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        bar.foo = foo
        self.store.add(bar)

        self.assertEquals(bar.foo.id, None)
        self.assertEquals(bar.foo.title, "Title 40")

        foo.id = AutoReload

        # Variable shouldn't be autoreloaded yet.
        obj_info = get_obj_info(foo)
        self.assertEquals(obj_info.variables[Foo.id].get_lazy(), AutoReload)

        self.assertEquals(type(foo.id), int)

        self.store.flush()

        self.assertTrue(bar.foo.id)
        self.assertEquals(bar.foo.title, "Title 40")

        result = self.store.execute("SELECT foo.title FROM foo, bar "
                                    "WHERE bar.id=400 AND "
                                    "foo.id = bar.foo_id")
        self.assertEquals(result.get_one(), ("Title 40",))

    def test_reference_assign_none(self):
        foo = Foo()
        foo.title = u"Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        bar.foo = foo
        bar.foo = None
        bar.foo = None # Twice to make sure it doesn't blow up.
        self.store.add(bar)

        self.store.flush()

        self.assertEquals(type(bar.id), int)
        self.assertEquals(foo.id, None)

    def test_reference_on_added_composed_key(self):
        class Bar(object):
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int()
            title = Unicode()
            foo = Reference((foo_id, title), (Foo.id, Foo.title))

        foo = Foo()
        foo.title = u"Title 40"
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

    def test_reference_assign_composed_remote_key(self):
        class Bar(object):
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int()
            title = Unicode()
            foo = Reference((foo_id, title), (Foo.id, Foo.title))

        bar = Bar()
        bar.id = 400
        bar.foo = (20, u"Title 20")
        self.store.add(bar)

        self.assertEquals(bar.foo_id, 20)
        self.assertEquals(bar.foo.id, 20)
        self.assertEquals(bar.title, "Title 20")
        self.assertEquals(bar.foo.title, "Title 20")

    def test_reference_on_added_unlink_on_flush(self):
        foo = Foo()
        foo.title = u"Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.foo = foo
        bar.title = u"Title 400"
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
        foo1.title = u"Title 40"
        foo2 = Foo()
        foo2.title = u"Title 40"
        self.store.add(foo1)
        self.store.add(foo2)

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        bar.foo = foo1
        bar.foo = foo2
        self.store.add(bar)

        foo1.id = 40
        self.assertEquals(bar.foo_id, None)
        foo2.id = 50
        self.assertEquals(bar.foo_id, 50)

    def test_reference_on_added_and_changed_manually(self):
        foo = Foo()
        foo.title = u"Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        bar.foo = foo
        self.store.add(bar)

        bar.foo_id = 40
        foo.id = 50
        self.assertEquals(bar.foo_id, 40)

    def test_reference_on_added_composed_key_changed_manually(self):
        class Bar(object):
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int()
            title = Unicode()
            foo = Reference((foo_id, title), (Foo.id, Foo.title))

        foo = Foo()
        foo.title = u"Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.foo = foo
        self.store.add(bar)

        bar.title = u"Title 50"

        self.assertEquals(bar.foo, None)

        foo.id = 40

        self.assertEquals(bar.foo_id, None)

    def test_reference_on_added_no_local_store(self):
        foo = Foo()
        foo.title = u"Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        bar.foo = foo

        self.assertEquals(Store.of(bar), self.store)
        self.assertEquals(Store.of(foo), self.store)

    def test_reference_on_added_no_remote_store(self):
        foo = Foo()
        foo.title = u"Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        self.store.add(bar)

        bar.foo = foo

        self.assertEquals(Store.of(bar), self.store)
        self.assertEquals(Store.of(foo), self.store)

    def test_reference_on_added_no_store(self):
        foo = Foo()
        foo.title = u"Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        bar.foo = foo

        self.store.add(bar)

        self.assertEquals(Store.of(bar), self.store)
        self.assertEquals(Store.of(foo), self.store)

        self.store.flush()

        self.assertEquals(type(bar.foo_id), int)

    def test_reference_on_added_no_store_2(self):
        foo = Foo()
        foo.title = u"Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        bar.foo = foo

        self.store.add(foo)

        self.assertEquals(Store.of(bar), self.store)
        self.assertEquals(Store.of(foo), self.store)

        self.store.flush()

        self.assertEquals(type(bar.foo_id), int)

    def test_reference_on_added_wrong_store(self):
        store = self.create_store()

        foo = Foo()
        foo.title = u"Title 40"
        store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        self.store.add(bar)

        self.assertRaises(WrongStoreError, setattr, bar, "foo", foo)

    def test_reference_on_added_no_store_unlink_before_adding(self):
        foo1 = Foo()
        foo1.title = u"Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        bar.foo = foo1
        bar.foo = None

        self.store.add(bar)

        store = self.create_store()
        store.add(foo1)

        self.assertEquals(Store.of(bar), self.store)
        self.assertEquals(Store.of(foo1), store)

    def test_reference_on_removed_wont_add_back(self):
        bar = self.store.get(Bar, 200)
        foo = self.store.get(Foo, bar.foo_id)

        self.store.remove(bar)

        self.assertEquals(bar.foo, foo)
        self.store.flush()

        self.assertEquals(Store.of(bar), None)
        self.assertEquals(Store.of(foo), self.store)

    def test_reference_equals(self):
        foo = self.store.get(Foo, 10)

        bar = self.store.find(Bar, foo=foo).one()
        self.assertTrue(bar)
        self.assertEquals(bar.foo, foo)

        bar = self.store.find(Bar, foo=foo.id).one()
        self.assertTrue(bar)
        self.assertEquals(bar.foo, foo)

    def test_reference_equals_with_composed_key(self):
        # Interesting case of self-reference.
        class LinkWithRef(Link):
            myself = Reference((Link.foo_id, Link.bar_id),
                               (Link.foo_id, Link.bar_id))

        link = self.store.find(LinkWithRef, foo_id=10, bar_id=100).one()
        myself = self.store.find(LinkWithRef, myself=link).one()
        self.assertEquals(link, myself)

        myself = self.store.find(LinkWithRef,
                                 myself=(link.foo_id, link.bar_id)).one()
        self.assertEquals(link, myself)

    def test_reference_equals_with_wrapped(self):
        foo = self.store.get(Foo, 10)

        bar = self.store.find(Bar, foo=Wrapper(foo)).one()
        self.assertTrue(bar)
        self.assertEquals(bar.foo, foo)

    def test_reference_self(self):
        class Bar(object):
            __storm_table__ = "bar"
            id = Int(primary=True)
            title = Unicode()
            bar_id = Int("foo_id")
            bar = Reference(bar_id, id)

        bar = self.store.add(Bar())
        bar.id = 400
        bar.title = u"Title 400"
        bar.bar_id = 100
        self.assertEquals(bar.bar.id, 100)
        self.assertEquals(bar.bar.title, "Title 300")

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
        bar.title = u"Title 400"
        self.store.add(bar)

        foo = MyFoo()
        foo.bar = bar
        foo.title = u"Title 40"
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
        bar.title = u"Title 400"
        self.store.add(bar)

        foo = MyFoo()
        foo.bar = bar
        foo.title = u"Title 40"
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
        bar.title = u"Title 400"

        foo = MyFoo()
        foo.bar = bar
        foo.title = u"Title 40"

        self.store.add(bar)

        self.assertEquals(Store.of(bar), self.store)
        self.assertEquals(Store.of(foo), self.store)

        self.store.flush()

        self.assertEquals(type(bar.foo_id), int)

    def test_back_reference_on_added_no_store_2(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = u"Title 400"

        foo = MyFoo()
        foo.bar = bar
        foo.title = u"Title 40"

        self.store.add(foo)

        self.assertEquals(Store.of(bar), self.store)
        self.assertEquals(Store.of(foo), self.store)

        self.store.flush()

        self.assertEquals(type(bar.foo_id), int)

    def add_reference_set_bar_400(self):
        bar = Bar()
        bar.id = 400
        bar.foo_id = 20
        bar.title = u"Title 100"
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

    def test_reference_set_explicitly_with_wrapper(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSet, 20)

        items = []
        for bar in FooRefSet.bars.__get__(Wrapper(foo)):
            items.append((bar.id, bar.foo_id, bar.title))
        items.sort()

        self.assertEquals(items, [
                          (200, 20, "Title 200"),
                          (400, 20, "Title 100"),
                         ])

    def test_reference_set_with_added(self):
        bar1 = Bar()
        bar1.id = 400
        bar1.title = u"Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = u"Title 500"

        foo = FooRefSet()
        foo.title = u"Title 40"
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
        bar.title = u"Title 20"

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
        bar.title = u"Title 20"

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

        objects = list(foo.bars.find(Bar.title == u"Title 100"))
        self.assertEquals(len(objects), 1)
        self.assertTrue(objects[0] is bar)

        objects = list(foo.bars.find(title=u"Title 100"))
        self.assertEquals(len(objects), 1)
        self.assertTrue(objects[0] is bar)

    def test_reference_set_clear(self):
        foo = self.store.get(FooRefSet, 20)
        foo.bars.clear()
        self.assertEquals(list(foo.bars), [])

        # Object wasn't removed.
        self.assertTrue(self.store.get(Bar, 200))

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

    def test_reference_set_one(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSetOrderID, 20)
        self.assertRaises(NotOneError, foo.bars.one)

        foo = self.store.get(FooRefSetOrderID, 30)
        self.assertEquals(foo.bars.one().id, 300)

        foo = self.store.get(FooRefSetOrderID, 20)
        self.assertEquals(foo.bars.one(Bar.id > 400), None)

        foo = self.store.get(FooRefSetOrderID, 20)
        self.assertEquals(foo.bars.one(Bar.id < 400).id, 200)

        foo = self.store.get(FooRefSetOrderID, 20)
        self.assertEquals(foo.bars.one(id=200).id, 200)

    def test_reference_set_remove(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSet, 20)
        for bar in foo.bars:
            foo.bars.remove(bar)

        self.assertEquals(bar.foo_id, None)
        self.assertEquals(list(foo.bars), [])

    def test_reference_set_after_object_removed(self):
        class MyBar(Bar):
            # Make sure that this works even with allow_none=False.
            foo_id = Int(allow_none=False)

        class MyFoo(Foo):
            bars = ReferenceSet(Foo.id, MyBar.foo_id)

        foo = self.store.get(MyFoo, 20)
        bar = foo.bars.any()
        self.store.remove(bar)
        self.assertTrue(bar not in list(foo.bars))

    def test_reference_set_add(self):
        bar = Bar()
        bar.id = 400
        bar.title = u"Title 100"

        foo = self.store.get(FooRefSet, 20)
        foo.bars.add(bar)

        self.assertEquals(bar.foo_id, 20)
        self.assertEquals(Store.of(bar), self.store)

    def test_reference_set_add_no_store(self):
        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"

        foo = FooRefSet()
        foo.title = u"Title 40"
        foo.bars.add(bar)

        self.store.add(foo)

        self.assertEquals(Store.of(foo), self.store)
        self.assertEquals(Store.of(bar), self.store)

        self.store.flush()

        self.assertEquals(type(bar.foo_id), int)

    def test_reference_set_add_no_store_2(self):
        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"

        foo = FooRefSet()
        foo.title = u"Title 40"
        foo.bars.add(bar)

        self.store.add(bar)

        self.assertEquals(Store.of(foo), self.store)
        self.assertEquals(Store.of(bar), self.store)

        self.store.flush()

        self.assertEquals(type(bar.foo_id), int)

    def test_reference_set_add_no_store_unlink_after_adding(self):
        bar1 = Bar()
        bar1.id = 400
        bar1.title = u"Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = u"Title 500"

        foo = FooRefSet()
        foo.title = u"Title 40"
        foo.bars.add(bar1)
        foo.bars.add(bar2)
        foo.bars.remove(bar1)

        self.store.add(foo)

        store = self.create_store()
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
        bar1.title = u"Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = u"Title 500"
        self.store.add(bar1)
        self.store.add(bar2)

        foo = FooIndRefSet()
        foo.title = u"Title 40"
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
        for bar in foo.bars.find(Bar.title == u"Title 300"):
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

    def test_indirect_reference_set_one(self):
        foo = self.store.get(FooIndRefSetOrderID, 20)
        self.assertRaises(NotOneError, foo.bars.one)

        foo = self.store.get(FooIndRefSetOrderID, 30)
        self.assertEquals(foo.bars.one().id, 300)

        foo = self.store.get(FooIndRefSetOrderID, 20)
        self.assertEquals(foo.bars.one(Bar.id > 200), None)

        foo = self.store.get(FooIndRefSetOrderID, 20)
        self.assertEquals(foo.bars.one(Bar.id < 200).id, 100)

        foo = self.store.get(FooIndRefSetOrderID, 20)
        self.assertEquals(foo.bars.one(id=200).id, 200)

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

    def test_indirect_reference_set_add_remove_with_wrapper(self):
        foo = self.store.get(FooIndRefSet, 20)
        bar300 = self.store.get(Bar, 300)
        bar200 = self.store.get(Bar, 200)

        foo.bars.add(Wrapper(bar300))
        foo.bars.remove(Wrapper(bar200))

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEquals(items, [
                          (100, "Title 300"),
                          (300, "Title 100"),
                         ])

    def test_indirect_reference_set_add_remove_with_added(self):
        foo = FooIndRefSet()
        foo.id = 40
        bar1 = Bar()
        bar1.id = 400
        bar1.title = u"Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = u"Title 500"
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
        bar1.title = u"Title 400"
        bar2 = Bar()
        bar2.id = 500
        bar2.title = u"Title 500"

        foo = FooIndRefSet()
        foo.title = u"Title 40"

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

    def test_string_reference(self):
        class Base(object):
            __metaclass__ = PropertyPublisherMeta

        class MyBar(Base):
            __storm_table__ = "bar"
            id = Int(primary=True)
            title = Unicode()
            foo_id = Int()
            foo = Reference("foo_id", "MyFoo.id")

        class MyFoo(Base):
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode()

        bar = self.store.get(MyBar, 100)
        self.assertTrue(bar.foo)
        self.assertEquals(bar.foo.title, "Title 30")
        self.assertEquals(type(bar.foo), MyFoo)

    def test_string_indirect_reference_set(self):
        class Base(object):
            __metaclass__ = PropertyPublisherMeta

        class MyFoo(Base):
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode()
            bars = ReferenceSet("id", "MyLink.foo_id",
                                "MyLink.bar_id", "MyBar.id")

        class MyBar(Base):
            __storm_table__ = "bar"
            id = Int(primary=True)
            title = Unicode()

        class MyLink(Base):
            __storm_table__ = "link"
            __storm_primary__ = "foo_id", "bar_id"
            foo_id = Int()
            bar_id = Int()

        foo = self.store.get(MyFoo, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        self.assertEquals(items, [
                          (100, "Title 300"),
                          (200, "Title 200"),
                         ])

    def test_flush_order(self):
        foo1 = Foo()
        foo2 = Foo()
        foo3 = Foo()
        foo4 = Foo()
        foo5 = Foo()

        for i, foo in enumerate([foo1, foo2, foo3, foo4, foo5]):
            foo.title = u"Object %d" % (i+1)
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
        foo.title = u"Title 20"

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
        foo.title = u"Title 40"

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
        self.store.find(FooVariable, id=20).set(title=u"Title 20")

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "to_db(from_py(Title 20))"),
                          (30, "Title 10"),
                         ])

    def test_variable_filter_on_set_expr(self):
        foo = FooVariable()
        result = self.store.find(FooVariable, id=20)
        result.set(FooVariable.title == u"Title 20")

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "to_db(from_py(Title 20))"),
                          (30, "Title 10"),
                         ])

    def test_wb_result_set_variable(self):
        Result = self.store._connection.result_factory

        class MyResult(Result):
            def set_variable(self, variable, value):
                if variable.__class__ is UnicodeVariable:
                    variable.set(u"set_variable(%s)" % value)
                elif variable.__class__ is IntVariable:
                    variable.set(value+1)
                else:
                    variable.set(value)

        self.store._connection.result_factory = MyResult
        try:
            foo = self.store.get(Foo, 20)
        finally:
            self.store._connection.result_factory = Result

        self.assertEquals(foo.id, 21)
        self.assertEquals(foo.title, "set_variable(Title 20)")

    def test_default(self):
        class MyFoo(Foo):
            title = Unicode(default=u"Some default value")

        foo = MyFoo()
        self.store.add(foo)
        self.store.flush()

        result = self.store.execute("SELECT title FROM foo WHERE id=?",
                                    (foo.id,))
        self.assertEquals(result.get_one(), ("Some default value",))

        self.assertEquals(foo.title, "Some default value")

    def test_default_factory(self):
        class MyFoo(Foo):
            title = Unicode(default_factory=lambda:u"Some default value")

        foo = MyFoo()
        self.store.add(foo)
        self.store.flush()

        result = self.store.execute("SELECT title FROM foo WHERE id=?",
                                    (foo.id,))
        self.assertEquals(result.get_one(), ("Some default value",))

        self.assertEquals(foo.title, "Some default value")

    def test_pickle_variable(self):
        class PickleBlob(Blob):
            bin = Pickle()

        blob = self.store.get(Blob, 20)
        blob.bin = "\x80\x02}q\x01U\x01aK\x01s."
        self.store.flush()

        pickle_blob = self.store.get(PickleBlob, 20)
        self.assertEquals(pickle_blob.bin["a"], 1)

        pickle_blob.bin["b"] = 2

        self.store.flush()
        self.store.reload(blob)
        self.assertEquals(blob.bin, "\x80\x02}q\x01(U\x01aK\x01U\x01bK\x02u.")

    def test_undefined_variables_filled_on_find(self):
        """
        Check that when data is fetched from the database on a find,
        it is used to fill up any undefined variables.
        """
        # We do a first find to get the object_infos into the cache.
        foos = list(self.store.find(Foo, title=u"Title 20"))

        # Commit so that all foos are invalidated and variables are
        # set back to AutoReload.
        self.store.commit()

        # Another find which should reuse in-memory foos.
        for foo in self.store.find(Foo, title=u"Title 20"):
            # Make sure we have all variables defined, because
            # values were already retrieved by the find's select.
            obj_info = get_obj_info(foo)
            for column in obj_info.variables:
                self.assertTrue(obj_info.variables[column].is_defined())

    def test_defined_variables_not_overridden_on_find(self):
        """
        Check that the keep_defined=True setting in _load_object()
        is in place.  In practice, it ensures that already defined
        values aren't replaced during a find, when new data comes
        from the database and is used whenever possible.
        """
        blob = self.store.get(Blob, 20)
        blob.bin = "\x80\x02}q\x01U\x01aK\x01s."
        class PickleBlob(object):
            __storm_table__ = "bin"
            id = Int(primary=True)
            pickle = Pickle("bin")
        blob = self.store.get(PickleBlob, 20)
        value = blob.pickle
        # Now the find should not destroy our value pointer.
        blob = self.store.find(PickleBlob, id=20).one()
        self.assertTrue(value is blob.pickle)

    def test_pickle_variable_with_deleted_object(self):
        class PickleBlob(Blob):
            bin = Pickle()

        blob = self.store.get(Blob, 20)
        blob.bin = "\x80\x02}q\x01U\x01aK\x01s."
        self.store.flush()

        pickle_blob = self.store.get(PickleBlob, 20)
        self.assertEquals(pickle_blob.bin["a"], 1)

        pickle_blob.bin["b"] = 2

        del pickle_blob
        gc.collect()

        self.store.flush()
        self.store.reload(blob)
        self.assertEquals(blob.bin, "\x80\x02}q\x01(U\x01aK\x01U\x01bK\x02u.")

    def test_unhashable_object(self):

        class DictFoo(Foo, dict):
            pass

        foo = self.store.get(DictFoo, 20)
        foo["a"] = 1

        self.assertEquals(foo.items(), [("a", 1)])

        new_obj = DictFoo()
        new_obj.id = 40
        new_obj.title = u"My Title"

        self.store.add(new_obj)
        self.store.commit()

        self.assertTrue(self.store.get(DictFoo, 40) is new_obj)

    def test_wrapper(self):
        foo = self.store.get(Foo, 20)
        wrapper = Wrapper(foo)
        self.store.remove(wrapper)
        self.store.flush()
        self.assertEquals(self.store.get(Foo, 20), None)

    def test_rollback_loaded_and_still_in_cached(self):
        # Explore problem found on interaction between caching, commits,
        # and rollbacks, when they still existed.
        foo1 = self.store.get(Foo, 20)
        self.store.commit()
        self.store.rollback()
        foo2 = self.store.get(Foo, 20)
        self.assertTrue(foo1 is foo2)

    def test_class_alias(self):
        FooAlias = ClassAlias(Foo)
        result = self.store.find(FooAlias, FooAlias.id < Foo.id)
        self.assertEquals([(foo.id, foo.title) for foo in result
                           if type(foo) is Foo], [
                          (10, "Title 30"),
                          (10, "Title 30"),
                          (20, "Title 20"),
                         ])

    def test_expr_values(self):
        foo = self.store.get(Foo, 20)

        foo.title = SQL("'New title'")

        # No commits yet.
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.store.flush()

        # Now it should be there.

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "New title"),
                          (30, "Title 10"),
                         ])

        self.assertEquals(foo.title, "New title")

    def test_expr_values_flush_on_demand(self):
        foo = self.store.get(Foo, 20)

        foo.title = SQL("'New title'")

        # No commits yet.
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.assertEquals(foo.title, "New title")

        # Now it should be there.

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "New title"),
                          (30, "Title 10"),
                         ])

    def test_expr_values_flush_and_load_in_separate_steps(self):
        foo = self.store.get(Foo, 20)

        foo.title = SQL("'New title'")

        self.store.flush()

        # It's already in the database.
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "New title"),
                          (30, "Title 10"),
                         ])

        # But our value is now an AutoReload.
        lazy_value = get_obj_info(foo).variables[Foo.title].get_lazy()
        self.assertTrue(lazy_value is AutoReload)

        # Which gets resolved once touched.
        self.assertEquals(foo.title, u"New title")

    def test_expr_values_flush_on_demand_with_added(self):
        foo = Foo()
        foo.id = 40
        foo.title = SQL("'New title'")

        self.store.add(foo)

        # No commits yet.
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.assertEquals(foo.title, "New title")

        # Now it should be there.

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                          (40, "New title"),
                         ])

    def test_expr_values_flush_on_demand_with_removed_and_added(self):
        foo = self.store.get(Foo, 20)
        foo.title = SQL("'New title'")

        self.store.remove(foo)
        self.store.add(foo)

        # No commits yet.
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.assertEquals(foo.title, "New title")

        # Now it should be there.

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "New title"),
                          (30, "Title 10"),
                         ])

    def test_expr_values_flush_on_demand_with_removed_and_rollbacked(self):
        foo = self.store.get(Foo, 20)

        self.store.remove(foo)
        self.store.rollback()

        foo.title = SQL("'New title'")

        # No commits yet.
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.assertEquals(foo.title, "New title")

        # Now it should be there.

        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "New title"),
                          (30, "Title 10"),
                         ])

    def test_expr_values_flush_on_demand_with_added_and_removed(self):

        # This test tries to trigger a problem in a few different ways.
        # It uses the same id of an existing object, and add and remove
        # the object. This object should never get in the database, nor
        # update the object that is already there, nor flush any other
        # pending changes when the lazy value is accessed.

        foo = Foo()
        foo.id = 20

        foo_dep = Foo()
        foo_dep.id = 50

        self.store.add(foo)
        self.store.add(foo_dep)

        foo.title = SQL("'New title'")

        # Add ordering to see if it helps triggering a bug of
        # incorrect flushing.
        self.store.add_flush_order(foo_dep, foo)

        self.store.remove(foo)

        # No changes.
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.assertEquals(foo.title, None)

        # Still no changes. There's no reason why foo_dep would be flushed.
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_expr_values_flush_on_demand_with_removed(self):

        # Similar case, but removing an existing object instead.

        foo = self.store.get(Foo, 20)

        foo_dep = Foo()
        foo_dep.id = 50

        self.store.add(foo_dep)

        foo.title = SQL("'New title'")

        # Add ordering to see if it helps triggering a bug of
        # incorrect flushing.
        self.store.add_flush_order(foo_dep, foo)

        self.store.remove(foo)

        # No changes.
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

        self.assertEquals(foo.title, None)

        # Still no changes. There's no reason why foo_dep would be flushed.
        self.assertEquals(self.get_items(), [
                          (10, "Title 30"),
                          (20, "Title 20"),
                          (30, "Title 10"),
                         ])

    def test_expr_values_with_columns(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = Bar.id+1
        self.assertEquals(bar.foo_id, 201)

    def test_autoreload_attribute(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.assertEquals(foo.title, "Title 20")
        foo.title = AutoReload
        self.assertEquals(foo.title, "New Title")
        self.assertFalse(get_obj_info(foo).variables[Foo.title].has_changed())

    def test_autoreload_attribute_with_changed_primary_key(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.assertEquals(foo.title, "Title 20")
        foo.id = 40
        foo.title = AutoReload
        self.assertEquals(foo.title, "New Title")
        self.assertEquals(foo.id, 40)

    def test_autoreload_object(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.assertEquals(foo.title, "Title 20")
        self.store.autoreload(foo)
        self.assertEquals(foo.title, "New Title")

    def test_autoreload_primary_key_of_unflushed_object(self):
        foo = Foo()
        self.store.add(foo)
        foo.id = AutoReload
        foo.title = u"New Title"
        self.assertTrue(isinstance(foo.id, (int, long)))
        self.assertEquals(foo.title, "New Title")

    def test_autoreload_primary_key_doesnt_reload_everything_else(self):
        foo = self.store.get(Foo, 20)
        self.store.autoreload(foo)

        obj_info = get_obj_info(foo)

        self.assertEquals(obj_info.variables[Foo.id].get_lazy(), None)
        self.assertEquals(obj_info.variables[Foo.title].get_lazy(), AutoReload)

        self.assertEquals(foo.id, 20)

        self.assertEquals(obj_info.variables[Foo.id].get_lazy(), None)
        self.assertEquals(obj_info.variables[Foo.title].get_lazy(), AutoReload)

    def test_autoreload_all_objects(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.assertEquals(foo.title, "Title 20")
        self.store.autoreload()
        self.assertEquals(foo.title, "New Title")

    def test_autoreload_and_get_will_not_reload(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.store.autoreload(foo)

        obj_info = get_obj_info(foo)

        self.assertEquals(obj_info.variables[Foo.title].get_lazy(), AutoReload)
        self.store.get(Foo, 20)
        self.assertEquals(obj_info.variables[Foo.title].get_lazy(), AutoReload)
        self.assertEquals(foo.title, "New Title")

    def test_autoreload_object_doesnt_tag_as_dirty(self):
        foo = self.store.get(Foo, 20)
        self.store.autoreload(foo)
        self.assertTrue(get_obj_info(foo) not in self.store._dirty)

    def test_autoreload_missing_columns_on_insertion(self):
        foo = Foo()
        self.store.add(foo)
        self.store.flush()
        lazy_value = get_obj_info(foo).variables[Foo.title].get_lazy()
        self.assertEquals(lazy_value, AutoReload)
        self.assertEquals(foo.title, u"Default Title")

    def test_reference_break_on_local_diverged_doesnt_autoreload(self):
        foo = self.store.get(Foo, 10)
        self.store.autoreload(foo)

        bar = self.store.get(Bar, 100)
        self.assertTrue(bar.foo)
        bar.foo_id = 40
        self.assertEquals(bar.foo, None)

        obj_info = get_obj_info(foo)
        self.assertEquals(obj_info.variables[Foo.title].get_lazy(), AutoReload)

    def test_invalidate_and_get_object(self):
        foo = self.store.get(Foo, 20)
        self.store.invalidate(foo)
        self.assertEquals(self.store.get(Foo, 20), foo)
        self.assertEquals(self.store.find(Foo, id=20).one(), foo)

    def test_invalidate_and_get_removed_object(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("DELETE FROM foo WHERE id=20")
        self.store.invalidate(foo)
        self.assertEquals(self.store.get(Foo, 20), None)
        self.assertEquals(self.store.find(Foo, id=20).one(), None)

    def test_invalidate_and_validate_with_find(self):
        foo = self.store.get(Foo, 20)
        self.store.invalidate(foo)
        self.assertEquals(self.store.find(Foo, id=20).one(), foo)

        # Cache should be considered valid again at this point.
        self.store.execute("DELETE FROM foo WHERE id=20")
        self.assertEquals(self.store.get(Foo, 20), foo)

    def test_invalidate_object_gets_validated(self):
        foo = self.store.get(Foo, 20)
        self.store.invalidate(foo)
        self.assertEquals(self.store.get(Foo, 20), foo)

        # At this point the object is valid again, so deleting it
        # from the database directly shouldn't affect caching.
        self.store.execute("DELETE FROM foo WHERE id=20")
        self.assertEquals(self.store.get(Foo, 20), foo)

    def test_invalidate_object_with_only_primary_key(self):
        link = self.store.get(Link, (20, 200))
        self.store.execute("DELETE FROM link WHERE foo_id=20 AND bar_id=200")
        self.store.invalidate(link)
        self.assertEquals(self.store.get(Link, (20, 200)), None)

    def test_invalidate_added_object(self):
        foo = Foo()
        self.store.add(foo)
        self.store.invalidate(foo)
        foo.id = 40
        foo.title = u"Title 40"
        self.store.flush()

        # Object must have a valid cache at this point, since it was
        # just added.
        self.store.execute("DELETE FROM foo WHERE id=40")
        self.assertEquals(self.store.get(Foo, 40), foo)

    def test_invalidate_and_update(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("DELETE FROM foo WHERE id=20")
        self.store.invalidate(foo)
        self.assertRaises(LostObjectError, setattr, foo, "title", u"Title 40")

    def test_invalidate_and_get_returns_autoreloaded(self):
        foo = self.store.get(Foo, 20)
        self.store.invalidate(foo)
        foo = self.store.get(Foo, 20)
        self.assertEquals(get_obj_info(foo).variables[Foo.title].get_lazy(),
                          AutoReload)
        self.assertEquals(foo.title, "Title 20")

    def test_invalidated_hook(self):
        called = []
        class MyFoo(Foo):
            def __storm_invalidated__(self):
                called.append(True)
        foo = self.store.get(MyFoo, 20)
        self.assertEquals(called, [])
        self.store.autoreload(foo)
        self.assertEquals(called, [])
        self.store.invalidate(foo)
        self.assertEquals(called, [True])

    def test_invalidated_hook_called_after_all_invalidated(self):
        """
        Ensure that invalidated hooks are called only when all objects have
        already been marked as invalidated. See comment in
        store.py:_mark_autoreload.
        """
        called = []
        class MyFoo(Foo):
            def __storm_invalidated__(self):
                if not called:
                    called.append(get_obj_info(foo1).get("invalidated"))
                    called.append(get_obj_info(foo2).get("invalidated"))
        foo1 = self.store.get(MyFoo, 10)
        foo2 = self.store.get(MyFoo, 20)
        self.store.invalidate()
        self.assertEquals(called, [True, True])

    def test_result_union(self):
        result1 = self.store.find(Foo, id=30)
        result2 = self.store.find(Foo, id=10)
        result3 = result1.union(result2)

        result3.order_by(Foo.title)
        self.assertEquals([(foo.id, foo.title) for foo in result3], [
                          (30, "Title 10"),
                          (10, "Title 30"),
                         ])

        result3.order_by(Desc(Foo.title))
        self.assertEquals([(foo.id, foo.title) for foo in result3], [
                          (10, "Title 30"),
                          (30, "Title 10"),
                         ])

    def test_result_union_duplicated(self):
        result1 = self.store.find(Foo, id=30)
        result2 = self.store.find(Foo, id=30)

        result3 = result1.union(result2)

        self.assertEquals([(foo.id, foo.title) for foo in result3], [
                          (30, "Title 10"),
                         ])

    def test_result_union_duplicated_with_all(self):
        result1 = self.store.find(Foo, id=30)
        result2 = self.store.find(Foo, id=30)

        result3 = result1.union(result2, all=True)

        self.assertEquals([(foo.id, foo.title) for foo in result3], [
                          (30, "Title 10"),
                          (30, "Title 10"),
                         ])

    def test_result_union_with_empty(self):
        result1 = self.store.find(Foo, id=30)
        result2 = EmptyResultSet()

        result3 = result1.union(result2)

        self.assertEquals([(foo.id, foo.title) for foo in result3], [
                          (30, "Title 10"),
                         ])

    def test_result_union_incompatible(self):
        result1 = self.store.find(Foo, id=10)
        result2 = self.store.find(Bar, id=100)
        self.assertRaises(FeatureError, result1.union, result2)

    def test_result_union_unsupported_methods(self):
        result1 = self.store.find(Foo, id=30)
        result2 = self.store.find(Foo, id=10)
        result3 = result1.union(result2)

        self.assertRaises(FeatureError, result3.set, title=u"Title 40")
        self.assertRaises(FeatureError, result3.remove)

    def test_result_union_count(self):
        result1 = self.store.find(Foo, id=30)
        result2 = self.store.find(Foo, id=30)

        result3 = result1.union(result2, all=True)

        self.assertEquals(result3.count(), 2)

    def test_result_difference(self):
        if self.__class__.__name__.startswith("MySQL"):
            return

        result1 = self.store.find(Foo)
        result2 = self.store.find(Foo, id=20)
        result3 = result1.difference(result2)

        result3.order_by(Foo.title)
        self.assertEquals([(foo.id, foo.title) for foo in result3], [
                          (30, "Title 10"),
                          (10, "Title 30"),
                         ])

        result3.order_by(Desc(Foo.title))
        self.assertEquals([(foo.id, foo.title) for foo in result3], [
                          (10, "Title 30"),
                          (30, "Title 10"),
                         ])

    def test_result_difference_with_empty(self):
        if self.__class__.__name__.startswith("MySQL"):
            return

        result1 = self.store.find(Foo, id=30)
        result2 = EmptyResultSet()

        result3 = result1.difference(result2)

        self.assertEquals([(foo.id, foo.title) for foo in result3], [
                          (30, "Title 10"),
                         ])

    def test_result_difference_incompatible(self):
        if self.__class__.__name__.startswith("MySQL"):
            return

        result1 = self.store.find(Foo, id=10)
        result2 = self.store.find(Bar, id=100)
        self.assertRaises(FeatureError, result1.difference, result2)

    def test_result_difference_count(self):
        if self.__class__.__name__.startswith("MySQL"):
            return

        result1 = self.store.find(Foo)
        result2 = self.store.find(Foo, id=20)

        result3 = result1.difference(result2)

        self.assertEquals(result3.count(), 2)

    def test_result_intersection(self):
        if self.__class__.__name__.startswith("MySQL"):
            return

        result1 = self.store.find(Foo)
        result2 = self.store.find(Foo, Foo.id.is_in((10, 30)))
        result3 = result1.intersection(result2)

        result3.order_by(Foo.title)
        self.assertEquals([(foo.id, foo.title) for foo in result3], [
                          (30, "Title 10"),
                          (10, "Title 30"),
                         ])

        result3.order_by(Desc(Foo.title))
        self.assertEquals([(foo.id, foo.title) for foo in result3], [
                          (10, "Title 30"),
                          (30, "Title 10"),
                         ])

    def test_result_intersection_with_empty(self):
        if self.__class__.__name__.startswith("MySQL"):
            return

        result1 = self.store.find(Foo, id=30)
        result2 = EmptyResultSet()
        result3 = result1.intersection(result2)

        self.assertEquals(len(list(result3)), 0)

    def test_result_intersection_incompatible(self):
        if self.__class__.__name__.startswith("MySQL"):
            return

        result1 = self.store.find(Foo, id=10)
        result2 = self.store.find(Bar, id=100)
        self.assertRaises(FeatureError, result1.intersection, result2)

    def test_result_intersection_count(self):
        if self.__class__.__name__.startswith("MySQL"):
            return

        result1 = self.store.find(Foo, Foo.id.is_in((10, 20)))
        result2 = self.store.find(Foo, Foo.id.is_in((10, 30)))
        result3 = result1.intersection(result2)

        self.assertEquals(result3.count(), 1)

    def test_proxy(self):
        bar = self.store.get(BarProxy, 200)
        self.assertEquals(bar.foo_title, "Title 20")

    def test_proxy_equals(self):
        bar = self.store.find(BarProxy, BarProxy.foo_title == u"Title 20").one()
        self.assertTrue(bar)
        self.assertEquals(bar.id, 200)

    def test_proxy_as_column(self):
        result = self.store.find(BarProxy, BarProxy.id == 200)
        self.assertEquals(list(result.values(BarProxy.foo_title)),
                          ["Title 20"])

    def test_proxy_set(self):
        bar = self.store.get(BarProxy, 200)
        bar.foo_title = u"New Title"
        foo = self.store.get(Foo, 20)
        self.assertEquals(foo.title, "New Title")

    def get_bar_proxy_with_string(self):
        class Base(object):
            __metaclass__ = PropertyPublisherMeta

        class MyBarProxy(Base):
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int()
            foo = Reference("foo_id", "MyFoo.id")
            foo_title = Proxy(foo, "MyFoo.title")

        class MyFoo(Base):
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode()

        return MyBarProxy, MyFoo

    def test_proxy_with_string(self):
        MyBarProxy, MyFoo = self.get_bar_proxy_with_string()
        bar = self.store.get(MyBarProxy, 200)
        self.assertEquals(bar.foo_title, "Title 20")

    def test_proxy_with_string_variable_factory_attribute(self):
        MyBarProxy, MyFoo = self.get_bar_proxy_with_string()
        variable = MyBarProxy.foo_title.variable_factory(value=u"Hello")
        self.assertTrue(isinstance(variable, UnicodeVariable))

    def test_proxy_with_extra_table(self):
        """
        Proxies use a join on auto_tables. It should work even if we have
        more tables in the query.
        """
        result = self.store.find((BarProxy, Link),
                                 BarProxy.foo_title == u"Title 20",
                                 BarProxy.foo_id == Link.foo_id)
        results = list(result)
        self.assertEquals(len(results), 2)
        for bar, link in results:
            self.assertEquals(bar.id, 200)
            self.assertEquals(bar.foo_title, u"Title 20")
            self.assertEquals(bar.foo_id, 20)
            self.assertEquals(link.foo_id, 20)

    def test_get_decimal_property(self):
        money = self.store.get(Money, 10)
        self.assertEquals(money.value, decimal.Decimal("12.3455"))

    def test_set_decimal_property(self):
        money = self.store.get(Money, 10)
        money.value = decimal.Decimal("12.3456")
        self.store.flush()
        result = self.store.find(Money, value=decimal.Decimal("12.3456"))
        self.assertEquals(result.one(), money)

    def test_fill_missing_primary_key_with_lazy_value(self):
        foo = self.store.get(Foo, 10)
        foo.id = SQL("40")
        self.store.flush()
        self.assertEquals(foo.id, 40)
        self.assertEquals(self.store.get(Foo, 10), None)
        self.assertEquals(self.store.get(Foo, 40), foo)

    def test_fill_missing_primary_key_with_lazy_value_on_creation(self):
        foo = Foo()
        foo.id = SQL("40")
        self.store.add(foo)
        self.store.flush()
        self.assertEquals(foo.id, 40)
        self.assertEquals(self.store.get(Foo, 40), foo)

    def test_preset_primary_key(self):
        check = []
        def preset_primary_key(primary_columns, primary_variables):
            check.append([(variable.is_defined(), variable.get_lazy())
                          for variable in primary_variables])
            check.append([column.name for column in primary_columns])
            primary_variables[0].set(SQL("40"))

        class DatabaseWrapper(object):
            """Wrapper to inject our custom preset_primary_key hook."""

            def __init__(self, database):
                self.database = database

            def connect(self):
                connection = self.database.connect()
                connection.preset_primary_key = preset_primary_key
                return connection

        store = Store(DatabaseWrapper(self.database))

        foo = store.add(Foo())

        store.flush()
        try:
            self.assertEquals(check, [[(False, None)], ["id"]])
            self.assertEquals(foo.id, 40)
        finally:
            store.close()

    def test_strong_cache_used(self):
        """
        Objects should be referenced in the cache if not referenced
        in application code.
        """
        foo = self.store.get(Foo, 20)
        foo.tainted = True
        obj_info = get_obj_info(foo)
        del foo
        gc.collect()
        cached = self.store.find(Foo).cached()
        self.assertEquals(len(cached), 1)
        foo = self.store.get(Foo, 20)
        self.assertEquals(cached, [foo])
        self.assertTrue(hasattr(foo, "tainted"))

    def test_strong_cache_cleared_on_invalidate_all(self):
        cache = self.get_cache(self.store)
        foo = self.store.get(Foo, 20)
        self.assertEquals(cache.get_cached(), [get_obj_info(foo)])
        self.store.invalidate()
        self.assertEquals(cache.get_cached(), [])

    def test_strong_cache_loses_object_on_invalidate(self):
        cache = self.get_cache(self.store)
        foo = self.store.get(Foo, 20)
        self.assertEquals(cache.get_cached(), [get_obj_info(foo)])
        self.store.invalidate(foo)
        self.assertEquals(cache.get_cached(), [])

    def test_strong_cache_loses_object_on_remove(self):
        """
        Make sure an object gets removed from the strong reference
        cache when removed from the store.
        """
        cache = self.get_cache(self.store)
        foo = self.store.get(Foo, 20)
        self.assertEquals(cache.get_cached(), [get_obj_info(foo)])
        self.store.remove(foo)
        self.store.flush()
        self.assertEquals(cache.get_cached(), [])

    def test_strong_cache_renews_object_on_get(self):
        cache = self.get_cache(self.store)
        foo1 = self.store.get(Foo, 10)
        foo2 = self.store.get(Foo, 20)
        foo1 = self.store.get(Foo, 10)
        self.assertEquals(cache.get_cached(),
                          [get_obj_info(foo1), get_obj_info(foo2)])

    def test_strong_cache_renews_object_on_find(self):
        cache = self.get_cache(self.store)
        foo1 = self.store.find(Foo, id=10).one()
        foo2 = self.store.find(Foo, id=20).one()
        foo1 = self.store.find(Foo, id=10).one()
        self.assertEquals(cache.get_cached(),
                          [get_obj_info(foo1), get_obj_info(foo2)])

    def test_unicode(self):
        class MyFoo(Foo):
            pass
        foo = self.store.get(Foo, 20)
        myfoo = self.store.get(MyFoo, 20)
        for title in [u'Cừơng', u'Đức', u'Hạnh']:
            foo.title = title
            self.store.commit()
            try:
                self.assertEquals(myfoo.title, title)
            except AssertionError, e:
                raise AssertionError(str(e) +
                    " (ensure your database was created with CREATE DATABASE"
                    " ... CHARACTER SET utf8)")


class EmptyResultSetTest(object):

    def setUp(self):
        self.create_database()
        self.drop_tables()
        self.create_tables()
        self.create_store()
        self.empty = EmptyResultSet()
        self.result = self.store.find(Foo)

    def tearDown(self):
        self.drop_store()
        self.drop_tables()
        self.drop_database()

    def create_database(self):
        raise NotImplementedError

    def create_tables(self):
        raise NotImplementedError

    def create_store(self):
        self.store = Store(self.database)

    def drop_database(self):
        pass

    def drop_tables(self):
        for table in ["foo", "bar", "bin", "link"]:
            connection = self.database.connect()
            try:
                connection.execute("DROP TABLE %s" % table)
                connection.commit()
            except:
                connection.rollback()

    def drop_store(self):
        self.store.rollback()
        # Closing the store is needed because testcase objects are all
        # instantiated at once, and thus connections are kept open.
        self.store.close()

    def test_iter(self):
        self.assertEquals(list(self.result), list(self.empty))

    def test_copy(self):
        self.assertNotEquals(self.result.copy(), self.result)
        self.assertNotEquals(self.empty.copy(), self.empty)
        self.assertEquals(list(self.result.copy()), list(self.empty.copy()))

    def test_config(self):
        self.result.config(distinct=True, offset=1, limit=1)
        self.empty.config(distinct=True, offset=1, limit=1)
        self.assertEquals(list(self.result), list(self.empty))

    def test_slice(self):
        self.assertEquals(list(self.result[:]), [])
        self.assertEquals(list(self.empty[:]), [])

    def test_any(self):
        self.assertEquals(self.result.any(), None)
        self.assertEquals(self.empty.any(), None)

    def test_first_unordered(self):
        self.assertRaises(UnorderedError, self.result.first)
        self.assertRaises(UnorderedError, self.empty.first)

    def test_first_ordered(self):
        self.result.order_by(Foo.title)
        self.assertEquals(self.result.first(), None)
        self.empty.order_by(Foo.title)
        self.assertEquals(self.empty.first(), None)

    def test_last_unordered(self):
        self.assertRaises(UnorderedError, self.result.last)
        self.assertRaises(UnorderedError, self.empty.last)

    def test_last_ordered(self):
        self.result.order_by(Foo.title)
        self.assertEquals(self.result.last(), None)
        self.empty.order_by(Foo.title)
        self.assertEquals(self.empty.last(), None)

    def test_one(self):
        self.assertEquals(self.result.one(), None)
        self.assertEquals(self.empty.one(), None)

    def test_order_by(self):
        self.assertEquals(self.result.order_by(Foo.title), self.result)
        self.assertEquals(self.empty.order_by(Foo.title), self.empty)

    def test_remove(self):
        self.assertEquals(self.result.remove(), None)
        self.assertEquals(self.empty.remove(), None)

    def test_count(self):
        self.assertEquals(self.result.count(), 0)
        self.assertEquals(self.empty.count(), 0)
        self.assertEquals(self.empty.count(column="abc"), 0)
        self.assertEquals(self.empty.count(distinct=True), 0)

    def test_max(self):
        self.assertEquals(self.result.max(Foo.id), None)
        self.assertEquals(self.empty.max(Foo.id), None)

    def test_min(self):
        self.assertEquals(self.result.min(Foo.id), None)
        self.assertEquals(self.empty.min(Foo.id), None)

    def test_avg(self):
        self.assertEquals(self.result.avg(Foo.id), None)
        self.assertEquals(self.empty.avg(Foo.id), None)

    def test_sum(self):
        self.assertEquals(self.result.sum(Foo.id), None)
        self.assertEquals(self.empty.sum(Foo.id), None)

    def test_values_no_columns(self):
        self.assertRaises(FeatureError, list, self.result.values())
        self.assertRaises(FeatureError, list, self.empty.values())

    def test_values(self):
        self.assertEquals(list(self.result.values(Foo.title)), [])
        self.assertEquals(list(self.empty.values(Foo.title)), [])

    def test_set_no_args(self):
        self.assertEquals(self.result.set(), None)
        self.assertEquals(self.empty.set(), None)

    def test_cached(self):
        self.assertEquals(self.result.cached(), [])
        self.assertEquals(self.empty.cached(), [])

    def test_union(self):
        self.assertEquals(self.empty.union(self.empty), self.empty)
        self.assertEquals(type(self.empty.union(self.result)),
                          type(self.result))
        self.assertEquals(type(self.result.union(self.empty)),
                          type(self.result))

    def test_difference(self):
        self.assertEquals(self.empty.difference(self.empty), self.empty)
        self.assertEquals(self.empty.difference(self.result), self.empty)
        self.assertEquals(self.result.difference(self.empty), self.result)

    def test_intersection(self):
        self.assertEquals(self.empty.intersection(self.empty), self.empty)
        self.assertEquals(self.empty.intersection(self.result), self.empty)
        self.assertEquals(self.result.intersection(self.empty), self.empty)

