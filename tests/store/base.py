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
import operator
import pytest
from uuid import uuid4
import weakref

from storm.compat import add_metaclass, iter_range, iter_values, long_int, StringIO, ustr
from storm.references import Reference, ReferenceSet, Proxy
from storm.database import Result, STATE_DISCONNECTED
from storm.properties import (
    Int, Float, JSON, RawStr, Unicode, Property, UUID)
from storm.properties import PropertyPublisherMeta, Decimal
from storm.expr import (
    Asc, Desc, Select, LeftJoin, SQL, Count, Sum, Avg, And, Or, Eq, Lower)
from storm.variables import Variable, JSONVariable, UnicodeVariable, IntVariable
from storm.info import get_obj_info, ClassAlias
from storm.exceptions import (
    ClosedError, ConnectionBlockedError, FeatureError, LostObjectError,
    NoStoreError, NotFlushedError, NotOneError, OrderLoopError, UnorderedError,
    WrongStoreError, DisconnectionError)
from storm.cache import Cache
from storm.store import AutoReload, EmptyResultSet, Store, ResultSet
from storm.tracer import debug

from tests.base import Wrapper
from tests.helper import TestHelper


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

class UniqueID(object):
    __storm_table__ = "unique_id"
    id = UUID(primary=True)
    def __init__(self, id):
        self.id = id

class Json(object):
    __storm_table__ = "json"
    id = Int(primary=True)
    data = JSON()

class Link(object):
    __storm_table__ = "link"
    __storm_primary__ = "foo_id", "bar_id"
    foo_id = Int()
    bar_id = Int()

class SelfRef(object):
    __storm_table__ = "selfref"
    id = Int(primary=True)
    title = Unicode()
    selfref_id = Int()
    selfref = Reference(selfref_id, id)
    selfref_on_remote = Reference(id, selfref_id, on_remote=True)

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


class FooValue(object):
    __storm_table__ = "foovalue"
    id = Int(primary=True)
    foo_id = Int()
    value1 = Int()
    value2 = Int()

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


class DummyDatabase(object):

    def connect(self, event=None):
        return None


class StoreCacheTest(TestHelper):

    def test_wb_custom_cache(self):
        cache = Cache(25)
        store = Store(DummyDatabase(), cache=cache)
        assert store._cache == cache

    def test_wb_default_cache_size(self):
        store = Store(DummyDatabase())
        assert store._cache._size == 1000


class StoreDatabaseTest(TestHelper):

    def test_store_has_reference_to_its_database(self):
        database = DummyDatabase()
        store = Store(database)
        assert store.get_database() is database


class StoreTest(object):

    def setUp(self):
        self.store = None
        self.stores = []
        self.create_database()
        self.connection = self.database.connect()
        self.drop_tables()
        self.create_tables()
        self.create_sample_data()
        self.create_store()

    def tearDown(self):
        self.drop_store()
        self.drop_sample_data()
        self.drop_tables()
        self.drop_database()
        self.connection.close()

    def create_database(self):
        raise NotImplementedError

    def create_tables(self):
        raise NotImplementedError

    def create_sample_data(self):
        connection = self.connection
        connection.execute("INSERT INTO foo (id, title)"
                           " VALUES (10, 'Title 30')")
        connection.execute("INSERT INTO foo (id, title)"
                           " VALUES (20, 'Title 20')")
        connection.execute("INSERT INTO foo (id, title)"
                           " VALUES (30, 'Title 10')")
        connection.execute("INSERT INTO bar (id, foo_id, title)"
                           " VALUES (100, 10, 'Title 300')")
        connection.execute("INSERT INTO bar (id, foo_id, title)"
                           " VALUES (200, 20, 'Title 200')")
        connection.execute("INSERT INTO bar (id, foo_id, title)"
                           " VALUES (300, 30, 'Title 100')")
        connection.execute("INSERT INTO json (id, data) VALUES (10, '{}')")
        connection.execute("INSERT INTO json (id, data) VALUES (20, '{\"a\": 1}')")
        connection.execute("INSERT INTO json (id, data) VALUES (30, '{\"b\": 2}')")
        connection.execute("INSERT INTO link (foo_id, bar_id) VALUES (10, 100)")
        connection.execute("INSERT INTO link (foo_id, bar_id) VALUES (10, 200)")
        connection.execute("INSERT INTO link (foo_id, bar_id) VALUES (10, 300)")
        connection.execute("INSERT INTO link (foo_id, bar_id) VALUES (20, 100)")
        connection.execute("INSERT INTO link (foo_id, bar_id) VALUES (20, 200)")
        connection.execute("INSERT INTO link (foo_id, bar_id) VALUES (30, 300)")
        connection.execute("INSERT INTO money (id, value)"
                           " VALUES (10, '12.3455')")
        connection.execute("INSERT INTO selfref (id, title, selfref_id)"
                           " VALUES (15, 'SelfRef 15', NULL)")
        connection.execute("INSERT INTO selfref (id, title, selfref_id)"
                           " VALUES (25, 'SelfRef 25', NULL)")
        connection.execute("INSERT INTO selfref (id, title, selfref_id)"
                           " VALUES (35, 'SelfRef 35', 15)")
        connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (1, 10, 2, 1)")
        connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (2, 10, 2, 1)")
        connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (3, 10, 2, 1)")
        connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (4, 10, 2, 2)")
        connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (5, 20, 1, 3)")
        connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (6, 20, 1, 3)")
        connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (7, 20, 1, 4)")
        connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (8, 20, 1, 4)")
        connection.execute("INSERT INTO foovalue (id, foo_id, value1, value2)"
                           " VALUES (9, 20, 1, 2)")

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
        for table in ["foo", "bar", "json", "link", "money", "selfref",
                      "foovalue", "unique_id"]:
            try:
                self.connection.execute("DROP TABLE %s" % table)
                self.connection.commit()
            except:
                self.connection.rollback()

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
        assert isinstance(result, Result)
        assert result.get_one() == (1,)

        result = self.store.execute("SELECT 1", noresult=True)
        assert result == None

    def test_execute_params(self):
        result = self.store.execute("SELECT ?", [1])
        assert isinstance(result, Result)
        assert result.get_one() == (1,)

    def test_execute_flushes(self):
        foo = self.store.get(Foo, 10)
        foo.title = u"New Title"

        result = self.store.execute("SELECT title FROM foo WHERE id=10")
        assert result.get_one() == ("New Title",)

    def test_close(self):
        store = Store(self.database)
        store.close()
        with pytest.raises(ClosedError):
            store.execute("SELECT 1")

    def test_get(self):
        foo = self.store.get(Foo, 10)
        assert foo.id == 10
        assert foo.title == "Title 30"

        foo = self.store.get(Foo, 20)
        assert foo.id == 20
        assert foo.title == "Title 20"

        foo = self.store.get(Foo, 40)
        assert foo == None

    def test_get_cached(self):
        foo = self.store.get(Foo, 10)
        assert self.store.get(Foo, 10) is foo

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
        assert not getattr(foo, "taint", False)

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
        assert self.store.add(foo) == foo

    def test_add_and_stop_referencing(self):
        # After adding an object, no references should be needed in
        # python for it still to be added to the database.
        foo = Foo()
        foo.title = u"live"
        self.store.add(foo)

        del foo
        gc.collect()

        assert self.store.find(Foo, title=u"live").one()

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

        assert obj_info.get_obj() == None

        foo = self.store.find(MyFoo, id=20).one()
        assert foo
        assert not getattr(foo, "tainted", False)

        # The object was rebuilt, so the loaded hook must have run.
        assert foo.loaded

    def test_obj_info_with_deleted_object_and_changed_event(self):
        """
        When an object is collected, the variables disable change notification
        to not create a leak. If we're holding a reference to the obj_info and
        rebuild the object, it should re-enable change notication.
        """
        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        json = self.store.get(Json, 20)
        obj_info = get_obj_info(json)
        del json
        gc.collect()
        assert obj_info.get_obj() == None

        json = self.store.get(Json, 20)
        json.data["b"] = 2
        events = []
        obj_info.event.hook("changed", lambda *args: events.append(args))

        self.store.flush()
        assert len(events) == 1

    def test_wb_flush_event_with_deleted_object_before_flush(self):
        """
        When an object is deleted before flush and it contains mutable
        variables, those variables unhook from the global event system to
        prevent a leak.
        """
        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        json = self.store.get(Json, 20)
        json.data["b"] = 2
        self.store.flush()
        del json
        gc.collect()

        json = self.store.get(Json, 20)
        del json.data["a"]
        del json

        self.store.flush()
        assert self.store._event._hooks["flush"] == set()

    def test_mutable_variable_detect_change_from_alive(self):
        """
        Changes in a mutable variable like a L{JSONVariable} are correctly
        detected, even if the object comes from the alive cache.
        """
        json = Json()
        json.data = {"x": 0}
        json.id = 4000
        self.store.add(json)
        self.store.commit()

        json = self.store.find(Json, Json.id == 4000).one()
        json.data["y"] = 1

        self.store.commit()

        json = self.store.find(Json, Json.id == 4000).one()
        assert json.data == {"x": 0, "y": 1}

    def test_wb_checkpoint_doesnt_override_changed(self):
        """
        This test ensures that we don't uselessly checkpoint when getting back
        objects from the alive cache, which would hide changed values from the
        store.
        """
        foo = self.store.get(Foo, 20)
        foo.title = u"changed"
        self.store.block_implicit_flushes()
        foo2 = self.store.find(Foo, Foo.id == 20).one()
        self.store.unblock_implicit_flushes()
        self.store.commit()

        foo3 = self.store.find(Foo, Foo.id == 20).one()
        assert foo3.title == u"changed"

    def test_obj_info_with_deleted_object_with_get(self):
        # Same thing, but using get rather than find.

        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        foo = self.store.get(Foo, 20)
        foo.tainted = True
        obj_info = get_obj_info(foo)

        del foo
        gc.collect()

        assert obj_info.get_obj() == None

        foo = self.store.get(Foo, 20)
        assert foo
        assert not getattr(foo, "tainted", False)

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

        assert obj_info.get_obj()

    def test_get_tuple(self):
        class MyFoo(Foo):
            __storm_primary__ = "title", "id"
        foo = self.store.get(MyFoo, (u"Title 30", 10))
        assert foo.id == 10
        assert foo.title == "Title 30"

        foo = self.store.get(MyFoo, (u"Title 20", 10))
        assert foo == None

    def test_of(self):
        foo = self.store.get(Foo, 10)
        assert Store.of(foo) == self.store
        assert Store.of(Foo()) == None
        assert Store.of(object()) == None

    def test_is_empty(self):
        result = self.store.find(Foo, id=300)
        assert result.is_empty() == True
        result = self.store.find(Foo, id=30)
        assert result.is_empty() == False

    def test_is_empty_strips_order_by(self):
        """
        L{ResultSet.is_empty} strips the C{ORDER BY} clause, if one is
        present, since it isn't required to actually determine if a result set
        has any matching rows.  This should provide a performance improvement
        when the ordered result set would be large.
        """
        stream = StringIO()
        self.addCleanup(debug, False)
        debug(True, stream)

        result = self.store.find(Foo, Foo.id == 300)
        result.order_by(Foo.id)
        assert True == result.is_empty()
        assert "ORDER BY" not in stream.getvalue()

    def test_is_empty_with_composed_key(self):
        result = self.store.find(Link, foo_id=300, bar_id=3000)
        assert result.is_empty() == True
        result = self.store.find(Link, foo_id=30, bar_id=300)
        assert result.is_empty() == False

    def test_is_empty_with_expression_find(self):
        result = self.store.find(Foo.title, Foo.id == 300)
        assert result.is_empty() == True
        result = self.store.find(Foo.title, Foo.id == 30)
        assert result.is_empty() == False

    def test_find_iter(self):
        result = self.store.find(Foo)

        lst = [(foo.id, foo.title) for foo in result]
        lst.sort()
        assert lst == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

    def test_find_from_cache(self):
        foo = self.store.get(Foo, 10)
        assert self.store.find(Foo, id=10).one() is foo

    def test_find_expr(self):
        result = self.store.find(Foo, Foo.id == 20,
                                 Foo.title == u"Title 20")
        assert [(foo.id, foo.title) for foo in result] == [(20, "Title 20")]

        result = self.store.find(Foo, Foo.id == 10,
                                 Foo.title == u"Title 20")
        assert [(foo.id, foo.title) for foo in result] == []

    def test_find_sql(self):
        foo = self.store.find(Foo, SQL("foo.id = 20")).one()
        assert foo.title == "Title 20"

    def test_find_str(self):
        foo = self.store.find(Foo, "foo.id = 20").one()
        assert foo.title == "Title 20"

    def test_find_keywords(self):
        result = self.store.find(Foo, id=20, title=u"Title 20")
        assert [(foo.id, foo.title) for foo in result] == [(20, u"Title 20")]

        result = self.store.find(Foo, id=10, title=u"Title 20")
        assert [(foo.id, foo.title) for foo in result] == []

    def test_find_order_by(self, *args):
        result = self.store.find(Foo).order_by(Foo.title)
        lst = [(foo.id, foo.title) for foo in result]
        assert lst == [
            (30, "Title 10"),
            (20, "Title 20"),
            (10, "Title 30"),
        ]

    def test_find_order_asc(self, *args):
        result = self.store.find(Foo).order_by(Asc(Foo.title))
        lst = [(foo.id, foo.title) for foo in result]
        assert lst == [
            (30, "Title 10"),
            (20, "Title 20"),
            (10, "Title 30"),
        ]

    def test_find_order_desc(self, *args):
        result = self.store.find(Foo).order_by(Desc(Foo.title))
        lst = [(foo.id, foo.title) for foo in result]
        assert lst == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

    def test_find_default_order_asc(self):
        class MyFoo(Foo):
            __storm_order__ = "title"

        result = self.store.find(MyFoo)
        lst = [(foo.id, foo.title) for foo in result]
        assert lst == [
            (30, "Title 10"),
            (20, "Title 20"),
            (10, "Title 30"),
        ]

    def test_find_default_order_desc(self):
        class MyFoo(Foo):
            __storm_order__ = "-title"

        result = self.store.find(MyFoo)
        lst = [(foo.id, foo.title) for foo in result]
        assert lst == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

    def test_find_default_order_with_tuple(self):
        class MyLink(Link):
            __storm_order__ = ("foo_id", "-bar_id")

        result = self.store.find(MyLink)
        lst = [(link.foo_id, link.bar_id) for link in result]
        assert lst == [
            (10, 300),
            (10, 200),
            (10, 100),
            (20, 200),
            (20, 100),
            (30, 300),
        ]

    def test_find_default_order_with_tuple_and_expr(self):
        class MyLink(Link):
            __storm_order__ = ("foo_id", Desc(Link.bar_id))

        result = self.store.find(MyLink)
        lst = [(link.foo_id, link.bar_id) for link in result]
        assert lst == [
            (10, 300),
            (10, 200),
            (10, 100),
            (20, 200),
            (20, 100),
            (30, 300),
        ]

    def test_find_index(self):
        """
        L{ResultSet.__getitem__} returns the object at the specified index.
        if a slice is used, a new L{ResultSet} is returned configured with the
        appropriate offset and limit.
        """
        foo = self.store.find(Foo).order_by(Foo.title)[0]
        assert foo.id == 30
        assert foo.title == "Title 10"

        foo = self.store.find(Foo).order_by(Foo.title)[1]
        assert foo.id == 20
        assert foo.title == "Title 20"

        foo = self.store.find(Foo).order_by(Foo.title)[2]
        assert foo.id == 10
        assert foo.title == "Title 30"

        foo = self.store.find(Foo).order_by(Foo.title)[1:][1]
        assert foo.id == 10
        assert foo.title == "Title 30"

        result = self.store.find(Foo).order_by(Foo.title)
        with pytest.raises(IndexError):
            result.__getitem__(3)

    def test_find_slice(self):
        result = self.store.find(Foo).order_by(Foo.title)[1:2]
        lst = [(foo.id, foo.title) for foo in result]
        assert lst == [(20, "Title 20")]

    def test_find_slice_offset(self):
        result = self.store.find(Foo).order_by(Foo.title)[1:]
        lst = [(foo.id, foo.title) for foo in result]
        assert lst == [(20, "Title 20"), (10, "Title 30")]

    def test_find_slice_offset_any(self):
        foo = self.store.find(Foo).order_by(Foo.title)[1:].any()
        assert foo.id == 20
        assert foo.title == "Title 20"

    def test_find_slice_offset_one(self):
        foo = self.store.find(Foo).order_by(Foo.title)[1:2].one()
        assert foo.id == 20
        assert foo.title == "Title 20"

    def test_find_slice_offset_first(self):
        foo = self.store.find(Foo).order_by(Foo.title)[1:].first()
        assert foo.id == 20
        assert foo.title == "Title 20"

    def test_find_slice_offset_last(self):
        foo = self.store.find(Foo).order_by(Foo.title)[1:].last()
        assert foo.id == 10
        assert foo.title == "Title 30"

    def test_find_slice_limit(self):
        result = self.store.find(Foo).order_by(Foo.title)[:2]
        lst = [(foo.id, foo.title) for foo in result]
        assert lst == [(30, "Title 10"), (20, "Title 20")]

    def test_find_slice_limit_last(self):
        result = self.store.find(Foo).order_by(Foo.title)[:2]
        with pytest.raises(FeatureError):
            result.last()

    def test_find_slice_slice(self):
        result = self.store.find(Foo).order_by(Foo.title)[0:2][1:3]
        lst = [(foo.id, foo.title) for foo in result]
        assert lst == [(20, "Title 20")]

        result = self.store.find(Foo).order_by(Foo.title)[:2][1:3]
        lst = [(foo.id, foo.title) for foo in result]
        assert lst == [(20, "Title 20")]

        result = self.store.find(Foo).order_by(Foo.title)[1:3][0:1]
        lst = [(foo.id, foo.title) for foo in result]
        assert lst == [(20, "Title 20")]

        result = self.store.find(Foo).order_by(Foo.title)[1:3][:1]
        lst = [(foo.id, foo.title) for foo in result]
        assert lst == [(20, "Title 20")]

        result = self.store.find(Foo).order_by(Foo.title)[5:5][1:1]
        lst = [(foo.id, foo.title) for foo in result]
        assert lst == []

    def test_find_slice_order_by(self):
        result = self.store.find(Foo)[2:]
        with pytest.raises(FeatureError):
            result.order_by(None)

        result = self.store.find(Foo)[:2]
        with pytest.raises(FeatureError):
            result.order_by(None)

    def test_find_slice_remove(self):
        result = self.store.find(Foo)[2:]
        with pytest.raises(FeatureError):
            result.remove()

        result = self.store.find(Foo)[:2]
        with pytest.raises(FeatureError):
            result.remove()

    def test_find_contains(self):
        foo = self.store.get(Foo, 10)
        result = self.store.find(Foo)
        assert foo in result
        result = self.store.find(Foo, Foo.id == 20)
        assert foo not in result
        result = self.store.find(Foo, "foo.id = 20")
        assert foo not in result

    def test_find_contains_wrong_type(self):
        foo = self.store.get(Foo, 10)
        bar = self.store.get(Bar, 200)
        with pytest.raises(TypeError):
            operator.contains(self.store.find(Foo), bar)
        with pytest.raises(TypeError):
            operator.contains(self.store.find((Foo,)), foo)
        with pytest.raises(TypeError):
            operator.contains(self.store.find(Foo), (foo,))
        with pytest.raises(TypeError):
            operator.contains(self.store.find((Foo, Bar)), (bar, foo))

    def test_find_contains_does_not_use_iter(self):
        def no_iter(self):
            raise RuntimeError()
        orig_iter = ResultSet.__iter__
        ResultSet.__iter__ = no_iter
        try:
            foo = self.store.get(Foo, 10)
            result = self.store.find(Foo)
            assert foo in result
        finally:
            ResultSet.__iter__ = orig_iter

    def test_find_contains_with_composed_key(self):
        link = self.store.get(Link, (10, 100))
        result = self.store.find(Link, Link.foo_id == 10)
        assert link in result
        result = self.store.find(Link, Link.bar_id == 200)
        assert link not in result

    def test_find_contains_with_set_expression(self):
        foo = self.store.get(Foo, 10)
        result1 = self.store.find(Foo, Foo.id == 10)
        result2 = self.store.find(Foo, Foo.id != 10)

        assert foo in result1.union(result2)
        assert foo not in result1.intersection(result2)
        assert foo in result1.intersection(result1)
        assert foo in result1.difference(result2)
        assert foo not in result1.difference(result1)

    def test_find_any(self, *args):
        """
        L{ResultSet.any} returns an arbitrary objects from the result set.
        """
        assert None != self.store.find(Foo).any()
        assert None == self.store.find(Foo, id=40).any()

    def test_find_any_strips_order_by(self):
        """
        L{ResultSet.any} strips the C{ORDER BY} clause, if one is present,
        since it isn't required.  This should provide a performance
        improvement when the ordered result set would be large.
        """
        stream = StringIO()
        self.addCleanup(debug, False)
        debug(True, stream)

        result = self.store.find(Foo, Foo.id == 300)
        result.order_by(Foo.id)
        result.any()
        assert "ORDER BY" not in stream.getvalue()

    def test_find_first(self, *args):
        with pytest.raises(UnorderedError):
            self.store.find(Foo).first()

        foo = self.store.find(Foo).order_by(Foo.title).first()
        assert foo.id == 30
        assert foo.title == "Title 10"

        foo = self.store.find(Foo).order_by(Foo.id).first()
        assert foo.id == 10
        assert foo.title == "Title 30"

        foo = self.store.find(Foo, id=40).order_by(Foo.id).first()
        assert foo == None

    def test_find_last(self, *args):
        with pytest.raises(UnorderedError):
            self.store.find(Foo).last()

        foo = self.store.find(Foo).order_by(Foo.title).last()
        assert foo.id == 10
        assert foo.title == "Title 30"

        foo = self.store.find(Foo).order_by(Foo.id).last()
        assert foo.id == 30
        assert foo.title == "Title 10"

        foo = self.store.find(Foo, id=40).order_by(Foo.id).last()
        assert foo == None

    def test_find_last_desc(self, *args):
        foo = self.store.find(Foo).order_by(Desc(Foo.title)).last()
        assert foo.id == 30
        assert foo.title == "Title 10"

        foo = self.store.find(Foo).order_by(Asc(Foo.id)).last()
        assert foo.id == 30
        assert foo.title == "Title 10"

    def test_find_one(self, *args):
        with pytest.raises(NotOneError):
            self.store.find(Foo).one()

        foo = self.store.find(Foo, id=10).one()
        assert foo.id == 10
        assert foo.title == "Title 30"

        foo = self.store.find(Foo, id=40).one()
        assert foo == None

    def test_find_count(self):
        assert self.store.find(Foo).count() == 3

    def test_find_count_after_slice(self):
        """
        When we slice a ResultSet obtained after a set operation (like union),
        we get a fresh select that doesn't modify the limit and offset
        attribute of the original ResultSet.
        """
        result1 = self.store.find(Foo, Foo.id == 10)
        result2 = self.store.find(Foo, Foo.id == 20)
        result3 = result1.union(result2)
        result3.order_by(Foo.id)
        assert result3.count() == 2

        result_slice = list(result3[:2])
        assert result3.count() == 2

    def test_find_count_column(self):
        assert self.store.find(Link).count(Link.foo_id) == 6

    def test_find_count_column_distinct(self):
        count = self.store.find(Link).count(Link.foo_id, distinct=True)
        assert count == 3

    def test_find_limit_count(self):
        result = self.store.find(Link.foo_id)
        result.config(limit=2)
        count = result.count()
        assert count == 2

    def test_find_offset_count(self):
        result = self.store.find(Link.foo_id)
        result.config(offset=3)
        count = result.count()
        assert count == 3

    def test_find_sliced_count(self):
        result = self.store.find(Link.foo_id)
        count = result[2:4].count()
        assert count == 2

    def test_find_distinct_count(self):
        result = self.store.find(Link.foo_id)
        result.config(distinct=True)
        count = result.count()
        assert count == 3

    def test_find_distinct_order_by_limit_count(self):
        result = self.store.find(Foo)
        result.order_by(Foo.title)
        result.config(distinct=True, limit=3)
        count = result.count()
        assert count == 3

    def test_find_distinct_count_multiple_columns(self):
        result = self.store.find((Link.foo_id, Link.bar_id))
        result.config(distinct=True)
        count = result.count()
        assert count == 6

    def test_find_count_column_with_implicit_distinct(self):
        result = self.store.find(Link)
        result.config(distinct=True)
        count = result.count(Link.foo_id)
        assert count == 6

    def test_find_max(self):
        assert self.store.find(Foo).max(Foo.id) == 30

    def test_find_max_expr(self):
        assert self.store.find(Foo).max(Foo.id + 1) == 31

    def test_find_max_unicode(self):
        title = self.store.find(Foo).max(Foo.title)
        assert title == "Title 30"
        assert isinstance(title, ustr)

    def test_find_max_with_empty_result_and_disallow_none(self):
        class Bar(object):
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int(allow_none=False)

        result = self.store.find(Bar, Bar.id > 1000)
        assert result.is_empty()
        assert result.max(Bar.foo_id) == None

    def test_find_min(self):
        assert self.store.find(Foo).min(Foo.id) == 10

    def test_find_min_expr(self):
        assert self.store.find(Foo).min(Foo.id - 1) == 9

    def test_find_min_unicode(self):
        title = self.store.find(Foo).min(Foo.title)
        assert title == "Title 10"
        assert isinstance(title, ustr)

    def test_find_min_with_empty_result_and_disallow_none(self):
        class Bar(object):
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int(allow_none=False)

        result = self.store.find(Bar, Bar.id > 1000)
        assert result.is_empty()
        assert result.min(Bar.foo_id) == None

    def test_find_avg(self):
        assert self.store.find(Foo).avg(Foo.id) == 20

    def test_find_avg_expr(self):
        assert self.store.find(Foo).avg(Foo.id + 10) == 30

    def test_find_avg_float(self):
        foo = Foo()
        foo.id = 15
        foo.title = u"Title 15"
        self.store.add(foo)
        assert self.store.find(Foo).avg(Foo.id) == 18.75

    def test_find_sum(self):
        assert self.store.find(Foo).sum(Foo.id) == 60

    def test_find_sum_expr(self):
        assert self.store.find(Foo).sum(Foo.id * 2) == 120

    def test_find_sum_with_empty_result_and_disallow_none(self):
        class Bar(object):
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int(allow_none=False)

        result = self.store.find(Bar, Bar.id > 1000)
        assert result.is_empty()
        assert result.sum(Bar.foo_id) == None

    def test_find_max_order_by(self):
        """Interaction between order by and aggregation shouldn't break."""
        result = self.store.find(Foo)
        assert result.order_by(Foo.id).max(Foo.id) == 30

    def test_find_get_select_expr_without_columns(self):
        """
        A L{FeatureError} is raised if L{ResultSet.get_select_expr} is called
        without a list of L{Column}s.
        """
        result = self.store.find(Foo)
        with pytest.raises(FeatureError):
            result.get_select_expr()

    def test_find_get_select_expr(self):
        """
        Only the specified L{Column}s are included in the L{Select} expression
        provided by L{ResultSet.get_select_expr}.
        """
        foo = self.store.get(Foo, 10)
        result1 = self.store.find(Foo, Foo.id <= 10)
        subselect = result1.get_select_expr(Foo.id)
        assert (Foo.id,) == subselect.columns
        result2 = self.store.find(Foo, Foo.id.is_in(subselect))
        assert [foo] == list(result2)

    def test_find_get_select_expr_with_set_expression(self):
        """
        A L{FeatureError} is raised if L{ResultSet.get_select_expr} is used
        with a L{ResultSet} that represents a set expression, such as a union.
        """
        result1 = self.store.find(Foo, Foo.id == 10)
        result2 = self.store.find(Foo, Foo.id == 20)
        result3 = result1.union(result2)
        with pytest.raises(FeatureError):
            result3.get_select_expr(Foo.id)

    def test_find_values(self):
        values = self.store.find(Foo).order_by(Foo.id).values(Foo.id)
        assert list(values) == [10, 20, 30]

        values = self.store.find(Foo).order_by(Foo.id).values(Foo.title)
        values = list(values)
        assert values == ["Title 30", "Title 20", "Title 10"]
        assert [type(value) for value in values] == [ustr, ustr, ustr]

    def test_find_multiple_values(self):
        result = self.store.find(Foo).order_by(Foo.id)
        values = result.values(Foo.id, Foo.title)
        assert list(values) == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

    def test_find_values_with_no_arguments(self):
        result = self.store.find(Foo).order_by(Foo.id)
        with pytest.raises(FeatureError):
            next(result.values())

    def test_find_slice_values(self):
        values = self.store.find(Foo).order_by(Foo.id)[1:2].values(Foo.id)
        assert list(values) == [20]

    def test_find_values_with_set_expression(self):
        """
        A L{FeatureError} is raised if L{ResultSet.values} is used with a
        L{ResultSet} that represents a set expression, such as a union.
        """
        result1 = self.store.find(Foo, Foo.id == 10)
        result2 = self.store.find(Foo, Foo.id == 20)
        result3 = result1.union(result2)
        with pytest.raises(FeatureError):
            list(result3.values(Foo.id))

    def test_find_remove(self):
        self.store.find(Foo, Foo.id == 20).remove()
        assert self.get_items() == [
            (10, "Title 30"),
            (30, "Title 10"),
        ]

    def test_find_cached(self):
        foo = self.store.get(Foo, 20)
        bar = self.store.get(Bar, 200)
        assert foo
        assert bar
        assert self.store.find(Foo).cached() == [foo]

    def test_find_cached_where(self):
        foo1 = self.store.get(Foo, 10)
        foo2 = self.store.get(Foo, 20)
        bar = self.store.get(Bar, 200)
        assert foo1
        assert foo2
        assert bar
        assert self.store.find(Foo, title=u"Title 20").cached() == [foo2]

    def test_find_cached_invalidated(self):
        foo = self.store.get(Foo, 20)
        self.store.invalidate(foo)
        assert self.store.find(Foo).cached() == [foo]

    def test_find_cached_invalidated_and_deleted(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("DELETE FROM foo WHERE id=20")
        self.store.invalidate(foo)
        # Do not look for the primary key (id), since it's able to get
        # it without touching the database. Use the title instead.
        assert self.store.find(Foo, title=u"Title 20").cached() == []

    def test_find_cached_with_info_alive_and_object_dead(self):
        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)

        foo = self.store.get(Foo, 20)
        foo.tainted = True
        obj_info = get_obj_info(foo)
        del foo
        gc.collect()
        cached = self.store.find(Foo).cached()
        assert len(cached) == 1
        foo = self.store.get(Foo, 20)
        assert not hasattr(foo, "tainted")

    def test_using_find_join(self):
        bar = self.store.get(Bar, 100)
        bar.foo_id = None

        tables = self.store.using(Foo, LeftJoin(Bar, Bar.foo_id == Foo.id))
        result = tables.find(Bar).order_by(Foo.id, Bar.id)
        lst = [bar and (bar.id, bar.title) for bar in result]
        assert lst == [
            None,
            (200, u"Title 200"),
            (300, u"Title 100"),
        ]

    def test_using_find_with_strings(self):
        foo = self.store.using("foo").find(Foo, id=10).one()
        assert foo.title == "Title 30"

        foo = self.store.using("foo", "bar").find(Foo, id=10).any()
        assert foo.title == "Title 30"

    def test_using_find_join_with_strings(self):
        bar = self.store.get(Bar, 100)
        bar.foo_id = None

        tables = self.store.using(LeftJoin("foo", "bar",
                                           "bar.foo_id = foo.id"))
        result = tables.find(Bar).order_by(Foo.id, Bar.id)
        lst = [bar and (bar.id, bar.title) for bar in result]
        assert lst == [
            None,
            (200, u"Title 200"),
            (300, u"Title 100"),
        ]

    def test_find_tuple(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None

        result = self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        result = result.order_by(Foo.id)
        lst = [(foo and (foo.id, foo.title), bar and (bar.id, bar.title))
               for (foo, bar) in result]
        assert lst == [
            ((10, u"Title 30"), (100, u"Title 300")),
            ((30, u"Title 10"), (300, u"Title 100")),
        ]

    def test_find_tuple_using(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None

        tables = self.store.using(Foo, LeftJoin(Bar, Bar.foo_id == Foo.id))
        result = tables.find((Foo, Bar)).order_by(Foo.id)
        lst = [(foo and (foo.id, foo.title), bar and (bar.id, bar.title))
               for (foo, bar) in result]
        assert lst == [
            ((10, u"Title 30"), (100, u"Title 300")),
            ((20, u"Title 20"), None),
            ((30, u"Title 10"), (300, u"Title 100")),
        ]

    def test_find_tuple_using_with_disallow_none(self):
        class Bar(object):
            __storm_table__ = "bar"
            id = Int(primary=True, allow_none=False)
            title = Unicode()
            foo_id = Int()
            foo = Reference(foo_id, Foo.id)

        bar = self.store.get(Bar, 200)
        self.store.remove(bar)

        tables = self.store.using(Foo, LeftJoin(Bar, Bar.foo_id == Foo.id))
        result = tables.find((Foo, Bar)).order_by(Foo.id)
        lst = [(foo and (foo.id, foo.title), bar and (bar.id, bar.title))
               for (foo, bar) in result]
        assert lst == [
            ((10, u"Title 30"), (100, u"Title 300")),
            ((20, u"Title 20"), None),
            ((30, u"Title 10"), (300, u"Title 100")),
        ]

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
        assert lst == [
            ((100, u"Title 300"), (100, 10)),
            ((100, u"Title 300"), (100, 20)),
            (None, None),
            ((300, u"Title 100"), (300, 10)),
            ((300, u"Title 100"), (300, 30)),
        ]

    def test_find_tuple_contains(self):
        foo = self.store.get(Foo, 10)
        bar = self.store.get(Bar, 100)
        bar200 = self.store.get(Bar, 200)
        result = self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        assert (foo, bar) in result
        assert (foo, bar200) not in result

    def test_find_tuple_contains_with_set_expression(self):
        foo = self.store.get(Foo, 10)
        bar = self.store.get(Bar, 100)
        bar200 = self.store.get(Bar, 200)
        result1 = self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        result2 = self.store.find((Foo, Bar), Bar.foo_id == Foo.id)

        assert (foo, bar) in result1.union(result2)
        assert (foo, bar) in result1.intersection(result2)
        assert (foo, bar) not in result1.difference(result2)

    def test_find_tuple_any(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None

        result = self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        foo, bar = result.order_by(Foo.id).any()
        assert foo.id == 10
        assert foo.title == u"Title 30"
        assert bar.id == 100
        assert bar.title == u"Title 300"

    def test_find_tuple_first(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None

        result = self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        foo, bar = result.order_by(Foo.id).first()
        assert foo.id == 10
        assert foo.title == u"Title 30"
        assert bar.id == 100
        assert bar.title == u"Title 300"

    def test_find_tuple_last(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None

        result = self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        foo, bar = result.order_by(Foo.id).last()
        assert foo.id == 30
        assert foo.title == u"Title 10"
        assert bar.id == 300
        assert bar.title == u"Title 100"

    def test_find_tuple_one(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None

        result = self.store.find((Foo, Bar),
                                 Bar.foo_id == Foo.id, Foo.id == 10)
        foo, bar = result.order_by(Foo.id).one()
        assert foo.id == 10
        assert foo.title == u"Title 30"
        assert bar.id == 100
        assert bar.title == u"Title 300"

    def test_find_tuple_count(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None
        result = self.store.find((Foo, Bar), Bar.foo_id == Foo.id)
        assert result.count() == 2

    def test_find_tuple_remove(self):
        result = self.store.find((Foo, Bar))
        with pytest.raises(FeatureError):
            result.remove()

    def test_find_tuple_set(self):
        result = self.store.find((Foo, Bar))
        with pytest.raises(FeatureError):
            result.set(title=u"Title 40")

    def test_find_tuple_kwargs(self):
        with pytest.raises(FeatureError):
            self.store.find((Foo, Bar), title=u"Title 10")

    def test_find_tuple_cached(self):
        result = self.store.find((Foo, Bar))
        with pytest.raises(FeatureError):
            result.cached()

    def test_find_using_cached(self):
        result = self.store.using(Foo, Bar).find(Foo)
        with pytest.raises(FeatureError):
            result.cached()

    def test_find_with_expr(self):
        result = self.store.find(Foo.title)
        assert sorted(result) == [u"Title 10", u"Title 20", u"Title 30"]

    def test_find_with_expr_uses_variable_set(self):
        result = self.store.find(FooVariable.title,
                                 FooVariable.id == 10)
        assert list(result) == [u"to_py(from_db(Title 30))"]

    def test_find_tuple_with_expr(self):
        result = self.store.find((Foo, Bar.id, Bar.title),
                                 Bar.foo_id == Foo.id)
        result.order_by(Foo.id)
        assert [
            (foo.id, foo.title, bar_id, bar_title)
            for foo, bar_id, bar_title in result
        ] == [
            (10, u"Title 30", 100, u"Title 300"),
            (20, u"Title 20", 200, u"Title 200"),
            (30, u"Title 10", 300, u"Title 100"),
        ]

    def test_find_using_with_expr(self):
        result = self.store.using(Foo).find(Foo.title)
        assert sorted(result) == [u"Title 10", u"Title 20", u"Title 30"]

    def test_find_with_expr_contains(self):
        result = self.store.find(Foo.title)
        assert u"Title 10" in result
        assert u"Title 42" not in result

    def test_find_tuple_with_expr_contains(self):
        foo = self.store.get(Foo, 10)
        result = self.store.find((Foo, Bar.title),
                                 Bar.foo_id == Foo.id)
        assert (foo, u"Title 300") in result
        assert (foo, u"Title 100") not in result

    def test_find_with_expr_contains_with_set_expression(self):
        result1 = self.store.find(Foo.title)
        result2 = self.store.find(Foo.title)
        assert u"Title 10" in result1.union(result2)
        assert u"Title 10" in result1.intersection(result2)
        assert u"Title 10" not in result1.difference(result2)

    def test_find_with_expr_remove_unsupported(self):
        result = self.store.find(Foo.title)
        with pytest.raises(FeatureError):
            result.remove()

    def test_find_tuple_with_expr_remove_unsupported(self):
        result = self.store.find((Foo, Bar.title), Bar.foo_id == Foo.id)
        with pytest.raises(FeatureError):
            result.remove()

    def test_find_with_expr_count(self):
        result = self.store.find(Foo.title)
        assert result.count() == 3

    def test_find_tuple_with_expr_count(self):
        result = self.store.find((Foo, Bar.title), Bar.foo_id == Foo.id)
        assert result.count() == 3

    def test_find_with_expr_values(self):
        result = self.store.find(Foo.title)
        assert sorted(result.values(Foo.title)) == [
            u"Title 10",
            u"Title 20",
            u"Title 30",
        ]

    def test_find_tuple_with_expr_values(self):
        result = self.store.find((Foo, Bar.title), Bar.foo_id == Foo.id)
        assert sorted(result.values(Foo.title)) == [
            u"Title 10",
            u"Title 20",
            u"Title 30",
        ]

    def test_find_with_expr_set_unsupported(self):
        result = self.store.find(Foo.title)
        with pytest.raises(FeatureError):
            result.set()

    def test_find_tuple_with_expr_set_unsupported(self):
        result = self.store.find((Foo, Bar.title), Bar.foo_id == Foo.id)
        with pytest.raises(FeatureError):
            result.set()

    def test_find_with_expr_cached_unsupported(self):
        result = self.store.find(Foo.title)
        with pytest.raises(FeatureError):
            result.cached()

    def test_find_tuple_with_expr_cached_unsupported(self):
        result = self.store.find((Foo, Bar.title), Bar.foo_id == Foo.id)
        with pytest.raises(FeatureError):
            result.cached()

    def test_find_with_expr_union(self):
        result1 = self.store.find(Foo.title, Foo.id == 10)
        result2 = self.store.find(Foo.title, Foo.id != 10)
        result = result1.union(result2)
        assert sorted(result) == [u"Title 10", u"Title 20", u"Title 30"]

    def test_find_with_expr_union_mismatch(self):
        result1 = self.store.find(Foo.title)
        result2 = self.store.find(Bar.foo_id)
        with pytest.raises(FeatureError):
            result1.union(result2)

    def test_find_tuple_with_expr_union(self):
        result1 = self.store.find(
            (Foo, Bar.title), Bar.foo_id == Foo.id, Bar.title == u"Title 100")
        result2 = self.store.find(
            (Foo, Bar.title), Bar.foo_id == Foo.id, Bar.title == u"Title 200")
        result = result1.union(result2)
        assert sorted((foo.id, title) for (foo, title) in result) == [
            (20, u"Title 200"),
            (30, u"Title 100"),
        ]

    def test_get_does_not_validate(self):
        def validator(object, attr, value):
            self.fail("validator called with arguments (%r, %r, %r)" %
                      (object, attr, value))

        class Foo(object):
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode(validator=validator)

        foo = self.store.get(Foo, 10)
        assert foo.title == "Title 30"

    def test_get_does_not_validate_default_value(self):
        def validator(object, attr, value):
            self.fail("validator called with arguments (%r, %r, %r)" %
                      (object, attr, value))

        class Foo(object):
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode(validator=validator, default=u"default value")

        foo = self.store.get(Foo, 10)
        assert foo.title == "Title 30"

    def test_find_does_not_validate(self):
        def validator(object, attr, value):
            self.fail("validator called with arguments (%r, %r, %r)" %
                      (object, attr, value))

        class Foo(object):
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode(validator=validator)

        foo = self.store.find(Foo, Foo.id == 10).one()
        assert foo.title == "Title 30"

    def test_find_group_by(self):
        result = self.store.find((Count(FooValue.id), Sum(FooValue.value1)))
        result.group_by(FooValue.value2)
        result.order_by(Count(FooValue.id), Sum(FooValue.value1))
        result = list(result)
        assert result == [(2, 2), (2, 2), (2, 3), (3, 6)]

    def test_find_group_by_table(self):
        result = self.store.find(
            (Sum(FooValue.value2), Foo), Foo.id == FooValue.foo_id)
        result.group_by(Foo)
        foo1 = self.store.get(Foo, 10)
        foo2 = self.store.get(Foo, 20)
        assert list(result) == [(5, foo1), (16, foo2)]

    def test_find_group_by_table_contains(self):
        result = self.store.find(
            (Sum(FooValue.value2), Foo), Foo.id == FooValue.foo_id)
        result.group_by(Foo)
        foo1 = self.store.get(Foo, 10)
        assert (5, foo1) in result

    def test_find_group_by_multiple_tables(self):
        result = self.store.find(
            Sum(FooValue.value2), Foo.id == FooValue.foo_id)
        result.group_by(Foo.id)
        result.order_by(Sum(FooValue.value2))
        result = list(result)
        assert result == [5, 16]

        result = self.store.find(
            (Sum(FooValue.value2), Foo), Foo.id == FooValue.foo_id)
        result.group_by(Foo)
        result.order_by(Sum(FooValue.value2))
        result = list(result)
        foo1 = self.store.get(Foo, 10)
        foo2 = self.store.get(Foo, 20)
        assert result == [(5, foo1), (16, foo2)]

        result = self.store.find(
            (Foo.id, Sum(FooValue.value2), Avg(FooValue.value1)),
            Foo.id == FooValue.foo_id)
        result.group_by(Foo.id)
        result.order_by(Foo.id)
        result = list(result)
        assert result == [(10, 5, 2), (20, 16, 1)]

    def test_find_group_by_having(self):
        result = self.store.find(
            Sum(FooValue.value2), Foo.id == FooValue.foo_id)
        result.group_by(Foo.id)
        result.having(Sum(FooValue.value2) == 5)
        assert list(result) == [5]
        result = self.store.find(
            Sum(FooValue.value2), Foo.id == FooValue.foo_id)
        result.group_by(Foo.id)
        result.having(Count() == 5)
        assert list(result) == [16]

    def test_find_having_without_group_by(self):
        result = self.store.find(FooValue)
        with pytest.raises(FeatureError):
            result.having(FooValue.value1 == 1)

    def test_find_group_by_multiple_having(self):
        result = self.store.find((Count(), FooValue.value2))
        result.group_by(FooValue.value2)
        result.having(Count() == 2, FooValue.value2 >= 3)
        result.order_by(Count(), FooValue.value2)
        list_result = list(result)
        assert list_result == [(2, 3), (2, 4)]

    def test_find_successive_group_by(self):
        result = self.store.find(Count())
        result.group_by(FooValue.value2)
        result.order_by(Count())
        list_result = list(result)
        assert list_result == [2, 2, 2, 3]
        result.group_by(FooValue.value1)
        list_result = list(result)
        assert list_result == [4, 5]

    def test_find_multiple_group_by(self):
        result = self.store.find(Count())
        result.group_by(FooValue.value2, FooValue.value1)
        result.order_by(Count())
        list_result = list(result)
        assert list_result == [1, 1, 2, 2, 3]

    def test_find_multiple_group_by_with_having(self):
        result = self.store.find((Count(), FooValue.value2))
        result.group_by(FooValue.value2, FooValue.value1).having(Count() == 2)
        result.order_by(Count(), FooValue.value2)
        list_result = list(result)
        assert list_result == [(2, 3), (2, 4)]

    def test_find_group_by_avg(self):
        result = self.store.find((Count(FooValue.id), Sum(FooValue.value1)))
        result.group_by(FooValue.value2)
        with pytest.raises(FeatureError):
            result.avg(FooValue.value2)

    def test_find_group_by_values(self):
        result = self.store.find(
            (Sum(FooValue.value2), Foo), Foo.id == FooValue.foo_id)
        result.group_by(Foo)
        result.order_by(Foo.title)
        result = list(result.values(Foo.title))
        assert result == [u'Title 20', u'Title 30']

    def test_find_group_by_union(self):
        result1 = self.store.find(Foo, id=30)
        result2 = self.store.find(Foo, id=10)
        result3 = result1.union(result2)
        with pytest.raises(FeatureError):
            result3.group_by(Foo.title)

    def test_find_group_by_remove(self):
        result = self.store.find((Count(FooValue.id), Sum(FooValue.value1)))
        result.group_by(FooValue.value2)
        with pytest.raises(FeatureError):
            result.remove()

    def test_find_group_by_set(self):
        result = self.store.find((Count(FooValue.id), Sum(FooValue.value1)))
        result.group_by(FooValue.value2)
        with pytest.raises(FeatureError):
            result.set(FooValue.value1 == 1)

    def test_add_commit(self):
        foo = Foo()
        foo.id = 40
        foo.title = u"Title 40"

        self.store.add(foo)

        assert self.get_committed_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

        self.store.commit()

        assert self.get_committed_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
            (40, "Title 40"),
        ]

    def test_add_rollback_commit(self):
        foo = Foo()
        foo.id = 40
        foo.title = u"Title 40"

        self.store.add(foo)
        self.store.rollback()

        assert self.store.get(Foo, 3) is None
        assert self.get_committed_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

        self.store.commit()
        assert self.get_committed_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

    def test_add_get(self):
        foo = Foo()
        foo.id = 40
        foo.title = u"Title 40"

        self.store.add(foo)

        old_foo = foo

        foo = self.store.get(Foo, 40)

        assert foo.id == 40
        assert foo.title == "Title 40"

        assert foo is old_foo

    def test_add_find(self):
        foo = Foo()
        foo.id = 40
        foo.title = u"Title 40"

        self.store.add(foo)

        old_foo = foo

        foo = self.store.find(Foo, Foo.id == 40).one()

        assert foo.id == 40
        assert foo.title == "Title 40"

        assert foo is old_foo

    def test_add_twice(self):
        foo = Foo()
        self.store.add(foo)
        self.store.add(foo)
        assert Store.of(foo) == self.store

    def test_add_loaded(self):
        foo = self.store.get(Foo, 10)
        self.store.add(foo)
        assert Store.of(foo) == self.store

    def test_add_twice_to_wrong_store(self):
        foo = Foo()
        self.store.add(foo)
        with pytest.raises(WrongStoreError):
            Store(self.database).add(foo)

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
        assert bar.title == "Title 500"

    def test_add_completely_undefined(self):
        foo = Foo()
        self.store.add(foo)
        self.store.flush()
        assert type(foo.id) == int
        assert foo.title == u"Default Title"

    def test_add_uuid(self):
        unique_id = self.store.add(UniqueID(uuid4()))
        assert unique_id == self.store.find(UniqueID).one()

    def test_remove_commit(self):
        foo = self.store.get(Foo, 20)
        self.store.remove(foo)
        assert Store.of(foo) == self.store
        self.store.flush()
        assert Store.of(foo) == None

        assert self.get_items() == [
            (10, "Title 30"),
            (30, "Title 10"),
        ]
        assert self.get_committed_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

        self.store.commit()
        assert Store.of(foo) == None
        assert self.get_committed_items() == [
            (10, "Title 30"),
            (30, "Title 10"),
        ]

    def test_remove_rollback_update(self):
        foo = self.store.get(Foo, 20)

        self.store.remove(foo)
        self.store.rollback()

        foo.title = u"Title 200"

        self.store.flush()

        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 200"),
            (30, "Title 10"),
        ]

    def test_remove_flush_rollback_update(self):
        foo = self.store.get(Foo, 20)

        self.store.remove(foo)
        self.store.flush()
        self.store.rollback()

        foo.title = u"Title 200"

        self.store.flush()

        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

    def test_remove_add_update(self):
        foo = self.store.get(Foo, 20)

        self.store.remove(foo)
        self.store.add(foo)

        foo.title = u"Title 200"

        self.store.flush()

        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 200"),
            (30, "Title 10"),
        ]

    def test_remove_flush_add_update(self):
        foo = self.store.get(Foo, 20)

        self.store.remove(foo)
        self.store.flush()
        self.store.add(foo)

        foo.title = u"Title 200"

        self.store.flush()

        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 200"),
            (30, "Title 10"),
        ]

    def test_remove_twice(self):
        foo = self.store.get(Foo, 10)
        self.store.remove(foo)
        self.store.remove(foo)

    def test_remove_unknown(self):
        foo = Foo()
        with pytest.raises(WrongStoreError):
            self.store.remove(foo)

    def test_remove_from_wrong_store(self):
        foo = self.store.get(Foo, 20)
        with pytest.raises(WrongStoreError):
            Store(self.database).remove(foo)

    def test_wb_remove_flush_update_isnt_dirty(self):
        foo = self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        self.store.remove(foo)
        self.store.flush()

        foo.title = u"Title 200"

        assert obj_info not in self.store._dirty

    def test_wb_remove_rollback_isnt_dirty(self):
        foo = self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        self.store.remove(foo)
        self.store.rollback()

        assert obj_info not in self.store._dirty

    def test_wb_remove_flush_rollback_isnt_dirty(self):
        foo = self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        self.store.remove(foo)
        self.store.flush()
        self.store.rollback()

        assert obj_info not in self.store._dirty

    def test_add_rollback_not_in_store(self):
        foo = Foo()
        foo.id = 40
        foo.title = u"Title 40"

        self.store.add(foo)
        self.store.rollback()

        assert Store.of(foo) == None

    def test_update_flush_commit(self):
        foo = self.store.get(Foo, 20)
        foo.title = u"Title 200"

        pre_update = [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]
        post_update = [
            (10, "Title 30"),
            (20, "Title 200"),
            (30, "Title 10"),
        ]

        assert self.get_items() == pre_update
        assert self.get_committed_items() == pre_update

        self.store.flush()
        assert self.get_items() == post_update
        assert self.get_committed_items() == pre_update

        self.store.commit()
        assert self.get_items() == post_update
        assert self.get_committed_items() == post_update

    def test_update_flush_reload_rollback(self):
        foo = self.store.get(Foo, 20)
        foo.title = u"Title 200"
        self.store.flush()
        self.store.reload(foo)
        self.store.rollback()
        assert foo.title == "Title 20"

    def test_update_commit(self):
        foo = self.store.get(Foo, 20)
        foo.title = u"Title 200"

        self.store.commit()

        assert self.get_committed_items() == [
            (10, "Title 30"),
            (20, "Title 200"),
            (30, "Title 10"),
        ]

    def test_update_commit_twice(self):
        foo = self.store.get(Foo, 20)
        foo.title = u"Title 200"
        self.store.commit()
        foo.title = u"Title 2000"
        self.store.commit()

        assert self.get_committed_items() == [
            (10, "Title 30"),
            (20, "Title 2000"),
            (30, "Title 10"),
        ]

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
        assert bar.title == "Title 500"

    def test_update_primary_key(self):
        foo = self.store.get(Foo, 20)
        foo.id = 25

        self.store.commit()

        assert self.get_committed_items() == [
            (10, "Title 30"),
            (25, "Title 20"),
            (30, "Title 10"),
        ]

        # Update twice to see if the notion of primary key for the
        # existent object was updated as well.

        foo.id = 27

        self.store.commit()
        assert self.get_committed_items() == [
            (10, "Title 30"),
            (27, "Title 20"),
            (30, "Title 10"),
        ]

        # Ensure only the right ones are there.
        assert self.store.get(Foo, 27) is foo
        assert self.store.get(Foo, 25) is None
        assert self.store.get(Foo, 20) is None

    def test_update_primary_key_exchange(self):
        foo1 = self.store.get(Foo, 10)
        foo2 = self.store.get(Foo, 30)

        foo1.id = 40
        self.store.flush()
        foo2.id = 10
        self.store.flush()
        foo1.id = 30

        assert self.store.get(Foo, 30) is foo1
        assert self.store.get(Foo, 10) is foo2

        self.store.commit()

        assert self.get_committed_items() == [
            (10, "Title 10"),
            (20, "Title 20"),
            (30, "Title 30"),
        ]

    def test_wb_update_not_dirty_after_flush(self):
        foo = self.store.get(Foo, 20)
        foo.title = u"Title 200"

        self.store.flush()

        # If changes get committed even with the notification disabled,
        # it means the dirty flag isn't being cleared.

        self.store._disable_change_notification(get_obj_info(foo))

        foo.title = u"Title 2000"

        self.store.flush()

        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 200"),
            (30, "Title 10"),
        ]

    def test_update_find(self):
        foo = self.store.get(Foo, 20)
        foo.title = u"Title 200"

        result = self.store.find(Foo, Foo.title == u"Title 200")
        assert result.one() is foo

    def test_update_get(self):
        foo = self.store.get(Foo, 20)
        foo.id = 200
        assert self.store.get(Foo, 200) is foo

    def test_add_update(self):
        foo = Foo()
        foo.id = 40
        foo.title = u"Title 40"

        self.store.add(foo)

        foo.title = u"Title 400"

        self.store.flush()
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
            (40, "Title 400"),
        ]

    def test_add_remove_add(self):
        foo = Foo()
        foo.id = 40
        foo.title = u"Title 40"

        self.store.add(foo)
        self.store.remove(foo)

        assert Store.of(foo) == None

        foo.title = u"Title 400"

        self.store.add(foo)

        foo.id = 400

        self.store.commit()

        assert Store.of(foo) == self.store
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
            (400, "Title 400"),
        ]

        assert self.store.get(Foo, 400) is foo

    def test_wb_add_remove_add(self):
        foo = Foo()
        obj_info = get_obj_info(foo)
        self.store.add(foo)
        assert obj_info in self.store._dirty
        self.store.remove(foo)
        assert obj_info not in self.store._dirty
        self.store.add(foo)
        assert obj_info in self.store._dirty
        assert Store.of(foo) is self.store

    def test_wb_update_remove_add(self):
        foo = self.store.get(Foo, 20)
        foo.title = u"Title 200"

        obj_info = get_obj_info(foo)

        self.store.remove(foo)
        self.store.add(foo)

        assert obj_info in self.store._dirty

    def test_commit_autoreloads(self):
        foo = self.store.get(Foo, 20)
        assert foo.title == "Title 20"
        self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        assert foo.title == "Title 20"
        self.store.commit()
        assert foo.title == "New Title"

    def test_commit_invalidates(self):
        foo = self.store.get(Foo, 20)
        assert foo
        self.store.execute("DELETE FROM foo WHERE id=20")
        assert self.store.get(Foo, 20) == foo
        self.store.commit()
        assert self.store.get(Foo, 20) is None

    def test_rollback_autoreloads(self):
        foo = self.store.get(Foo, 20)
        assert foo.title == "Title 20"
        self.store.rollback()
        self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        assert foo.title == "New Title"

    def test_rollback_invalidates(self):
        foo = self.store.get(Foo, 20)
        assert foo
        assert self.store.get(Foo, 20) == foo
        self.store.rollback()
        self.store.execute("DELETE FROM foo WHERE id=20")
        assert self.store.get(Foo, 20) is None

    def test_sub_class(self):
        class SubFoo(Foo):
            id = Float(primary=True)

        foo1 = self.store.get(Foo, 20)
        foo2 = self.store.get(SubFoo, 20)

        assert foo1.id == 20
        assert foo2.id == 20
        assert type(foo1.id) == int
        assert type(foo2.id) == float

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

        assert [(foo.id, foo.title) for foo in result] == [
            (20, "Title 20"),
            (20, "Title 20"),
        ]

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

        assert [(foo.id, foo.title) for foo in result] == [
            (20, "Title 20"),
        ]

    def test_sub_select(self):
        foo = self.store.find(Foo, Foo.id == Select(SQL("20"))).one()
        assert foo
        assert foo.id == 20
        assert foo.title == "Title 20"

    def test_cache_has_improper_object(self):
        foo = self.store.get(Foo, 20)
        self.store.remove(foo)
        self.store.commit()

        self.store.execute("INSERT INTO foo VALUES (20, 'Title 20')")

        assert self.store.get(Foo, 20) is not foo

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

        assert self.store.get(Foo, 20) is foo

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

        assert loaded == [(20, "Title 20")]
        assert foo.title == "Title 200"
        assert foo.some_attribute == 1

        foo.some_attribute = 2

        self.store.flush()

        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 200"),
            (30, "Title 10"),
        ]

        self.store.rollback()

        assert foo.title == "Title 20"
        assert foo.some_attribute == 2

    def test_flush_hook(self):

        class MyFoo(Foo):
            counter = 0
            def __storm_pre_flush__(self):
                if self.counter == 0:
                    self.title = u"Flushing: %s" % self.title
                self.counter += 1

        foo = self.store.get(MyFoo, 20)

        assert foo.title == "Title 20"
        self.store.flush()
        assert foo.title == "Title 20"  # It wasn't dirty.
        foo.title = u"Something"
        self.store.flush()
        assert foo.title == "Flushing: Something"

        # It got in the database, because it was flushed *twice* (the
        # title was changed after flushed, and thus the object got dirty
        # again).
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Flushing: Something"),
            (30, "Title 10"),
        ]

        # This shouldn't do anything, because the object is clean again.
        foo.counter = 0
        self.store.flush()
        assert foo.title == "Flushing: Something"

    def test_flush_hook_all(self):

        class MyFoo(Foo):
            def __storm_pre_flush__(self):
                other = [foo1, foo2][foo1 is self]
                other.title = u"Changed in hook: " + other.title

        foo1 = self.store.get(MyFoo, 10)
        foo2 = self.store.get(MyFoo, 20)
        foo1.title = u"Changed"
        self.store.flush()

        assert foo1.title == "Changed in hook: Changed"
        assert foo2.title == "Changed in hook: Title 20"

    def test_flushed_hook(self):

        class MyFoo(Foo):
            done = False
            def __storm_flushed__(self):
                if not self.done:
                    self.done = True
                    self.title = u"Flushed: %s" % self.title

        foo = self.store.get(MyFoo, 20)

        assert foo.title == "Title 20"
        self.store.flush()
        assert foo.title == "Title 20"  # It wasn't dirty.
        foo.title = u"Something"
        self.store.flush()
        assert foo.title == "Flushed: Something"

        # It got in the database, because it was flushed *twice* (the
        # title was changed after flushed, and thus the object got dirty
        # again).
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Flushed: Something"),
            (30, "Title 10"),
        ]

        # This shouldn't do anything, because the object is clean again.
        foo.done = False
        self.store.flush()
        assert foo.title == "Flushed: Something"

    def test_retrieve_default_primary_key(self):
        foo = Foo()
        foo.title = u"Title 40"
        self.store.add(foo)
        self.store.flush()
        assert foo.id != None
        assert self.store.get(Foo, foo.id) is foo

    def test_retrieve_default_value(self):
        foo = Foo()
        foo.id = 40
        self.store.add(foo)
        self.store.flush()
        assert foo.title == "Default Title"

    def test_retrieve_null_when_no_default(self):
        bar = Bar()
        bar.id = 400
        self.store.add(bar)
        self.store.flush()
        assert bar.title == None

    def test_wb_remove_prop_not_dirty(self):
        foo = self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        del foo.title
        assert obj_info not in self.store._dirty

    def test_flush_with_removed_prop(self):
        foo = self.store.get(Foo, 20)
        del foo.title
        self.store.flush()
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

    def test_flush_with_removed_prop_forced_dirty(self):
        foo = self.store.get(Foo, 20)
        del foo.title
        foo.id = 40
        foo.id = 20
        self.store.flush()
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

    def test_flush_with_removed_prop_really_dirty(self):
        foo = self.store.get(Foo, 20)
        del foo.title
        foo.id = 25
        self.store.flush()
        assert self.get_items() == [
            (10, "Title 30"),
            (25, "Title 20"),
            (30, "Title 10"),
        ]

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
        with pytest.raises(RuntimeError):
            self.store.get(Foo, 20)

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
        with pytest.raises(RuntimeError):
            self.store.get(Foo, 20)

    def test_block_access(self):
        """Access to the store is blocked by block_access()."""
        # The set_blocked() method blocks access to the connection.
        self.store.block_access()
        with pytest.raises(ConnectionBlockedError):
            self.store.execute("SELECT 1")
        with pytest.raises(ConnectionBlockedError):
            self.store.commit()
        # The rollback method is not blocked.
        self.store.rollback()
        self.store.unblock_access()
        self.store.execute("SELECT 1")

    def test_reload(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("UPDATE foo SET title='Title 40' WHERE id=20")
        assert foo.title == "Title 20"
        self.store.reload(foo)
        assert foo.title == "Title 40"

    def test_reload_not_changed(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("UPDATE foo SET title='Title 40' WHERE id=20")
        self.store.reload(foo)
        for variable in iter_values(get_obj_info(foo).variables):
            assert not variable.has_changed()

    def test_reload_new(self):
        foo = Foo()
        foo.id = 40
        foo.title = u"Title 40"
        with pytest.raises(WrongStoreError):
            self.store.reload(foo)

    def test_reload_new_unflushed(self):
        foo = Foo()
        foo.id = 40
        foo.title = u"Title 40"
        self.store.add(foo)
        with pytest.raises(NotFlushedError):
            self.store.reload(foo)

    def test_reload_removed(self):
        foo = self.store.get(Foo, 20)
        self.store.remove(foo)
        self.store.flush()
        with pytest.raises(WrongStoreError):
            self.store.reload(foo)

    def test_reload_unknown(self):
        foo = self.store.get(Foo, 20)
        store = self.create_store()
        with pytest.raises(WrongStoreError):
            store.reload(foo)

    def test_wb_reload_not_dirty(self):
        foo = self.store.get(Foo, 20)
        obj_info = get_obj_info(foo)
        foo.title = u"Title 40"
        self.store.reload(foo)
        assert obj_info not in self.store._dirty

    def test_find_set_empty(self):
        self.store.find(Foo, title=u"Title 20").set()
        foo = self.store.get(Foo, 20)
        assert foo.title == "Title 20"

    def test_find_set(self):
        self.store.find(Foo, title=u"Title 20").set(title=u"Title 40")
        foo = self.store.get(Foo, 20)
        assert foo.title == "Title 40"

    def test_find_set_with_func_expr(self):
        self.store.find(Foo, title=u"Title 20").set(title=Lower(u"Title 40"))
        foo = self.store.get(Foo, 20)
        assert foo.title == "title 40"

    def test_find_set_equality_with_func_expr(self):
        self.store.find(Foo, title=u"Title 20").set(
            Foo.title == Lower(u"Title 40"))
        foo = self.store.get(Foo, 20)
        assert foo.title == "title 40"

    def test_find_set_column(self):
        self.store.find(Bar, title=u"Title 200").set(foo_id=Bar.id)
        bar = self.store.get(Bar, 200)
        assert bar.foo_id == 200

    def test_find_set_expr(self):
        self.store.find(Foo, title=u"Title 20").set(Foo.title == u"Title 40")
        foo = self.store.get(Foo, 20)
        assert foo.title == "Title 40"

    def test_find_set_none(self):
        self.store.find(Foo, title=u"Title 20").set(title=None)
        foo = self.store.get(Foo, 20)
        assert foo.title == None

    def test_find_set_expr_column(self):
        self.store.find(Bar, id=200).set(Bar.foo_id == Bar.id)
        bar = self.store.get(Bar, 200)
        assert bar.id == 200
        assert bar.foo_id == 200

    def test_find_set_on_cached(self):
        foo1 = self.store.get(Foo, 20)
        foo2 = self.store.get(Foo, 30)
        self.store.find(Foo, id=20).set(id=40)
        assert foo1.id == 40
        assert foo2.id == 30

    def test_find_set_expr_on_cached(self):
        bar = self.store.get(Bar, 200)
        self.store.find(Bar, id=200).set(Bar.foo_id == Bar.id)
        assert bar.id == 200
        assert bar.foo_id == 200

    def test_find_set_none_on_cached(self):
        foo = self.store.get(Foo, 20)
        self.store.find(Foo, title=u"Title 20").set(title=None)
        assert foo.title == None

    def test_find_set_on_cached_but_removed(self):
        foo1 = self.store.get(Foo, 20)
        foo2 = self.store.get(Foo, 30)
        self.store.remove(foo1)
        self.store.find(Foo, id=20).set(id=40)
        assert foo1.id == 20
        assert foo2.id == 30

    def test_find_set_on_cached_unsupported_python_expr(self):
        foo1 = self.store.get(Foo, 20)
        foo2 = self.store.get(Foo, 30)
        self.store.find(
            Foo, Foo.id == Select(SQL("20"))).set(title=u"Title 40")
        assert foo1.title == "Title 40"
        assert foo2.title == "Title 10"

    def test_find_set_expr_unsupported(self):
        result = self.store.find(Foo, title=u"Title 20")
        with pytest.raises(FeatureError):
            result.set(Foo.title > u"Title 40")

    def test_find_set_expr_unsupported_without_column(self):
        result = self.store.find(Foo, title=u"Title 20")
        with pytest.raises(FeatureError):
            result.set(Eq(object(), IntVariable(1)))

    def test_find_set_expr_unsupported_without_expr_or_variable(self):
        result = self.store.find(Foo, title=u"Title 20")
        with pytest.raises(FeatureError):
            result.set(Eq(Foo.id, object()))

    def test_find_set_expr_unsupported_autoreloads(self):
        bar1 = self.store.get(Bar, 200)
        bar2 = self.store.get(Bar, 300)
        self.store.find(Bar, id=Select(SQL("200"))).set(title=u"Title 400")
        bar1_vars = get_obj_info(bar1).variables
        bar2_vars = get_obj_info(bar2).variables
        assert bar1_vars[Bar.title].get_lazy() == AutoReload
        assert bar2_vars[Bar.title].get_lazy() == AutoReload
        assert bar1_vars[Bar.foo_id].get_lazy() == None
        assert bar2_vars[Bar.foo_id].get_lazy() == None
        assert bar1.title == "Title 400"
        assert bar2.title == "Title 100"

    def test_find_set_expr_unsupported_mixed_autoreloads(self):
        # For an expression that does not compile (eg:
        # ResultSet.cached() raises a CompileError), while setting
        # cached entries' columns to AutoReload, if objects of
        # different types could be found in the cache then a KeyError
        # would happen if some object did not have a matching
        # column. See Bug #328603 for more info.
        foo1 = self.store.get(Foo, 20)
        bar1 = self.store.get(Bar, 200)
        self.store.find(Bar, id=Select(SQL("200"))).set(title=u"Title 400")
        foo1_vars = get_obj_info(foo1).variables
        bar1_vars = get_obj_info(bar1).variables
        assert foo1_vars[Foo.title].get_lazy() != AutoReload
        assert bar1_vars[Bar.title].get_lazy() == AutoReload
        assert bar1_vars[Bar.foo_id].get_lazy() == None
        assert foo1.title == "Title 20"
        assert bar1.title == "Title 400"

    def test_find_set_autoreloads_with_func_expr(self):
        # In the process of fixing this bug, we've temporarily
        # introduced another bug: the expression would be called
        # twice. We've used an expression that increments the value by
        # one here to see if that case is triggered. In the buggy
        # bugfix, the value would end up being incremented by two due
        # to misfiring two updates.
        foo1 = self.store.get(FooValue, 1)
        assert foo1.value1 == 2
        self.store.find(FooValue, id=1).set(value1=SQL("value1 + 1"))
        foo1_vars = get_obj_info(foo1).variables
        assert foo1_vars[FooValue.value1].get_lazy() == AutoReload
        assert foo1.value1 == 3

    def test_find_set_equality_autoreloads_with_func_expr(self):
        foo1 = self.store.get(FooValue, 1)
        assert foo1.value1 == 2
        self.store.find(FooValue, id=1).set(
            FooValue.value1 == SQL("value1 + 1"))
        foo1_vars = get_obj_info(foo1).variables
        assert foo1_vars[FooValue.value1].get_lazy() == AutoReload
        assert foo1.value1 == 3

    def test_wb_find_set_checkpoints(self):
        bar = self.store.get(Bar, 200)
        self.store.find(Bar, id=200).set(title=u"Title 400")
        self.store._connection.execute("UPDATE bar SET "
                                       "title='Title 500' "
                                       "WHERE id=200")
        # When not checkpointing, this flush will set title again.
        self.store.flush()
        self.store.reload(bar)
        assert bar.title == "Title 500"

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
        assert not hasattr(foo, "tainted")
        assert foo.title == "Title 40"

    def test_reference(self):
        bar = self.store.get(Bar, 100)
        assert bar.foo
        assert bar.foo.title == "Title 30"

    def test_reference_explicitly_with_wrapper(self):
        bar = self.store.get(Bar, 100)
        foo = Bar.foo.__get__(Wrapper(bar))
        assert foo
        assert foo.title == "Title 30"

    def test_reference_break_on_local_diverged(self):
        bar = self.store.get(Bar, 100)
        assert bar.foo
        bar.foo_id = 40
        assert bar.foo == None

    def test_reference_break_on_remote_diverged(self):
        bar = self.store.get(Bar, 100)
        bar.foo.id = 40
        assert bar.foo == None

    def test_reference_break_on_local_diverged_by_lazy(self):
        bar = self.store.get(Bar, 100)
        assert bar.foo.id == 10
        bar.foo_id = SQL("20")
        assert bar.foo.id == 20

    def test_reference_remote_leak_on_flush_with_changed(self):
        """
        "changed" events only hold weak references to remote infos object, thus
        not creating a leak when unhooked.
        """
        self.get_cache(self.store).set_size(0)
        bar = self.store.get(Bar, 100)
        bar.foo.title = u"Changed title"
        bar_ref = weakref.ref(get_obj_info(bar))
        foo = bar.foo
        del bar
        self.store.flush()
        gc.collect()
        assert bar_ref() == None

    def test_reference_remote_leak_on_flush_with_removed(self):
        """
        "removed" events only hold weak references to remote infos objects,
        thus not creating a leak when unhooked.
        """
        self.get_cache(self.store).set_size(0)
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        foo = self.store.get(MyFoo, 10)
        foo.bar.title = u"Changed title"
        foo_ref = weakref.ref(get_obj_info(foo))
        bar = foo.bar
        del foo
        self.store.flush()
        gc.collect()
        assert foo_ref() is None

    def test_reference_break_on_remote_diverged_by_lazy(self):
        class MyBar(Bar):
            pass
        MyBar.foo = Reference(MyBar.title, Foo.title)
        bar = self.store.get(MyBar, 100)
        bar.title = u"Title 30"
        self.store.flush()
        assert bar.foo.id == 10
        bar.foo.title = SQL("'Title 40'")
        assert bar.foo is None
        assert self.store.find(Foo, title=u"Title 30").one() is None
        assert self.store.get(Foo, 10).title == u"Title 40"

    def test_reference_on_non_primary_key(self):
        self.store.execute("INSERT INTO bar VALUES (400, 40, 'Title 30')")
        class MyBar(Bar):
            foo = Reference(Bar.title, Foo.title)

        bar = self.store.get(Bar, 400)
        assert bar.title == "Title 30"
        assert bar.foo == None

        mybar = self.store.get(MyBar, 400)
        assert mybar.title == "Title 30"
        assert mybar.foo != None
        assert mybar.foo.id == 10
        assert mybar.foo.title == "Title 30"

    def test_new_reference(self):
        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        bar.foo_id = 10

        assert bar.foo == None

        self.store.add(bar)

        assert bar.foo
        assert bar.foo.title == "Title 30"

    def test_set_reference(self):
        bar = self.store.get(Bar, 100)
        assert bar.foo.id == 10
        foo = self.store.get(Foo, 30)
        bar.foo = foo
        assert bar.foo.id == 30
        result = self.store.execute("SELECT foo_id FROM bar WHERE id=100")
        assert result.get_one() == (30,)

    def test_set_reference_explicitly_with_wrapper(self):
        bar = self.store.get(Bar, 100)
        assert bar.foo.id == 10
        foo = self.store.get(Foo, 30)
        Bar.foo.__set__(Wrapper(bar), Wrapper(foo))
        assert bar.foo.id == 30
        result = self.store.execute("SELECT foo_id FROM bar WHERE id=100")
        assert result.get_one() == (30,)

    def test_reference_assign_remote_key(self):
        bar = self.store.get(Bar, 100)
        assert bar.foo.id == 10
        bar.foo = 30
        assert bar.foo_id == 30
        assert bar.foo.id == 30
        result = self.store.execute("SELECT foo_id FROM bar WHERE id=100")
        assert result.get_one() == (30,)

    def test_reference_on_added(self):
        foo = Foo()
        foo.title = u"Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        bar.foo = foo
        self.store.add(bar)

        assert bar.foo.id == None
        assert bar.foo.title == "Title 40"


        self.store.flush()

        assert bar.foo.id
        assert bar.foo.title == "Title 40"

        result = self.store.execute("SELECT foo.title FROM foo, bar "
                                    "WHERE bar.id=400 AND "
                                    "foo.id = bar.foo_id")
        assert result.get_one() == ("Title 40",)

    def test_reference_on_added_with_autoreload_key(self):
        foo = Foo()
        foo.title = u"Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        bar.foo = foo
        self.store.add(bar)

        assert bar.foo.id == None
        assert bar.foo.title == "Title 40"

        foo.id = AutoReload

        # Variable shouldn't be autoreloaded yet.
        obj_info = get_obj_info(foo)
        assert obj_info.variables[Foo.id].get_lazy() == AutoReload

        assert type(foo.id) == int

        self.store.flush()

        assert bar.foo.id
        assert bar.foo.title == "Title 40"

        result = self.store.execute("SELECT foo.title FROM foo, bar "
                                    "WHERE bar.id=400 AND "
                                    "foo.id = bar.foo_id")
        assert result.get_one() == ("Title 40",)

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

        assert type(bar.id) == int
        assert foo.id == None

    def test_reference_assign_none_with_unseen(self):
        bar = self.store.get(Bar, 200)
        bar.foo = None
        assert bar.foo == None

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

        assert bar.foo.id == None
        assert bar.foo.title == "Title 40"
        assert bar.title == "Title 40"

        self.store.flush()

        assert bar.foo.id
        assert bar.foo.title == "Title 40"

        result = self.store.execute("SELECT foo.title FROM foo, bar "
                                    "WHERE bar.id=400 AND "
                                    "foo.id = bar.foo_id")
        assert result.get_one() == ("Title 40",)

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

        assert bar.foo_id == 20
        assert bar.foo.id == 20
        assert bar.title == "Title 20"
        assert bar.foo.title == "Title 20"

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
        assert bar.foo_id == 40
        foo.id = 50
        assert bar.foo_id == 50
        foo.id = 60
        assert bar.foo_id == 60

        self.store.flush()

        foo.id = 70
        assert bar.foo_id == 60

    def test_reference_on_added_unsets_original_key(self):
        foo = Foo()
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.foo_id = 40
        bar.foo = foo

        assert bar.foo_id == None

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
        assert bar.foo_id == None
        foo2.id = 50
        assert bar.foo_id == 50

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
        assert bar.foo_id == 40

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

        assert bar.foo == None

        foo.id = 40

        assert bar.foo_id == None

    def test_reference_on_added_no_local_store(self):
        foo = Foo()
        foo.title = u"Title 40"
        self.store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        bar.foo = foo

        assert Store.of(bar) == self.store
        assert Store.of(foo) == self.store

    def test_reference_on_added_no_remote_store(self):
        foo = Foo()
        foo.title = u"Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        self.store.add(bar)

        bar.foo = foo

        assert Store.of(bar) == self.store
        assert Store.of(foo) == self.store

    def test_reference_on_added_no_store(self):
        foo = Foo()
        foo.title = u"Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        bar.foo = foo

        self.store.add(bar)

        assert Store.of(bar) == self.store
        assert Store.of(foo) == self.store

        self.store.flush()

        assert type(bar.foo_id) == int

    def test_reference_on_added_no_store_2(self):
        foo = Foo()
        foo.title = u"Title 40"

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        bar.foo = foo

        self.store.add(foo)

        assert Store.of(bar) == self.store
        assert Store.of(foo) == self.store

        self.store.flush()

        assert type(bar.foo_id) == int

    def test_reference_on_added_wrong_store(self):
        store = self.create_store()

        foo = Foo()
        foo.title = u"Title 40"
        store.add(foo)

        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"
        self.store.add(bar)

        with pytest.raises(WrongStoreError):
            bar.foo = foo

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

        assert Store.of(bar) == self.store
        assert Store.of(foo1) == store

    def test_reference_on_removed_wont_add_back(self):
        bar = self.store.get(Bar, 200)
        foo = self.store.get(Foo, bar.foo_id)

        self.store.remove(bar)

        assert bar.foo == foo
        self.store.flush()

        assert Store.of(bar) == None
        assert Store.of(foo) == self.store

    def test_reference_equals(self):
        foo = self.store.get(Foo, 10)

        bar = self.store.find(Bar, foo=foo).one()
        assert bar
        assert bar.foo == foo

        bar = self.store.find(Bar, foo=foo.id).one()
        assert bar
        assert bar.foo == foo

    def test_reference_equals_none(self):
        result = list(self.store.find(SelfRef, selfref=None))
        assert len(result) == 2
        assert result[0].selfref == None
        assert result[1].selfref == None

    def test_reference_equals_with_composed_key(self):
        # Interesting case of self-reference.
        class LinkWithRef(Link):
            myself = Reference((Link.foo_id, Link.bar_id),
                               (Link.foo_id, Link.bar_id))

        link = self.store.find(LinkWithRef, foo_id=10, bar_id=100).one()
        myself = self.store.find(LinkWithRef, myself=link).one()
        assert link == myself

        myself = self.store.find(LinkWithRef,
                                 myself=(link.foo_id, link.bar_id)).one()
        assert link == myself

    def test_reference_equals_with_wrapped(self):
        foo = self.store.get(Foo, 10)

        bar = self.store.find(Bar, foo=Wrapper(foo)).one()
        assert bar
        assert bar.foo == foo

    def test_reference_not_equals(self):
        foo = self.store.get(Foo, 10)

        result = self.store.find(Bar, Bar.foo != foo)
        assert [200 == 300], sorted(bar.id for bar in result)

    def test_reference_not_equals_none(self):
        obj = self.store.find(SelfRef, SelfRef.selfref != None).one()
        assert obj
        assert obj.selfref != None

    def test_reference_not_equals_with_composed_key(self):
        class LinkWithRef(Link):
            myself = Reference((Link.foo_id, Link.bar_id),
                               (Link.foo_id, Link.bar_id))

        link = self.store.find(LinkWithRef, foo_id=10, bar_id=100).one()
        result = list(self.store.find(LinkWithRef, LinkWithRef.myself != link))
        assert link not in result, "%r not in %r" % (link, result)

        result = list(self.store.find(
            LinkWithRef, LinkWithRef.myself != (link.foo_id, link.bar_id)))
        assert link not in result, "%r not in %r" % (link, result)

    def test_reference_self(self):
        selfref = self.store.add(SelfRef())
        selfref.id = 400
        selfref.title = u"Title 400"
        selfref.selfref_id = 25
        assert selfref.selfref.id == 25
        assert selfref.selfref.title == "SelfRef 25"

    def get_bar_200_title(self):
        connection = self.store._connection
        result = connection.execute("SELECT title FROM bar WHERE id=200")
        return result.get_one()[0]

    def test_reference_wont_touch_store_when_key_is_none(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = None
        bar.title = u"Don't flush this!"

        assert bar.foo == None

        # Bypass the store to prevent flushing.
        assert self.get_bar_200_title() == "Title 200"

    def test_reference_wont_touch_store_when_key_is_unset(self):
        bar = self.store.get(Bar, 200)
        del bar.foo_id
        bar.title = u"Don't flush this!"

        assert bar.foo == None

        # Bypass the store to prevent flushing.
        connection = self.store._connection
        result = connection.execute("SELECT title FROM bar WHERE id=200")
        assert result.get_one()[0] == "Title 200"

    def test_reference_wont_touch_store_with_composed_key_none(self):
        class Bar(object):
            __storm_table__ = "bar"
            id = Int(primary=True)
            foo_id = Int()
            title = Unicode()
            foo = Reference((foo_id, title), (Foo.id, Foo.title))

        bar = self.store.get(Bar, 200)
        bar.foo_id = None
        bar.title = None

        assert bar.foo == None

        # Bypass the store to prevent flushing.
        assert self.get_bar_200_title() == "Title 200"

    def test_reference_will_resolve_auto_reload(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = AutoReload
        assert bar.foo

    def test_back_reference(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        foo = self.store.get(MyFoo, 10)
        assert foo.bar
        assert foo.bar.title == "Title 300"

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

        assert foo.id
        assert bar.foo_id == foo.id

        result = self.store.execute("SELECT bar.title "
                                    "FROM foo, bar "
                                    "WHERE foo.id = bar.foo_id AND "
                                    "foo.title = 'Title 40'")
        assert result.get_one() == ("Title 400",)

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

        assert foo.bar == bar

        self.store.flush()

        assert foo.id == 40
        assert bar.foo_id == 40

        result = self.store.execute("SELECT bar.title "
                                    "FROM foo, bar "
                                    "WHERE foo.id = bar.foo_id AND "
                                    "foo.title = 'Title 40'")
        assert result.get_one() == ("Title 400",)

    def test_back_reference_assign_none_with_unseen(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)
        foo = self.store.get(MyFoo, 20)
        foo.bar = None
        assert foo.bar == None

    def test_back_reference_assign_none_from_none(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)
        self.store.execute("INSERT INTO foo (id, title)"
                           " VALUES (40, 'Title 40')")
        self.store.commit()
        foo = self.store.get(MyFoo, 40)
        foo.bar = None
        assert foo.bar == None

    def test_back_reference_on_added_unsets_original_key(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        foo = MyFoo()

        bar = Bar()
        bar.id = 400
        bar.foo_id = 40

        foo.bar = bar

        assert bar.foo_id == None

    def test_back_reference_on_added_no_store(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = u"Title 400"

        foo = MyFoo()
        foo.bar = bar
        foo.title = u"Title 40"

        self.store.add(bar)

        assert Store.of(bar) == self.store
        assert Store.of(foo) == self.store

        self.store.flush()

        assert type(bar.foo_id) == int

    def test_back_reference_on_added_no_store_2(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = u"Title 400"

        foo = MyFoo()
        foo.bar = bar
        foo.title = u"Title 40"

        self.store.add(foo)

        assert Store.of(bar) == self.store
        assert Store.of(foo) == self.store

        self.store.flush()

        assert type(bar.foo_id) == int

    def test_back_reference_remove_remote(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = u"Title 400"

        foo = MyFoo()
        foo.title = u"Title 40"
        foo.bar = bar

        self.store.add(foo)
        self.store.flush()

        assert foo.bar == bar
        self.store.remove(bar)
        assert foo.bar == None

    def test_back_reference_remove_remote_pending_add(self):
        class MyFoo(Foo):
            bar = Reference(Foo.id, Bar.foo_id, on_remote=True)

        bar = Bar()
        bar.title = u"Title 400"

        foo = MyFoo()
        foo.title = u"Title 40"
        foo.bar = bar

        self.store.add(foo)

        assert foo.bar == bar
        self.store.remove(bar)
        assert foo.bar == None

    def test_reference_loop_with_undefined_keys_fails(self):
        """A loop of references with undefined keys raises OrderLoopError."""
        ref1 = SelfRef()
        self.store.add(ref1)
        ref2 = SelfRef()
        ref2.selfref = ref1
        ref1.selfref = ref2

        with pytest.raises(OrderLoopError):
            self.store.flush()

    def test_reference_loop_with_dirty_keys_fails(self):
        ref1 = SelfRef()
        self.store.add(ref1)
        ref1.id = 42
        ref2 = SelfRef()
        ref2.id = 43
        ref2.selfref = ref1
        ref1.selfref = ref2

        with pytest.raises(OrderLoopError):
            self.store.flush()

    def test_reference_loop_with_dirty_keys_changed_later_fails(self):
        ref1 = SelfRef()
        ref2 = SelfRef()
        self.store.add(ref1)
        self.store.add(ref2)
        self.store.flush()
        ref2.selfref = ref1
        ref1.selfref = ref2
        ref1.id = 42
        ref2.id = 43

        with pytest.raises(OrderLoopError):
            self.store.flush()

    def test_reference_loop_with_dirty_keys_on_remote_fails(self):
        ref1 = SelfRef()
        self.store.add(ref1)
        ref1.id = 42
        ref2 = SelfRef()
        ref2.id = 43
        ref2.selfref_on_remote = ref1
        ref1.selfref_on_remote = ref2

        with pytest.raises(OrderLoopError):
            self.store.flush()

    def test_reference_loop_with_dirty_keys_on_remote_changed_later_fails(self):
        ref1 = SelfRef()
        ref2 = SelfRef()
        self.store.add(ref1)
        self.store.flush()
        ref2.selfref_on_remote = ref1
        ref1.selfref_on_remote = ref2
        ref1.id = 42
        ref2.id = 43

        with pytest.raises(OrderLoopError):
            self.store.flush()

    def test_reference_loop_with_unchanged_keys_succeeds(self):
        ref1 = SelfRef()
        self.store.add(ref1)
        ref1.id = 42
        ref2 = SelfRef()
        self.store.add(ref2)
        ref1.id = 43

        self.store.flush()

        # As ref1 and ref2 have been flushed to the database, so these
        # changes can be flushed.
        ref2.selfref = ref1
        ref1.selfref = ref2
        self.store.flush()

    def test_reference_loop_with_one_unchanged_key_succeeds(self):
        ref1 = SelfRef()
        self.store.add(ref1)
        self.store.flush()

        ref2 = SelfRef()
        ref2.selfref = ref1
        ref1.selfref = ref2

        # As ref1 and ref2 have been flushed to the database, so these
        # changes can be flushed.
        self.store.flush()

    def test_reference_loop_with_key_changed_later_succeeds(self):
        ref1 = SelfRef()
        self.store.add(ref1)
        self.store.flush()

        ref2 = SelfRef()
        ref1.selfref = ref2
        ref2.id = 42

        self.store.flush()

    def test_reference_loop_with_key_changed_later_on_remote_succeeds(self):
        ref1 = SelfRef()
        self.store.add(ref1)
        self.store.flush()

        ref2 = SelfRef()
        ref2.selfref_on_remote = ref1
        ref2.id = 42

        self.store.flush()

    def test_reference_loop_with_undefined_and_changed_keys_fails(self):
        ref1 = SelfRef()
        self.store.add(ref1)
        self.store.flush()

        ref1.id = 400
        ref2 = SelfRef()
        ref2.selfref = ref1
        ref1.selfref = ref2

        with pytest.raises(OrderLoopError):
            self.store.flush()

    def test_reference_loop_with_undefined_and_changed_keys_fails2(self):
        ref1 = SelfRef()
        self.store.add(ref1)
        self.store.flush()

        ref2 = SelfRef()
        ref2.selfref = ref1
        ref1.selfref = ref2
        ref1.id = 400

        with pytest.raises(OrderLoopError):
            self.store.flush()

    def test_reference_loop_broken_by_set(self):
        ref1 = SelfRef()
        ref2 = SelfRef()
        ref1.selfref = ref2
        ref2.selfref = ref1
        self.store.add(ref1)

        ref1.selfref = None
        self.store.flush()

    def test_reference_loop_set_only_removes_own_flush_order(self):
        ref1 = SelfRef()
        ref2 = SelfRef()
        self.store.add(ref2)
        self.store.flush()

        # The following does not create a loop since the keys are
        # dirty (as shown in another test).
        ref1.selfref = ref2
        ref2.selfref = ref1

        # Now add a flush order loop.
        self.store.add_flush_order(ref1, ref2)
        self.store.add_flush_order(ref2, ref1)

        # Now break the reference.  This should leave the flush
        # ordering loop we previously created in place..
        ref1.selfref = None
        with pytest.raises(OrderLoopError):
            self.store.flush()

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

        assert items == [
            (200, 20, "Title 200"),
            (400, 20, "Title 100"),
        ]

    def test_reference_set_assign_fails(self):
        foo = self.store.get(FooRefSet, 20)
        try:
            foo.bars = []
        except FeatureError:
            pass
        else:
            self.fail("FeatureError not raised")

    def test_reference_set_explicitly_with_wrapper(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSet, 20)

        items = []
        for bar in FooRefSet.bars.__get__(Wrapper(foo)):
            items.append((bar.id, bar.foo_id, bar.title))
        items.sort()

        assert items == [
            (200, 20, "Title 200"),
            (400, 20, "Title 100"),
        ]

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

        assert foo.id == None
        assert bar1.foo_id == None
        assert bar2.foo_id == None
        assert list(foo.bars.order_by(Bar.id)) == [bar1, bar2]
        assert type(foo.id) == int
        assert foo.id == bar1.foo_id
        assert foo.id == bar2.foo_id

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

        assert items == [(400, 20, "Title 20")]

        bar = self.store.get(Bar, 200)
        bar.title = u"Title 20"

        del items[:]
        for bar in foo.bars:
            items.append((bar.id, bar.foo_id, bar.title))
        items.sort()

        assert items == [
            (200, 20, "Title 20"),
            (400, 20, "Title 20"),
        ]

    def test_reference_set_contains(self):
        def no_iter(self):
            raise RuntimeError()
        from storm.references import BoundReferenceSetBase
        orig_iter = BoundReferenceSetBase.__iter__
        BoundReferenceSetBase.__iter__ = no_iter
        try:
            foo = self.store.get(FooRefSet, 20)
            bar = self.store.get(Bar, 200)
            assert bar in foo.bars
        finally:
            BoundReferenceSetBase.__iter__ = orig_iter

    def test_reference_set_find(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSet, 20)

        items = []
        for bar in foo.bars.find():
            items.append((bar.id, bar.foo_id, bar.title))
        items.sort()

        assert items == [
            (200, 20, "Title 200"),
            (400, 20, "Title 100"),
        ]

        # Notice that there's another item with this title in the base,
        # which isn't part of the reference.

        objects = list(foo.bars.find(Bar.title == u"Title 100"))
        assert len(objects) == 1
        assert objects[0] is bar

        objects = list(foo.bars.find(title=u"Title 100"))
        assert len(objects) == 1
        assert objects[0] is bar

    def test_reference_set_clear(self):
        foo = self.store.get(FooRefSet, 20)
        foo.bars.clear()
        assert list(foo.bars) == []

        # Object wasn't removed.
        assert self.store.get(Bar, 200)

    def test_reference_set_clear_cached(self):
        foo = self.store.get(FooRefSet, 20)
        bar = self.store.get(Bar, 200)
        assert bar.foo_id == 20
        foo.bars.clear()
        assert bar.foo_id == None

    def test_reference_set_clear_where(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSet, 20)
        foo.bars.clear(Bar.id > 200)

        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        assert items == [(200, 20, "Title 200")]

        bar = self.store.get(Bar, 400)
        bar.foo_id = 20

        foo.bars.clear(id=200)

        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        assert items == [(400, 20, "Title 100")]

    def test_reference_set_count(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSet, 20)

        assert foo.bars.count() == 2

    def test_reference_set_order_by(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSet, 20)

        items = []
        for bar in foo.bars.order_by(Bar.id):
            items.append((bar.id, bar.foo_id, bar.title))
        assert items == [
            (200, 20, "Title 200"),
            (400, 20, "Title 100"),
        ]

        del items[:]
        for bar in foo.bars.order_by(Bar.title):
            items.append((bar.id, bar.foo_id, bar.title))
        assert items == [
            (400, 20, "Title 100"),
            (200, 20, "Title 200"),
        ]

    def test_reference_set_default_order_by(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSetOrderID, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.foo_id, bar.title))
        assert items == [
            (200, 20, "Title 200"),
            (400, 20, "Title 100"),
        ]

        items = []
        for bar in foo.bars.find():
            items.append((bar.id, bar.foo_id, bar.title))
        assert items == [
            (200, 20, "Title 200"),
            (400, 20, "Title 100"),
        ]

        del items[:]
        foo = self.store.get(FooRefSetOrderTitle, 20)
        for bar in foo.bars:
            items.append((bar.id, bar.foo_id, bar.title))
        assert items == [
            (400, 20, "Title 100"),
            (200, 20, "Title 200"),
        ]

        del items[:]
        for bar in foo.bars.find():
            items.append((bar.id, bar.foo_id, bar.title))
        assert items == [
            (400, 20, "Title 100"),
            (200, 20, "Title 200"),
        ]

    def test_reference_set_first_last(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSetOrderID, 20)
        assert foo.bars.first().id == 200
        assert foo.bars.last().id == 400

        foo = self.store.get(FooRefSetOrderTitle, 20)
        assert foo.bars.first().id == 400
        assert foo.bars.last().id == 200

        foo = self.store.get(FooRefSetOrderTitle, 20)
        assert foo.bars.first(Bar.id > 400) == None
        assert foo.bars.last(Bar.id > 400) == None

        foo = self.store.get(FooRefSetOrderTitle, 20)
        assert foo.bars.first(Bar.id < 400).id == 200
        assert foo.bars.last(Bar.id < 400).id == 200

        foo = self.store.get(FooRefSetOrderTitle, 20)
        assert foo.bars.first(id=200).id == 200
        assert foo.bars.last(id=200).id == 200

        foo = self.store.get(FooRefSet, 20)
        with pytest.raises(UnorderedError):
            foo.bars.first()
        with pytest.raises(UnorderedError):
            foo.bars.last()

    def test_indirect_reference_set_any(self):
        """
        L{BoundReferenceSet.any} returns an arbitrary object from the set of
        referenced objects.
        """
        foo = self.store.get(FooRefSet, 20)
        assert None != foo.bars.any()

    def test_indirect_reference_set_any_filtered(self):
        """
        L{BoundReferenceSet.any} optionally takes a list of filtering criteria
        to narrow the set of objects to search.  When provided, the criteria
        are used to filter the set before returning an arbitrary object.
        """
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSetOrderTitle, 20)
        assert foo.bars.any(Bar.id > 400) == None

        foo = self.store.get(FooRefSetOrderTitle, 20)
        assert foo.bars.any(Bar.id < 400).id == 200

        foo = self.store.get(FooRefSetOrderTitle, 20)
        assert foo.bars.any(id=200).id == 200

    def test_reference_set_one(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSetOrderID, 20)
        with pytest.raises(NotOneError):
            foo.bars.one()

        foo = self.store.get(FooRefSetOrderID, 30)
        assert foo.bars.one().id == 300

        foo = self.store.get(FooRefSetOrderID, 20)
        assert foo.bars.one(Bar.id > 400) == None

        foo = self.store.get(FooRefSetOrderID, 20)
        assert foo.bars.one(Bar.id < 400).id == 200

        foo = self.store.get(FooRefSetOrderID, 20)
        assert foo.bars.one(id=200).id == 200

    def test_reference_set_remove(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSet, 20)
        for bar in foo.bars:
            foo.bars.remove(bar)

        assert bar.foo_id == None
        assert list(foo.bars) == []

    def test_reference_set_after_object_removed(self):
        class MyBar(Bar):
            # Make sure that this works even with allow_none=False.
            foo_id = Int(allow_none=False)

        class MyFoo(Foo):
            bars = ReferenceSet(Foo.id, MyBar.foo_id)

        foo = self.store.get(MyFoo, 20)
        bar = foo.bars.any()
        self.store.remove(bar)
        assert bar not in list(foo.bars)

    def test_reference_set_add(self):
        bar = Bar()
        bar.id = 400
        bar.title = u"Title 100"

        foo = self.store.get(FooRefSet, 20)
        foo.bars.add(bar)

        assert bar.foo_id == 20
        assert Store.of(bar) == self.store

    def test_reference_set_add_no_store(self):
        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"

        foo = FooRefSet()
        foo.title = u"Title 40"
        foo.bars.add(bar)

        self.store.add(foo)

        assert Store.of(foo) == self.store
        assert Store.of(bar) == self.store

        self.store.flush()

        assert type(bar.foo_id) == int

    def test_reference_set_add_no_store_2(self):
        bar = Bar()
        bar.id = 400
        bar.title = u"Title 400"

        foo = FooRefSet()
        foo.title = u"Title 40"
        foo.bars.add(bar)

        self.store.add(bar)

        assert Store.of(foo) == self.store
        assert Store.of(bar) == self.store

        self.store.flush()

        assert type(bar.foo_id) == int

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

        assert Store.of(foo) == self.store
        assert Store.of(bar1) == store
        assert Store.of(bar2) == self.store

    def test_reference_set_values(self):
        self.add_reference_set_bar_400()

        foo = self.store.get(FooRefSetOrderID, 20)

        values = list(foo.bars.values(Bar.id, Bar.foo_id, Bar.title))
        assert values == [(200, 20, "Title 200"), (400, 20, "Title 100")]

    def test_reference_set_order_by_desc_id(self):
        self.add_reference_set_bar_400()

        class FooRefSetOrderByDescID(Foo):
            bars = ReferenceSet(Foo.id, Bar.foo_id, order_by=Desc(Bar.id))

        foo = self.store.get(FooRefSetOrderByDescID, 20)

        values = list(foo.bars.values(Bar.id, Bar.foo_id, Bar.title))
        assert values == [(400, 20, "Title 100"), (200, 20, "Title 200")]

        assert foo.bars.first().id == 400
        assert foo.bars.last().id == 200

    def test_indirect_reference_set(self):
        foo = self.store.get(FooIndRefSet, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        assert items == [(100, "Title 300"), (200, "Title 200")]

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

        assert foo.id == None

        self.store.add(foo)

        assert foo.id == None
        assert bar1.foo_id == None
        assert bar2.foo_id == None
        assert list(foo.bars.order_by(Bar.id)) == [bar1, bar2]
        assert type(foo.id) == int
        assert type(bar1.id) == int
        assert type(bar2.id) == int

    def test_indirect_reference_set_find(self):
        foo = self.store.get(FooIndRefSet, 20)

        items = []
        for bar in foo.bars.find(Bar.title == u"Title 300"):
            items.append((bar.id, bar.title))
        items.sort()

        assert items == [(100, "Title 300")]

    def test_indirect_reference_set_clear(self):
        foo = self.store.get(FooIndRefSet, 20)
        foo.bars.clear()
        assert list(foo.bars) == []

    def test_indirect_reference_set_clear_where(self):
        foo = self.store.get(FooIndRefSet, 20)
        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        assert items == [
            (100, 10, "Title 300"),
            (200, 20, "Title 200"),
        ]

        foo = self.store.get(FooIndRefSet, 30)
        foo.bars.clear(Bar.id < 300)
        foo.bars.clear(id=200)

        foo = self.store.get(FooIndRefSet, 20)
        foo.bars.clear(Bar.id < 200)

        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        assert items == [(200, 20, "Title 200")]

        foo.bars.clear(id=200)

        items = [(bar.id, bar.foo_id, bar.title) for bar in foo.bars]
        assert items == []

    def test_indirect_reference_set_count(self):
        foo = self.store.get(FooIndRefSet, 20)
        assert foo.bars.count() == 2

    def test_indirect_reference_set_order_by(self):
        foo = self.store.get(FooIndRefSet, 20)

        items = []
        for bar in foo.bars.order_by(Bar.title):
            items.append((bar.id, bar.title))

        assert items == [
            (200, "Title 200"),
            (100, "Title 300"),
        ]

        del items[:]
        for bar in foo.bars.order_by(Bar.id):
            items.append((bar.id, bar.title))

        assert items == [
            (100, "Title 300"),
            (200, "Title 200"),
        ]

    def test_indirect_reference_set_default_order_by(self):
        foo = self.store.get(FooIndRefSetOrderTitle, 20)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        assert items == [
            (200, "Title 200"),
            (100, "Title 300"),
        ]

        del items[:]
        for bar in foo.bars.find():
            items.append((bar.id, bar.title))
        assert items == [
            (200, "Title 200"),
            (100, "Title 300"),
        ]

        foo = self.store.get(FooIndRefSetOrderID, 20)
        del items[:]
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        assert items == [
            (100, "Title 300"),
            (200, "Title 200"),
        ]

        del items[:]
        for bar in foo.bars.find():
            items.append((bar.id, bar.title))
        assert items == [
            (100, "Title 300"),
            (200, "Title 200"),
        ]

    def test_indirect_reference_set_first_last(self):
        foo = self.store.get(FooIndRefSetOrderID, 20)
        assert foo.bars.first().id == 100
        assert foo.bars.last().id == 200

        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        assert foo.bars.first().id == 200
        assert foo.bars.last().id == 100

        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        assert foo.bars.first(Bar.id > 200) == None
        assert foo.bars.last(Bar.id > 200) == None

        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        assert foo.bars.first(Bar.id < 200).id == 100
        assert foo.bars.last(Bar.id < 200).id == 100

        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        assert foo.bars.first(id=200).id == 200
        assert foo.bars.last(id=200).id == 200

        foo = self.store.get(FooIndRefSet, 20)
        with pytest.raises(UnorderedError):
            foo.bars.first()
        with pytest.raises(UnorderedError):
            foo.bars.last()

    def test_indirect_reference_set_any(self):
        """
        L{BoundIndirectReferenceSet.any} returns an arbitrary object from the
        set of referenced objects.
        """
        foo = self.store.get(FooIndRefSet, 20)
        assert None != foo.bars.any()

    def test_indirect_reference_set_any_filtered(self):
        """
        L{BoundIndirectReferenceSet.any} optionally takes a list of filtering
        criteria to narrow the set of objects to search.  When provided, the
        criteria are used to filter the set before returning an arbitrary
        object.
        """
        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        assert foo.bars.any(Bar.id > 200) == None

        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        assert foo.bars.any(Bar.id < 200).id == 100

        foo = self.store.get(FooIndRefSetOrderTitle, 20)
        assert foo.bars.any(id=200).id == 200

    def test_indirect_reference_set_one(self):
        foo = self.store.get(FooIndRefSetOrderID, 20)
        with pytest.raises(NotOneError):
            foo.bars.one()

        foo = self.store.get(FooIndRefSetOrderID, 30)
        assert foo.bars.one().id == 300

        foo = self.store.get(FooIndRefSetOrderID, 20)
        assert foo.bars.one(Bar.id > 200) == None

        foo = self.store.get(FooIndRefSetOrderID, 20)
        assert foo.bars.one(Bar.id < 200).id == 100

        foo = self.store.get(FooIndRefSetOrderID, 20)
        assert foo.bars.one(id=200).id == 200

    def test_indirect_reference_set_add(self):
        foo = self.store.get(FooIndRefSet, 20)
        bar = self.store.get(Bar, 300)

        foo.bars.add(bar)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        assert items == [
            (100, "Title 300"),
            (200, "Title 200"),
            (300, "Title 100"),
        ]

    def test_indirect_reference_set_remove(self):
        foo = self.store.get(FooIndRefSet, 20)
        bar = self.store.get(Bar, 200)

        foo.bars.remove(bar)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        assert items == [(100, "Title 300")]

    def test_indirect_reference_set_add_remove(self):
        foo = self.store.get(FooIndRefSet, 20)
        bar = self.store.get(Bar, 300)

        foo.bars.add(bar)
        foo.bars.remove(bar)

        items = []
        for bar in foo.bars:
            items.append((bar.id, bar.title))
        items.sort()

        assert items == [
            (100, "Title 300"),
            (200, "Title 200"),
        ]

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

        assert items == [
            (100, "Title 300"),
            (300, "Title 100"),
        ]

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

        assert items == [(500, "Title 500")]

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

        assert Store.of(foo) == self.store
        assert Store.of(bar1) == self.store
        assert Store.of(bar2) == self.store

        assert foo.id == None
        assert bar1.foo_id == None
        assert bar2.foo_id == None

        assert list(foo.bars.order_by(Bar.id)) == [bar1, bar2]

    def test_indirect_reference_set_values(self):
        foo = self.store.get(FooIndRefSetOrderID, 20)

        values = list(foo.bars.values(Bar.id, Bar.foo_id, Bar.title))
        assert values == [
            (100, 10, "Title 300"),
            (200, 20, "Title 200"),
        ]

    def test_references_raise_nostore(self):
        foo1 = FooRefSet()
        foo2 = FooIndRefSet()

        with pytest.raises(NoStoreError):
            foo1.bars.__iter__()
        with pytest.raises(NoStoreError):
            foo2.bars.__iter__()
        with pytest.raises(NoStoreError):
            foo1.bars.find()
        with pytest.raises(NoStoreError):
            foo2.bars.find()
        with pytest.raises(NoStoreError):
            foo1.bars.order_by()
        with pytest.raises(NoStoreError):
            foo2.bars.order_by()
        with pytest.raises(NoStoreError):
            foo1.bars.count()
        with pytest.raises(NoStoreError):
            foo2.bars.count()
        with pytest.raises(NoStoreError):
            foo1.bars.clear()
        with pytest.raises(NoStoreError):
            foo2.bars.clear()
        with pytest.raises(NoStoreError):
            foo2.bars.remove(object())

    def test_string_reference(self):
        @add_metaclass(PropertyPublisherMeta)
        class Base(object):
            pass

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
        assert bar.foo
        assert bar.foo.title == "Title 30"
        assert type(bar.foo) == MyFoo

    def test_string_indirect_reference_set(self):
        """
        A L{ReferenceSet} can have its reference keys specified as strings
        when the class its a member of uses the L{PropertyPublisherMeta}
        metaclass.  This makes it possible to work around problems with
        circular dependencies by delaying property resolution.
        """
        @add_metaclass(PropertyPublisherMeta)
        class Base(object):
            pass

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

        assert items == [(100, "Title 300"), (200, "Title 200")]

    def test_string_reference_set_order_by(self):
        """
        A L{ReferenceSet} can have its default order by specified as a string
        when the class its a member of uses the L{PropertyPublisherMeta}
        metaclass.  This makes it possible to work around problems with
        circular dependencies by delaying resolution of the order by column.
        """
        @add_metaclass(PropertyPublisherMeta)
        class Base(object):
            pass

        class MyFoo(Base):
            __storm_table__ = "foo"
            id = Int(primary=True)
            title = Unicode()
            bars = ReferenceSet("id", "MyLink.foo_id",
                                "MyLink.bar_id", "MyBar.id",
                                order_by="MyBar.title")

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
        items = [(bar.id, bar.title) for bar in foo.bars]
        assert items == [(200, "Title 200"), (100, "Title 300")]

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

        with pytest.raises(OrderLoopError):
            self.store.flush()

        self.store.remove_flush_order(foo5, foo2)

        with pytest.raises(OrderLoopError):
            self.store.flush()

        self.store.remove_flush_order(foo5, foo2)

        self.store.flush()

        assert foo2.id < foo4.id
        assert foo4.id < foo1.id
        assert foo1.id < foo3.id
        assert foo3.id < foo5.id

    def test_variable_filter_on_load(self):
        foo = self.store.get(FooVariable, 20)
        assert foo.title == "to_py(from_db(Title 20))"

    def test_variable_filter_on_update(self):
        foo = self.store.get(FooVariable, 20)
        foo.title = u"Title 20"

        self.store.flush()
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "to_db(from_py(Title 20))"),
            (30, "Title 10"),
        ]

    def test_variable_filter_on_update_unchanged(self):
        foo = self.store.get(FooVariable, 20)
        self.store.flush()
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

    def test_variable_filter_on_insert(self):
        foo = FooVariable()
        foo.id = 40
        foo.title = u"Title 40"

        self.store.add(foo)
        self.store.flush()

        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
            (40, "to_db(from_py(Title 40))"),
        ]

    def test_variable_filter_on_missing_values(self):
        foo = FooVariable()
        foo.id = 40

        self.store.add(foo)
        self.store.flush()

        assert foo.title == "to_py(from_db(Default Title))"

    def test_variable_filter_on_set(self):
        foo = FooVariable()
        self.store.find(FooVariable, id=20).set(title=u"Title 20")

        assert self.get_items() == [
            (10, "Title 30"),
            (20, "to_db(from_py(Title 20))"),
            (30, "Title 10"),
        ]

    def test_variable_filter_on_set_expr(self):
        foo = FooVariable()
        result = self.store.find(FooVariable, id=20)
        result.set(FooVariable.title == u"Title 20")

        assert self.get_items() == [
            (10, "Title 30"),
            (20, "to_db(from_py(Title 20))"),
            (30, "Title 10"),
        ]

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

        assert foo.id == 21
        assert foo.title == "set_variable(Title 20)"

    def test_default(self):
        class MyFoo(Foo):
            title = Unicode(default=u"Some default value")

        foo = MyFoo()
        self.store.add(foo)
        self.store.flush()

        result = self.store.execute("SELECT title FROM foo WHERE id=?",
                                    (foo.id,))
        assert result.get_one() == ("Some default value",)

        assert foo.title == "Some default value"

    def test_default_factory(self):
        class MyFoo(Foo):
            title = Unicode(default_factory=lambda:u"Some default value")

        foo = MyFoo()
        self.store.add(foo)
        self.store.flush()

        result = self.store.execute("SELECT title FROM foo WHERE id=?",
                                    (foo.id,))
        assert result.get_one() == ("Some default value",)

        assert foo.title == "Some default value"

    def test_json_variable(self):
        class MyJson(Json):
            pass

        a = self.store.get(MyJson, 20)
        self.store.flush()

        b = self.store.get(Json, 20)
        assert b.data["a"] == 1
        b.data["b"] = 2
        self.store.flush()

        self.store.reload(a)
        assert a.data == {"a": 1, "b": 2}

    def test_json_variable_remove(self):
        """
        When an object is removed from a store, it should unhook from the
        "flush" event emitted by the store, and thus not emit a "changed" event
        if its content change and that the store is flushed.
        """
        class MyJson(Json):
            pass

        a = self.store.get(MyJson, 20)
        self.store.flush()

        b = self.store.get(Json, 20)
        self.store.remove(b)
        self.store.flush()

        #  Let's change the object
        b.data["b"] = 2

        # And subscribe to its changed event
        obj_info = get_obj_info(b)
        events = []
        obj_info.event.hook("changed", lambda *args: events.append(args))

        self.store.flush()
        assert events == []

    def test_json_variable_unhook(self):
        """
        A variable instance must unhook itself from the store event system when
        the store invalidates its objects.
        """
        # I create a custom JSONVariable, with no __slots__ definition, to be
        # able to get a weakref of it, thing that I can't do with
        # JSONVariable that defines __slots__ *AND* those parent is the C
        # implementation of Variable
        class CustomJsonVariable(JSONVariable):
            pass

        class CustomJson(JSON):
            variable_class = CustomJsonVariable

        class MyJson(Json):
            data = CustomJson()

        a = self.store.get(Json, 20)
        self.store.flush()

        b = self.store.get(MyJson, 20)
        self.store.flush()
        self.store.invalidate()

        obj_info = get_obj_info(b)
        variable = obj_info.variables[MyJson.data]
        var_ref = weakref.ref(variable)
        del variable, a, b, obj_info
        gc.collect()
        assert var_ref() is None

    def test_json_variable_referenceset(self):
        """
        A variable instance must unhook itself from the store event system
        explcitely when the store invalidates its objects: it's particulary
        important when a ReferenceSet is used, because it keeps strong
        references to objects involved.
        """
        class CustomJsonVariable(JSONVariable):
            pass

        class CustomJson(JSON):
            variable_class = CustomJsonVariable

        class MyJson(Json):
            data = CustomJson()
            foo_id = Int()

        class FooBlobRefSet(Foo):
            blobs = ReferenceSet(Foo.id, MyJson.foo_id)

        a = self.store.get(Json, 20)
        a.data["c"] = 3
        self.store.flush()

        b = self.store.get(MyJson, 20)
        foo = self.store.get(FooBlobRefSet, 10)
        foo.blobs.add(b)

        self.store.flush()
        self.store.invalidate()

        obj_info = get_obj_info(b)
        variable = obj_info.variables[MyJson.data]
        var_ref = weakref.ref(variable)
        del variable, a, b, obj_info, foo
        gc.collect()
        assert var_ref() is None

    def test_json_variable_referenceset_several_transactions(self):
        """
        Check that a JSONVariable fires the changed event when used among
        several transactions.
        """
        class MyJson(Json):
            foo_id = Int()

        class FooBlobRefSet(Foo):
            blobs = ReferenceSet(Foo.id, MyJson.foo_id)

        json = self.store.get(MyJson, 20)

        foo = self.store.get(FooBlobRefSet, 10)
        foo.blobs.add(json)

        self.store.flush()
        self.store.invalidate()
        self.store.reload(json)

        json.data = {"x": 0}
        obj_info = get_obj_info(json)
        events = []
        obj_info.event.hook("changed", lambda *args: events.append(args))
        self.store.flush()
        assert len(events) == 1

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
                assert obj_info.variables[column].is_defined()

    def test_storm_loaded_after_define(self):
        """
        C{__storm_loaded__} is only called once all the variables are correctly
        defined in the object. If the object is in the alive cache but
        disappeared, it used to be called without its variables defined.
        """
        # Disable the cache, which holds strong references.
        self.get_cache(self.store).set_size(0)
        loaded = []
        class MyFoo(Foo):
            def __storm_loaded__(oself):
                loaded.append(None)
                obj_info = get_obj_info(oself)
                for column in obj_info.variables:
                    assert obj_info.variables[column].is_defined()

        foo = self.store.get(MyFoo, 20)
        obj_info = get_obj_info(foo)

        del foo
        gc.collect()

        assert obj_info.get_obj() == None

        # Commit so that all foos are invalidated and variables are
        # set back to AutoReload.
        self.store.commit()

        foo = self.store.find(MyFoo, title=u"Title 20").one()
        assert foo.id == 20
        assert len(loaded) == 2

    def test_defined_variables_not_overridden_on_find(self):
        """
        Check that the keep_defined=True setting in _load_object()
        is in place.  In practice, it ensures that already defined
        values aren't replaced during a find, when new data comes
        from the database and is used whenever possible.
        """
        class MyJson(object):
            __storm_table__ = "json"
            id = Int(primary=True)
            renamed_data = JSON("data")
        obj = self.store.get(MyJson, 20)
        value = obj.renamed_data

        # Now the find should not destroy our value pointer.
        obj = self.store.find(MyJson, id=20).one()
        assert value is obj.renamed_data

    def test_json_variable_with_deleted_object(self):
        class MyJson(Json):
            pass

        a = self.store.get(Json, 20)
        b = self.store.get(MyJson, 20)
        assert a.data["a"] == 1
        assert b.data["a"] == 1

        b.data["b"] = 2

        del b
        gc.collect()

        self.store.flush()
        self.store.reload(a)
        assert a.data == {"a": 1, "b": 2}

    def test_unhashable_object(self):

        class DictFoo(Foo, dict):
            pass

        foo = self.store.get(DictFoo, 20)
        foo["a"] = 1

        assert list(foo.items()) == [("a", 1)]

        new_obj = DictFoo()
        new_obj.id = 40
        new_obj.title = u"My Title"

        self.store.add(new_obj)
        self.store.commit()

        assert self.store.get(DictFoo, 40) is new_obj

    def test_wrapper(self):
        foo = self.store.get(Foo, 20)
        wrapper = Wrapper(foo)
        self.store.remove(wrapper)
        self.store.flush()
        assert self.store.get(Foo, 20) is None

    def test_rollback_loaded_and_still_in_cached(self):
        # Explore problem found on interaction between caching, commits,
        # and rollbacks, when they still existed.
        foo1 = self.store.get(Foo, 20)
        self.store.commit()
        self.store.rollback()
        foo2 = self.store.get(Foo, 20)
        assert foo1 is foo2

    def test_class_alias(self):
        FooAlias = ClassAlias(Foo)
        result = self.store.find(FooAlias, FooAlias.id < Foo.id)
        assert [(foo.id, foo.title) for foo in result if type(foo) is Foo] == [
            (10, "Title 30"),
            (10, "Title 30"),
            (20, "Title 20"),
        ]

    def test_expr_values(self):
        foo = self.store.get(Foo, 20)

        foo.title = SQL("'New title'")

        # No commits yet.
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

        # Now it should be there.
        self.store.flush()
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "New title"),
            (30, "Title 10"),
        ]

        assert foo.title == "New title"

    def test_expr_values_flush_on_demand(self):
        foo = self.store.get(Foo, 20)

        foo.title = SQL("'New title'")

        # No commits yet.
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

        assert foo.title == "New title"

        # Now it should be there.

        assert self.get_items() == [
            (10, "Title 30"),
            (20, "New title"),
            (30, "Title 10"),
        ]

    def test_expr_values_flush_and_load_in_separate_steps(self):
        foo = self.store.get(Foo, 20)

        foo.title = SQL("'New title'")

        self.store.flush()

        # It's already in the database.
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "New title"),
            (30, "Title 10"),
        ]

        # But our value is now an AutoReload.
        lazy_value = get_obj_info(foo).variables[Foo.title].get_lazy()
        assert lazy_value is AutoReload

        # Which gets resolved once touched.
        assert foo.title == u"New title"

    def test_expr_values_flush_on_demand_with_added(self):
        foo = Foo()
        foo.id = 40
        foo.title = SQL("'New title'")

        self.store.add(foo)

        # No commits yet.
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

        assert foo.title == "New title"

        # Now it should be there.

        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
            (40, "New title"),
        ]

    def test_expr_values_flush_on_demand_with_removed_and_added(self):
        foo = self.store.get(Foo, 20)
        foo.title = SQL("'New title'")

        self.store.remove(foo)
        self.store.add(foo)

        # No commits yet.
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

        assert foo.title == "New title"

        # Now it should be there.

        assert self.get_items() == [
            (10, "Title 30"),
            (20, "New title"),
            (30, "Title 10"),
        ]

    def test_expr_values_flush_on_demand_with_removed_and_rollbacked(self):
        foo = self.store.get(Foo, 20)

        self.store.remove(foo)
        self.store.rollback()

        foo.title = SQL("'New title'")

        # No commits yet.
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]


        # Now it should be there. Accessing the attribute triggers a fetch.
        assert foo.title == "New title"
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "New title"),
            (30, "Title 10"),
        ]

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
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

        assert foo.title == None

        # Still no changes. There's no reason why foo_dep would be flushed.
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

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
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

        assert foo.title == None

        # Still no changes. There's no reason why foo_dep would be flushed.
        assert self.get_items() == [
            (10, "Title 30"),
            (20, "Title 20"),
            (30, "Title 10"),
        ]

    def test_lazy_value_preserved_with_subsequent_object_initialization(self):
        """
        If a lazy value has been modified on an object that is subsequently
        initialized from the database the lazy value is correctly preserved
        and the object is initialized properly.  This tests the fix for the
        problem reported in bug #620615.
        """
        # Retrieve an object, fully loaded.
        foo = self.store.get(Foo, 20)

        # Build and retrieve a result set ahead of time, so that
        # flushes won't happen when actually loading the object.
        result = self.store.find(Foo, Foo.id == 20)

        # Now, set an unflushed lazy value on an attribute.
        foo.title = SQL("'New title'")

        # Finally, get the existing object.
        foo = result.one()

        # We don't really have to test anything here, since the
        # explosion happened above, but here it is anyway.
        assert foo.title == "New title"

    def test_lazy_value_discarded_on_reload(self):
        """
        A counter-test to the above logic, also related to bug #620615. On
        an explicit reload, the lazy value must be discarded.
        """
        # Retrieve an object, fully loaded.
        foo = self.store.get(Foo, 20)

        # Build and retrieve a result set ahead of time, so that
        # flushes won't happen when actually loading the object.
        result = self.store.find(Foo, Foo.id == 20)

        # Now, set an unflushed lazy value on an attribute.
        foo.title = SQL("'New title'")

        # Give up on this and reload the original object.
        self.store.reload(foo)

        # We don't really have to test anything here, since the
        # explosion happened above, but here it is anyway.
        assert foo.title == "Title 20"

    def test_expr_values_with_columns(self):
        bar = self.store.get(Bar, 200)
        bar.foo_id = Bar.id+1
        assert bar.foo_id == 201

    def test_autoreload_attribute(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        assert foo.title == "Title 20"
        foo.title = AutoReload
        assert foo.title == "New Title"
        assert not get_obj_info(foo).variables[Foo.title].has_changed()

    def test_autoreload_attribute_with_changed_primary_key(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        assert foo.title == "Title 20"
        foo.id = 40
        foo.title = AutoReload
        assert foo.title == "New Title"
        assert foo.id == 40

    def test_autoreload_object(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        assert foo.title == "Title 20"
        self.store.autoreload(foo)
        assert foo.title == "New Title"

    def test_autoreload_primary_key_of_unflushed_object(self):
        foo = Foo()
        self.store.add(foo)
        foo.id = AutoReload
        foo.title = u"New Title"
        assert isinstance(foo.id, (int, long_int))
        assert foo.title == "New Title"

    def test_autoreload_primary_key_doesnt_reload_everything_else(self):
        foo = self.store.get(Foo, 20)
        self.store.autoreload(foo)

        obj_info = get_obj_info(foo)

        assert obj_info.variables[Foo.id].get_lazy() == None
        assert obj_info.variables[Foo.title].get_lazy() == AutoReload

        assert foo.id == 20

        assert obj_info.variables[Foo.id].get_lazy() == None
        assert obj_info.variables[Foo.title].get_lazy() == AutoReload

    def test_autoreload_all_objects(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        assert foo.title == "Title 20"
        self.store.autoreload()
        assert foo.title == "New Title"

    def test_autoreload_and_get_will_not_reload(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("UPDATE foo SET title='New Title' WHERE id=20")
        self.store.autoreload(foo)

        obj_info = get_obj_info(foo)

        assert obj_info.variables[Foo.title].get_lazy() == AutoReload
        self.store.get(Foo, 20)
        assert obj_info.variables[Foo.title].get_lazy() == AutoReload
        assert foo.title == "New Title"

    def test_autoreload_object_doesnt_tag_as_dirty(self):
        foo = self.store.get(Foo, 20)
        self.store.autoreload(foo)
        assert get_obj_info(foo) not in self.store._dirty

    def test_autoreload_missing_columns_on_insertion(self):
        foo = Foo()
        self.store.add(foo)
        self.store.flush()
        lazy_value = get_obj_info(foo).variables[Foo.title].get_lazy()
        assert lazy_value == AutoReload
        assert foo.title == u"Default Title"

    def test_reference_break_on_local_diverged_doesnt_autoreload(self):
        foo = self.store.get(Foo, 10)
        self.store.autoreload(foo)

        bar = self.store.get(Bar, 100)
        assert bar.foo
        bar.foo_id = 40
        assert bar.foo == None

        obj_info = get_obj_info(foo)
        assert obj_info.variables[Foo.title].get_lazy() == AutoReload

    def test_primary_key_reference(self):
        """
        When an object references another one using its primary key, it
        correctly checks for the invalidated state after the store has been
        committed, detecting if the referenced object has been removed behind
        its back.
        """
        class BarOnRemote(object):
            __storm_table__ = "bar"
            foo_id = Int(primary=True)
            foo = Reference(foo_id, Foo.id, on_remote=True)
        foo = self.store.get(Foo, 10)
        bar = self.store.get(BarOnRemote, 10)
        assert bar.foo == foo
        self.store.execute("DELETE FROM foo WHERE id = 10")
        self.store.commit()
        assert bar.foo == None

    def test_invalidate_and_get_object(self):
        foo = self.store.get(Foo, 20)
        self.store.invalidate(foo)
        assert self.store.get(Foo, 20) == foo
        assert self.store.find(Foo, id=20).one() == foo

    def test_invalidate_and_get_removed_object(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("DELETE FROM foo WHERE id=20")
        self.store.invalidate(foo)
        assert self.store.get(Foo, 20) is None
        assert self.store.find(Foo, id=20).one() is None

    def test_invalidate_and_validate_with_find(self):
        foo = self.store.get(Foo, 20)
        self.store.invalidate(foo)
        assert self.store.find(Foo, id=20).one() == foo

        # Cache should be considered valid again at this point.
        self.store.execute("DELETE FROM foo WHERE id=20")
        assert self.store.get(Foo, 20) == foo

    def test_invalidate_object_gets_validated(self):
        foo = self.store.get(Foo, 20)
        self.store.invalidate(foo)
        assert self.store.get(Foo, 20) == foo

        # At this point the object is valid again, so deleting it
        # from the database directly shouldn't affect caching.
        self.store.execute("DELETE FROM foo WHERE id=20")
        assert self.store.get(Foo, 20) == foo

    def test_invalidate_object_with_only_primary_key(self):
        link = self.store.get(Link, (20, 200))
        self.store.execute("DELETE FROM link WHERE foo_id=20 AND bar_id=200")
        self.store.invalidate(link)
        assert self.store.get(Link, (20, 200)) is None

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
        assert self.store.get(Foo, 40) == foo

    def test_invalidate_and_update(self):
        foo = self.store.get(Foo, 20)
        self.store.execute("DELETE FROM foo WHERE id=20")
        self.store.invalidate(foo)
        with pytest.raises(LostObjectError):
            foo.title = u"Title 40"

    def test_invalidated_objects_reloaded_by_get(self):
        foo = self.store.get(Foo, 20)
        self.store.invalidate(foo)
        foo = self.store.get(Foo, 20)
        title_variable = get_obj_info(foo).variables[Foo.title]
        assert title_variable.get_lazy() == None
        assert title_variable.get() == u"Title 20"
        assert foo.title == "Title 20"

    def test_invalidated_hook(self):
        called = []
        class MyFoo(Foo):
            def __storm_invalidated__(self):
                called.append(True)
        foo = self.store.get(MyFoo, 20)
        assert called == []
        self.store.autoreload(foo)
        assert called == []
        self.store.invalidate(foo)
        assert called == [True]

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
        assert called == [True, True]

    def test_reset_recreates_objects(self):
        """
        After resetting the store, all queries return fresh objects, even if
        there are other objects representing the same database rows still in
        memory.
        """
        foo1 = self.store.get(Foo, 10)
        foo1.dirty = True
        self.store.reset()
        new_foo1 = self.store.get(Foo, 10)
        assert not hasattr(new_foo1, "dirty")
        assert new_foo1 is not foo1

    def test_reset_unmarks_dirty(self):
        """
        If an object was dirty when store.reset() is called, its changes will
        not be affected.
        """
        foo1 = self.store.get(Foo, 10)
        foo1_title = foo1.title
        foo1.title = u"radix wuz here"
        self.store.reset()
        self.store.flush()
        new_foo1 = self.store.get(Foo, 10)
        assert new_foo1.title == foo1_title

    def test_reset_clears_cache(self):
        cache = self.get_cache(self.store)
        foo1 = self.store.get(Foo, 10)
        assert get_obj_info(foo1) in cache.get_cached()
        self.store.reset()
        assert cache.get_cached() == []

    def test_reset_breaks_store_reference(self):
        """
        After resetting the store, all objects that were associated with that
        store will no longer be.
        """
        foo1 = self.store.get(Foo, 10)
        self.store.reset()
        assert Store.of(foo1) is None

    def test_result_find(self):
        result1 = self.store.find(Foo, Foo.id <= 20)
        result2 = result1.find(Foo.id > 10)
        foo = result2.one()
        assert foo
        assert foo.id == 20

    def test_result_find_kwargs(self):
        result1 = self.store.find(Foo, Foo.id <= 20)
        result2 = result1.find(id=20)
        foo = result2.one()
        assert foo
        assert foo.id == 20

    def test_result_find_introduce_join(self):
        result1 = self.store.find(Foo, Foo.id <= 20)
        result2 = result1.find(Foo.id == Bar.foo_id,
                               Bar.title == u"Title 300")
        foo = result2.one()
        assert foo
        assert foo.id == 10

    def test_result_find_tuple(self):
        result1 = self.store.find((Foo, Bar), Foo.id == Bar.foo_id)
        result2 = result1.find(Bar.title == u"Title 100")
        foo_bar = result2.one()
        assert foo_bar
        foo, bar = foo_bar
        assert foo.id == 30
        assert bar.id == 300

    def test_result_find_undef_where(self):
        result = self.store.find(Foo, Foo.id == 20).find()
        foo = result.one()
        assert foo
        assert foo.id == 20
        result = self.store.find(Foo).find(Foo.id == 20)
        foo = result.one()
        assert foo
        assert foo.id == 20

    def test_result_find_fails_on_set_expr(self):
        result1 = self.store.find(Foo)
        result2 = self.store.find(Foo)
        result = result1.union(result2)
        with pytest.raises(FeatureError):
            result.find(Foo.id == 20)

    def test_result_find_fails_on_slice(self):
        result = self.store.find(Foo)[1:2]
        with pytest.raises(FeatureError):
            result.find(Foo.id == 20)

    def test_result_find_fails_on_group_by(self):
        result = self.store.find(Foo)
        result.group_by(Foo)
        with pytest.raises(FeatureError):
            result.find(Foo.id == 20)

    def test_result_union(self):
        result1 = self.store.find(Foo, id=30)
        result2 = self.store.find(Foo, id=10)
        result3 = result1.union(result2)

        result3.order_by(Foo.title)
        assert [(foo.id, foo.title) for foo in result3] == [
            (30, "Title 10"),
            (10, "Title 30"),
        ]

        result3.order_by(Desc(Foo.title))
        assert [(foo.id, foo.title) for foo in result3] == [
            (10, "Title 30"),
            (30, "Title 10"),
        ]

    def test_result_union_duplicated(self):
        result1 = self.store.find(Foo, id=30)
        result2 = self.store.find(Foo, id=30)

        result3 = result1.union(result2)

        assert [(foo.id, foo.title) for foo in result3] == [(30, "Title 10")]

    def test_result_union_duplicated_with_all(self):
        result1 = self.store.find(Foo, id=30)
        result2 = self.store.find(Foo, id=30)

        result3 = result1.union(result2, all=True)

        assert [(foo.id, foo.title) for foo in result3] == [
            (30, "Title 10"),
            (30, "Title 10"),
        ]

    def test_result_union_with_empty(self):
        result1 = self.store.find(Foo, id=30)
        result2 = EmptyResultSet()

        result3 = result1.union(result2)

        assert [(foo.id, foo.title) for foo in result3] == [(30, "Title 10")]

    def test_result_union_class_columns(self):
        """
        It's possible to do a union of two result sets on columns on
        different classes, as long as their variable classes are the
        same (e.g. both are IntVariables).
        """
        result1 = self.store.find(Foo.id, Foo.id == 10)
        result2 = self.store.find(Bar.foo_id, Bar.id == 200)
        assert [10 == 20], sorted(result1.union(result2))

    def test_result_union_incompatible(self):
        result1 = self.store.find(Foo, id=10)
        result2 = self.store.find(Bar, id=100)
        with pytest.raises(FeatureError):
            result1.union(result2)

    def test_result_union_unsupported_methods(self):
        result1 = self.store.find(Foo, id=30)
        result2 = self.store.find(Foo, id=10)
        result3 = result1.union(result2)

        with pytest.raises(FeatureError):
            result3.set(title=u"Title 40")
        with pytest.raises(FeatureError):
            result3.remove()

    def test_result_union_count(self):
        result1 = self.store.find(Foo, id=30)
        result2 = self.store.find(Foo, id=30)

        result3 = result1.union(result2, all=True)

        assert result3.count() == 2

    def test_result_union_limit_count(self):
        """
        It's possible to count the result of a union that is limited.
        """
        result1 = self.store.find(Foo, id=30)
        result2 = self.store.find(Foo, id=30)

        result3 = result1.union(result2, all=True)
        result3.order_by(Foo.id)
        result3.config(limit=1)

        assert result3.count() == 1
        assert result3.count(Foo.id) == 1

    def test_result_union_limit_avg(self):
        """
        It's possible to average the result of a union that is limited.
        """
        result1 = self.store.find(Foo, id=10)
        result2 = self.store.find(Foo, id=30)

        result3 = result1.union(result2, all=True)
        result3.order_by(Foo.id)
        result3.config(limit=1)

        # Since 30 was left off because of the limit, the only result will be
        # 10, and the average of that is 10.
        assert result3.avg(Foo.id) == 10

    def test_result_difference(self):
        result1 = self.store.find(Foo)
        result2 = self.store.find(Foo, id=20)
        result3 = result1.difference(result2)

        result3.order_by(Foo.title)
        assert [(foo.id, foo.title) for foo in result3] == [
            (30, "Title 10"),
            (10, "Title 30"),
        ]

        result3.order_by(Desc(Foo.title))
        assert [(foo.id, foo.title) for foo in result3] == [
            (10, "Title 30"),
            (30, "Title 10"),
        ]

    def test_result_difference_with_empty(self):
        result1 = self.store.find(Foo, id=30)
        result2 = EmptyResultSet()

        result3 = result1.difference(result2)

        assert [(foo.id, foo.title) for foo in result3] == [(30, "Title 10")]

    def test_result_difference_incompatible(self):
        result1 = self.store.find(Foo, id=10)
        result2 = self.store.find(Bar, id=100)
        with pytest.raises(FeatureError):
            result1.difference(result2)

    def test_result_difference_count(self):
        result1 = self.store.find(Foo)
        result2 = self.store.find(Foo, id=20)

        result3 = result1.difference(result2)

        assert result3.count() == 2

    def test_is_in_empty_result_set(self):
        result1 = self.store.find(Foo, Foo.id < 10)
        result2 = self.store.find(Foo, Or(Foo.id > 20, Foo.id.is_in(result1)))
        assert result2.count() == 1

    def test_is_in_empty_list(self):
        result2 = self.store.find(Foo, Eq(False, And(True, Foo.id.is_in([]))))
        assert result2.count() == 3

    def test_result_intersection(self):
        result1 = self.store.find(Foo)
        result2 = self.store.find(Foo, Foo.id.is_in((10, 30)))
        result3 = result1.intersection(result2)

        result3.order_by(Foo.title)
        assert [(foo.id, foo.title) for foo in result3] == [
            (30, "Title 10"),
            (10, "Title 30"),
        ]

        result3.order_by(Desc(Foo.title))
        assert [(foo.id, foo.title) for foo in result3] == [
            (10, "Title 30"),
            (30, "Title 10"),
        ]

    def test_result_intersection_with_empty(self):
        result1 = self.store.find(Foo, id=30)
        result2 = EmptyResultSet()
        result3 = result1.intersection(result2)

        assert len(list(result3)) == 0

    def test_result_intersection_incompatible(self):
        result1 = self.store.find(Foo, id=10)
        result2 = self.store.find(Bar, id=100)
        with pytest.raises(FeatureError):
            result1.intersection(result2)

    def test_result_intersection_count(self):
        result1 = self.store.find(Foo, Foo.id.is_in((10, 20)))
        result2 = self.store.find(Foo, Foo.id.is_in((10, 30)))
        result3 = result1.intersection(result2)

        assert result3.count() == 1

    def test_proxy(self):
        bar = self.store.get(BarProxy, 200)
        assert bar.foo_title == "Title 20"

    def test_proxy_equals(self):
        bar = self.store.find(BarProxy, BarProxy.foo_title == u"Title 20").one()
        assert bar
        assert bar.id == 200

    def test_proxy_as_column(self):
        result = self.store.find(BarProxy, BarProxy.id == 200)
        assert list(result.values(BarProxy.foo_title)) == ["Title 20"]

    def test_proxy_set(self):
        bar = self.store.get(BarProxy, 200)
        bar.foo_title = u"New Title"
        foo = self.store.get(Foo, 20)
        assert foo.title == "New Title"

    def get_bar_proxy_with_string(self):
        @add_metaclass(PropertyPublisherMeta)
        class Base(object):
            pass

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
        assert bar.foo_title == "Title 20"

    def test_proxy_with_string_variable_factory_attribute(self):
        MyBarProxy, MyFoo = self.get_bar_proxy_with_string()
        variable = MyBarProxy.foo_title.variable_factory(value=u"Hello")
        assert isinstance(variable, UnicodeVariable)

    def test_proxy_with_extra_table(self):
        """
        Proxies use a join on auto_tables. It should work even if we have
        more tables in the query.
        """
        result = self.store.find((BarProxy, Link),
                                 BarProxy.foo_title == u"Title 20",
                                 BarProxy.foo_id == Link.foo_id)
        results = list(result)
        assert len(results) == 2
        for bar, link in results:
            assert bar.id == 200
            assert bar.foo_title == u"Title 20"
            assert bar.foo_id == 20
            assert link.foo_id == 20

    def test_get_decimal_property(self):
        money = self.store.get(Money, 10)
        assert money.value == decimal.Decimal("12.3455")

    def test_set_decimal_property(self):
        money = self.store.get(Money, 10)
        money.value = decimal.Decimal("12.3456")
        self.store.flush()
        result = self.store.find(Money, value=decimal.Decimal("12.3456"))
        assert result.one() == money

    def test_fill_missing_primary_key_with_lazy_value(self):
        foo = self.store.get(Foo, 10)
        foo.id = SQL("40")
        self.store.flush()
        assert foo.id == 40
        assert self.store.get(Foo, 10) is None
        assert self.store.get(Foo, 40) == foo

    def test_fill_missing_primary_key_with_lazy_value_on_creation(self):
        foo = Foo()
        foo.id = SQL("40")
        self.store.add(foo)
        self.store.flush()
        assert foo.id == 40
        assert self.store.get(Foo, 40) == foo

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

            def connect(self, event=None):
                connection = self.database.connect(event)
                connection.preset_primary_key = preset_primary_key
                return connection

        store = Store(DatabaseWrapper(self.database))

        foo = store.add(Foo())

        store.flush()
        try:
            assert check == [[(False, None)], ["id"]]
            assert foo.id == 40
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
        assert len(cached) == 1
        foo = self.store.get(Foo, 20)
        assert cached == [foo]
        assert hasattr(foo, "tainted")

    def test_strong_cache_cleared_on_invalidate_all(self):
        cache = self.get_cache(self.store)
        foo = self.store.get(Foo, 20)
        assert cache.get_cached() == [get_obj_info(foo)]
        self.store.invalidate()
        assert cache.get_cached() == []

    def test_strong_cache_loses_object_on_invalidate(self):
        cache = self.get_cache(self.store)
        foo = self.store.get(Foo, 20)
        assert cache.get_cached() == [get_obj_info(foo)]
        self.store.invalidate(foo)
        assert cache.get_cached() == []

    def test_strong_cache_loses_object_on_remove(self):
        """
        Make sure an object gets removed from the strong reference
        cache when removed from the store.
        """
        cache = self.get_cache(self.store)
        foo = self.store.get(Foo, 20)
        assert cache.get_cached() == [get_obj_info(foo)]
        self.store.remove(foo)
        self.store.flush()
        assert cache.get_cached() == []

    def test_strong_cache_renews_object_on_get(self):
        cache = self.get_cache(self.store)
        foo1 = self.store.get(Foo, 10)
        foo2 = self.store.get(Foo, 20)
        foo1 = self.store.get(Foo, 10)
        assert cache.get_cached() == [get_obj_info(foo1), get_obj_info(foo2)]

    def test_strong_cache_renews_object_on_find(self):
        cache = self.get_cache(self.store)
        foo1 = self.store.find(Foo, id=10).one()
        foo2 = self.store.find(Foo, id=20).one()
        foo1 = self.store.find(Foo, id=10).one()
        assert cache.get_cached() == [get_obj_info(foo1), get_obj_info(foo2)]

    def test_unicode(self):
        class MyFoo(Foo):
            pass
        foo = self.store.get(Foo, 20)
        myfoo = self.store.get(MyFoo, 20)
        for title in [u'Cừơng', u'Đức', u'Hạnh']:
            foo.title = title
            self.store.commit()
            try:
                assert myfoo.title == title
            except AssertionError as e:
                raise AssertionError(ustr(e, 'replace') +
                    " (ensure your database was created with CREATE DATABASE"
                    " ... CHARACTER SET utf8)")

    def test_creation_order_is_preserved_when_possible(self):
        foos = [self.store.add(Foo()) for i in iter_range(10)]
        self.store.flush()
        for i in iter_range(len(foos)-1):
            assert foos[i].id < foos[i+1].id

    def test_update_order_is_preserved_when_possible(self):
        class MyFoo(Foo):
            sequence = 0
            def __storm_flushed__(self):
                self.flush_order = MyFoo.sequence
                MyFoo.sequence += 1

        foos = [self.store.add(MyFoo()) for i in iter_range(10)]
        self.store.flush()

        MyFoo.sequence = 0
        for foo in foos:
            foo.title = u"Changed Title"
        self.store.flush()

        for i, foo in enumerate(foos):
            assert foo.flush_order == i

    def test_removal_order_is_preserved_when_possible(self):
        class MyFoo(Foo):
            sequence = 0
            def __storm_flushed__(self):
                self.flush_order = MyFoo.sequence
                MyFoo.sequence += 1

        foos = [self.store.add(MyFoo()) for i in iter_range(10)]
        self.store.flush()

        MyFoo.sequence = 0
        for foo in foos:
            self.store.remove(foo)
        self.store.flush()

        for i, foo in enumerate(foos):
            assert foo.flush_order == i

    def test_cache_poisoning(self):
        """
        When a object update a field value to the previous value, which is in
        the cache, it correctly updates the value in the database.

        Because of change detection, this has been broken in the past, see bug
        #277095 in launchpad.
        """
        store = self.create_store()
        foo2 = store.get(Foo, 10)
        assert foo2.title == u"Title 30"
        store.commit()

        foo1 = self.store.get(Foo, 10)
        foo1.title = u"Title 40"
        self.store.commit()

        foo2.title = u"Title 30"
        store.commit()
        assert foo2.title == u"Title 30"

    def test_execute_sends_event(self):
        """Statement execution emits the register-transaction event."""
        calls = []
        def register_transaction(owner):
            calls.append(owner)
        self.store._event.hook("register-transaction", register_transaction)
        self.store.execute("SELECT 1")
        assert len(calls) == 1
        assert calls[0] == self.store

    def test_wb_event_before_check_connection(self):
        """
        The register-transaction event is emitted before checking the state of
        the connection.
        """
        calls = []
        def register_transaction(owner):
            calls.append(owner)
        self.store._event.hook("register-transaction", register_transaction)
        self.store._connection._state = STATE_DISCONNECTED
        with pytest.raises(DisconnectionError):
            self.store.execute("SELECT 1")
        assert len(calls) == 1
        assert calls[0] == self.store

    def test_add_sends_event(self):
        """Adding an object emits the register-transaction event."""
        calls = []
        def register_transaction(owner):
            calls.append(owner)
        self.store._event.hook("register-transaction", register_transaction)
        foo = Foo()
        foo.title = u"Foo"
        self.store.add(foo)
        assert len(calls) == 1
        assert calls[0] == self.store

    def test_remove_sends_event(self):
        """Adding an object emits the register-transaction event."""
        calls = []
        def register_transaction(owner):
            calls.append(owner)
        self.store._event.hook("register-transaction", register_transaction)
        foo = self.store.get(Foo, 10)
        del calls[:]

        self.store.remove(foo)
        assert len(calls) == 1
        assert calls[0] == self.store

    def test_change_invalidated_object_sends_event(self):
        """Modifying an object retrieved in a previous transaction emits the
        register-transaction event."""
        calls = []
        def register_transaction(owner):
            calls.append(owner)
        self.store._event.hook("register-transaction", register_transaction)
        foo = self.store.get(Foo, 10)
        self.store.rollback()
        del calls[:]

        foo.title = u"New title"
        assert len(calls) == 1
        assert calls[0] == self.store

    def test_rowcount_remove(self):
        # All supported backends support rowcount, so far.
        result_to_remove = self.store.find(Foo, Foo.id <= 30)
        assert result_to_remove.remove() == 3


class EmptyResultSetTest(object):

    def setUp(self):
        self.create_database()
        self.connection = self.database.connect()
        self.drop_tables()
        self.create_tables()
        self.create_store()
        # Most of the tests here exercise the same functionality using
        # self.empty and self.result to ensure that EmptyResultSet and
        # ResultSet behave the same way, in the same situations.
        self.empty = EmptyResultSet()
        self.result = self.store.find(Foo)

    def tearDown(self):
        self.drop_store()
        self.drop_tables()
        self.drop_database()
        self.connection.close()

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
            try:
                self.connection.execute("DROP TABLE %s" % table)
                self.connection.commit()
            except:
                self.connection.rollback()

    def drop_store(self):
        self.store.rollback()
        # Closing the store is needed because testcase objects are all
        # instantiated at once, and thus connections are kept open.
        self.store.close()

    def test_iter(self):
        assert list(self.result) == list(self.empty)

    def test_copy(self):
        assert self.result.copy() != self.result
        assert self.empty.copy() != self.empty
        assert list(self.result.copy()) == list(self.empty.copy())

    def test_config(self):
        self.result.config(distinct=True, offset=1, limit=1)
        self.empty.config(distinct=True, offset=1, limit=1)
        assert list(self.result) == list(self.empty)

    def test_slice(self):
        assert list(self.result[:]) == []
        assert list(self.empty[:]) == []

    def test_contains(self):
        assert Foo() not in self.empty

    def test_is_empty(self):
        assert self.result.is_empty() == True
        assert self.empty.is_empty() == True

    def test_any(self):
        assert self.result.any() == None
        assert self.empty.any() == None

    def test_first_unordered(self):
        with pytest.raises(UnorderedError):
            self.result.first()
        with pytest.raises(UnorderedError):
            self.empty.first()

    def test_first_ordered(self):
        self.result.order_by(Foo.title)
        assert self.result.first() == None
        self.empty.order_by(Foo.title)
        assert self.empty.first() == None

    def test_last_unordered(self):
        with pytest.raises(UnorderedError):
            self.result.last()
        with pytest.raises(UnorderedError):
            self.empty.last()

    def test_last_ordered(self):
        self.result.order_by(Foo.title)
        assert self.result.last() == None
        self.empty.order_by(Foo.title)
        assert self.empty.last() == None

    def test_one(self):
        assert self.result.one() == None
        assert self.empty.one() == None

    def test_order_by(self):
        assert self.result.order_by(Foo.title) == self.result
        assert self.empty.order_by(Foo.title) == self.empty

    def test_group_by(self):
        assert self.result.group_by(Foo.title) == self.result
        assert self.empty.group_by(Foo.title) == self.empty

    def test_remove(self):
        assert self.result.remove() == 0
        assert self.empty.remove() == 0

    def test_count(self):
        assert self.result.count() == 0
        assert self.empty.count() == 0
        assert self.empty.count(expr="abc") == 0
        assert self.empty.count(distinct=True) == 0

    def test_max(self):
        assert self.result.max(Foo.id) == None
        assert self.empty.max(Foo.id) == None

    def test_min(self):
        assert self.result.min(Foo.id) == None
        assert self.empty.min(Foo.id) == None

    def test_avg(self):
        assert self.result.avg(Foo.id) == None
        assert self.empty.avg(Foo.id) == None

    def test_sum(self):
        assert self.result.sum(Foo.id) == None
        assert self.empty.sum(Foo.id) == None

    def test_get_select_expr_without_columns(self):
        """
        A L{FeatureError} is raised if L{EmptyResultSet.get_select_expr} is
        called without a list of L{Column}s.
        """
        with pytest.raises(FeatureError):
            self.result.get_select_expr()
        with pytest.raises(FeatureError):
            self.empty.get_select_expr()

    def test_get_select_expr_(self):
        """
        A L{FeatureError} is raised if L{EmptyResultSet.get_select_expr} is
        called without a list of L{Column}s.
        """
        subselect = self.result.get_select_expr(Foo.id)
        assert (Foo.id,) == subselect.columns
        result = self.store.find(Foo, Foo.id.is_in(subselect))
        assert list(result) == []

        subselect = self.empty.get_select_expr(Foo.id)
        assert (Foo.id,) == subselect.columns
        result = self.store.find(Foo, Foo.id.is_in(subselect))
        assert list(result) == []

    def test_values_no_columns(self):
        with pytest.raises(FeatureError):
            list(self.result.values())
        with pytest.raises(FeatureError):
            list(self.empty.values())

    def test_values(self):
        assert list(self.result.values(Foo.title)) == []
        assert list(self.empty.values(Foo.title)) == []

    def test_set_no_args(self):
        assert self.result.set() == None
        assert self.empty.set() == None

    def test_cached(self):
        assert self.result.cached() == []
        assert self.empty.cached() == []

    def test_find(self):
        assert list(self.result.find(Foo.title == u"foo")) == []
        assert list(self.empty.find(Foo.title == u"foo")) == []

    def test_union(self):
        assert self.empty.union(self.empty) == self.empty
        assert type(self.empty.union(self.result)) == type(self.result)
        assert type(self.result.union(self.empty)) == type(self.result)

    def test_difference(self):
        assert self.empty.difference(self.empty) == self.empty
        assert self.empty.difference(self.result) == self.empty
        assert self.result.difference(self.empty) == self.result

    def test_intersection(self):
        assert self.empty.intersection(self.empty) == self.empty
        assert self.empty.intersection(self.result) == self.empty
        assert self.result.intersection(self.empty) == self.empty
