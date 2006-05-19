from weakref import WeakValueDictionary

from storm.properties import get_cls_info, get_obj_info, get_info
from storm.expr import Select, Insert, Update, Delete, Undef
from storm.expr import Column, Param, Count, Max, Min, Avg, Sum


class StoreError(Exception):
    pass


STATE_ADDED = 1
STATE_LOADED = 2
STATE_REMOVED = 3


class Store(object):

    def __init__(self, database):
        self._connection = database.connect()
        self._cache = WeakValueDictionary()
        self._dirty = {}

    @staticmethod
    def of(obj):
        try:
            return get_obj_info(obj).get("store")
        except AttributeError:
            return None

    def execute(self, statement, params=None, noresult=False):
        self.flush()
        return self._connection.execute(statement, params, noresult)

    def commit(self):
        self.flush()
        self._connection.commit()

    def rollback(self):
        dirty = self._dirty
        for id in dirty.keys():
            obj_info = get_obj_info(dirty[id])
            state = obj_info["state"]
            if state is STATE_REMOVED:
                obj_info["state"] = STATE_LOADED
                del dirty[id]
            elif state is STATE_ADDED:
                obj_info["state"] = None
                del dirty[id]
        self._connection.rollback()

    def get(self, cls, key):
        self.flush()

        if type(key) != tuple:
            key = (key,)
        
        #cached = self._cache.get((cls,)+key)
        #if cached is not None:
        #    return cached

        cls_info = get_cls_info(cls)

        # TODO: Assert same size

        where = None
        for i, prop in enumerate(cls_info.primary_key):
            if where is None:
                where = (prop == key[i])
            else:
                where &= (prop == key[i])

        select = Select(cls_info.properties, where,
                        default_tables=cls_info.table, limit=1)

        values = self._connection.execute(select).fetch_one()
        if values is None:
            return None

        return self._load_object(cls_info, values)

    def find(self, cls, *args, **kwargs):
        self.flush()

        cls_info = get_cls_info(cls)

        where = Undef
        if args:
            for arg in args:
                if where is Undef:
                    where = arg
                else:
                    where &= arg
        if kwargs:
            for key in kwargs:
                if where is Undef:
                    where = getattr(cls, key) == kwargs[key]
                else:
                    where &= getattr(cls, key) == kwargs[key]

        return ResultSet(self._connection.execute,
                         lambda values: self._load_object(cls_info, values),
                         cls_info.properties, where, cls_info.table)

    def add(self, obj):
        obj_info = get_obj_info(obj)
        store = obj_info.get("store")
        if store and store is not self:
            raise StoreError("%r is already in another store" % obj)
        state = obj_info.get("state")
        if state is STATE_REMOVED:
            obj_info["state"] = STATE_LOADED
        elif state is None:
            obj_info["store"] = self
            obj_info["state"] = STATE_ADDED
            self._dirty[id(obj)] = obj

    def remove(self, obj):
        obj_info = get_obj_info(obj)
        store = obj_info.get("store")
        if store and store is not self:
            raise StoreError("%r is not in this store" % obj)
        state = obj_info.get("state")
        if state is STATE_ADDED:
            obj_info["state"] = None
            obj_info["store"] = None
            del self._dirty[id(obj)]
        elif state is STATE_LOADED:
            obj_info["state"] = STATE_REMOVED
            self._dirty[id(obj)] = obj

    def flush(self):
        if not self._dirty:
            return
        for obj in self._dirty.values():
            obj_info, cls_info = get_info(obj)
            state = obj_info.get("state")
            if state is STATE_ADDED:
                expr = Insert(cls_info.properties,
                              [Param(prop.__get__(obj))
                               for prop in cls_info.properties],
                              cls_info.table)
                self._connection.execute(expr, noresult=True)
                self._reset_info(cls_info, obj_info, obj)
                obj_info["state"] = STATE_LOADED
            elif state is STATE_REMOVED:
                expr = Delete(self._build_key_where(cls_info, obj_info, obj),
                              cls_info.table)
                self._connection.execute(expr, noresult=True)
                obj_info["state"] = None
                obj_info["store"] = None
                obj_info.set_change_notification(None)
            elif state is STATE_LOADED and obj_info.check_changed():
                changes = obj_info.get_changes().copy()
                for column in changes:
                    changes[column] = Param(changes[column])
                expr = Update(changes,
                              self._build_key_where(cls_info, obj_info, obj),
                              cls_info.table)
                self._connection.execute(expr, noresult=True)
                self._reset_info(cls_info, obj_info, obj)
        self._dirty.clear()

    def _build_key_where(self, cls_info, obj_info, obj):
        where = Undef
        for prop, value in zip(cls_info.primary_key, obj_info["primary_key"]):
            if where is Undef:
                where = (prop == value)
            else:
                where &= (prop == value)
        return where

    def _load_object(self, cls_info, values):
        cls = cls_info.cls
        obj = self._cache.get((cls,)+tuple(values[i]
                                           for i in cls_info.primary_key_pos))
        if obj is not None:
            # FIXME: Check if object was not removed.
            return obj
        obj = object.__new__(cls)
        obj_info = get_obj_info(obj)
        obj_info["state"] = STATE_LOADED
        for attr, value in zip(cls_info.attributes, values):
            setattr(obj, attr, value)
        self._reset_info(cls_info, obj_info, obj)
        obj_info.save()
        return obj

    def _reset_info(self, cls_info, obj_info, obj):
        obj_info["store"] = self
        obj_info["primary_key"] = tuple(prop.__get__(obj)
                                        for prop in cls_info.primary_key)
        obj_info["cache_key"] = (cls_info.cls,)+obj_info["primary_key"]
        self._cache[obj_info["cache_key"]] = obj
        obj_info.set_change_notification(self._object_changed)
        return obj_info

    def _object_changed(self, obj, prop, old_value, new_value):
        # XXX: If prop is part of the primary key, the cache will
        #      contain an invalid key from now on. Which will result
        #      in get() acting wrong when using the cache.
        self._dirty[id(obj)] = obj


class ResultSet(object):

    def __init__(self, execute, object_factory, columns, where, table,
                 order_by=Undef):
        self._execute = execute
        self._object_factory = object_factory
        self._columns = columns
        self._where = where
        self._table = table
        self._order_by = order_by

    def __iter__(self):
        select = Select(self._columns, self._where, order_by=self._order_by,
                        default_tables=self._table, distinct=True)
        for values in self._execute(select):
            yield self._object_factory(values)

    def _aggregate(self, column):
        select = Select(column, self._where, order_by=self._order_by,
                        default_tables=self._table, distinct=True)
        return self._execute(select).fetch_one()[0]

    def one(self):
        select = Select(self._columns, self._where, order_by=self._order_by,
                        limit=1, default_tables=self._table, distinct=True)
        values = self._execute(select).fetch_one()
        if values:
            return self._object_factory(values)
        return None

    def order_by(self, *args):
        return self.__class__(self._execute, self._object_factory,
                              self._columns, self._where, self._table, args)

    def remove(self):
        self._execute(Delete(self._where, self._table), noresult=True)

    def count(self):
        return self._aggregate(Count())

    def max(self, prop):
        return self._aggregate(Max(prop))

    def min(self, prop):
        return self._aggregate(Min(prop))

    def avg(self, prop):
        return self._aggregate(Avg(prop))

    def sum(self, prop):
        return self._aggregate(Sum(prop))
