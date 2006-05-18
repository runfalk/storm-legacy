from weakref import WeakValueDictionary

from storm.expr import Select, Insert, Update, Delete
from storm.expr import Column, Param, Count, Max, Min, Avg, Sum
from storm.properties import ClassInfo, ObjectInfo


#class InvalidKeyError(Exception):
#    pass
#
#class WrongStoreError(Exception):
#    pass
#
#class AlreadyInStoreError(Exception):
#    pass

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
            return ObjectInfo(obj).store
        except AttributeError:
            return None

    def get_connection(self):
        return self._connection

    def commit(self):
        self.flush()
        self._connection.commit()

    def rollback(self):
        dirty = self._dirty
        for id in dirty.keys():
            obj_info = ObjectInfo(dirty[id])
            if obj_info.state == STATE_REMOVED:
                obj_info.state = STATE_LOADED
                del dirty[id]
            elif obj_info.state == STATE_ADDED:
                obj_info.state = None
                del dirty[id]
        self._connection.rollback()

    def get(self, cls, key):
        self.flush()

        if type(key) != tuple:
            key = (key,)

        cls_info = ClassInfo(cls)

        where = None
        for i, prop in enumerate(cls_info.pk_prop_insts):
            if where is None:
                where = (prop == key[i])
            else:
                where &= (prop == key[i])

        select = Select(cls_info.prop_insts, cls_info.tables, where, limit=1)

        prop_values = self._connection.execute(select).fetch_one()
        if prop_values is None:
            return None

        return self._load_object(cls_info, prop_values)

    def find(self, cls, *args, **kwargs):
        self.flush()

        cls_info = ClassInfo(cls)

        where = None
        if args:
            for arg in args:
                if where is None:
                    where = arg
                else:
                    where &= arg
        if kwargs:
            for key in kwargs:
                if where is None:
                    where = cls_info.attr_dict[key] == kwargs[key]
                else:
                    where &= cls_info.attr_dict[key] == kwargs[key]

        return ResultSet(self._connection.execute,
                         lambda values: self._load_object(cls_info, values),
                         columns=cls_info.prop_insts,
                         tables=cls_info.tables, where=where)

    def add(self, obj):
        # TODO: Check if object is in some store already. force=True could
        #       make it accept anyway, unless the store is already self.
        obj_info = ObjectInfo(obj)
        state = getattr(obj_info, "state", None)
        if state == STATE_REMOVED:
            obj_info.state = STATE_LOADED
        else:
            #obj_info.store = self
            obj_info.state = STATE_ADDED
            self._dirty[id(obj)] = obj

    def remove(self, obj):
        obj_info = ObjectInfo(obj)
        state = getattr(obj_info, "state", None)
        if state == STATE_ADDED:
            obj_info.state = None
        else:
            obj_info.state = STATE_REMOVED
        self._dirty[id(obj)] = obj

    def flush(self):
        if not self._dirty:
            return
        for obj in self._dirty.values():
            cls_info = ClassInfo(obj.__class__)
            obj_info = ObjectInfo(obj)
            state = obj_info.state
            if state == STATE_ADDED:
                expr = Insert(cls_info.table, cls_info.columns,
                              [Param(prop.__get__(obj))
                               for prop in cls_info.prop_insts])
                self._connection.execute(expr)
                self._reset_info(cls_info, obj_info, obj)
                obj_info.state = STATE_LOADED
            elif state == STATE_REMOVED:
                expr = Delete(cls_info.table,
                              self._build_key_where(cls_info, obj_info, obj))
                self._connection.execute(expr)
                obj_info.state = None
                obj_info.store = None
                obj_info.set_change_notification(None)
            elif state == STATE_LOADED and obj_info.check_changed():
                sets = [(Column(prop.name), Param(value))
                        for prop, value in obj_info.get_changes().items()]
                expr = Update(cls_info.table, sets,
                              self._build_key_where(cls_info, obj_info, obj))
                self._connection.execute(expr)
                self._reset_info(cls_info, obj_info, obj)
        self._dirty.clear()

    def _build_key_where(self, cls_info, obj_info, obj):
        where = None
        for prop, value in zip(cls_info.pk_prop_insts, obj_info.pk_values):
            if where is None:
                where = (prop == value)
            else:
                where &= (prop == value)
        return where

    def _load_object(self, cls_info, prop_values):
        cls = cls_info.cls
        obj = self._cache.get((cls,)+tuple(prop_values[i]
                                           for i in cls_info.pk_prop_idx))
        if obj is not None:
            return obj
        obj = object.__new__(cls)
        obj_info = ObjectInfo(obj)
        obj_info.state = STATE_LOADED
        for name, value in zip(cls_info.attr_names, prop_values):
            setattr(obj, name, value)
        self._reset_info(cls_info, obj_info, obj)
        return obj

    def _reset_info(self, cls_info, obj_info, obj):
        obj_info.store = self
        obj_info.pk_values = tuple(prop.__get__(obj)
                                   for prop in cls_info.pk_prop_insts)
        obj_info.save_state()
        obj_info.set_change_notification(self._object_changed)
        self._cache[(cls_info.cls,)+obj_info.pk_values] = obj
        return obj_info

    def _object_changed(self, obj, prop, old_value, new_value):
        self._dirty[id(obj)] = obj


class ResultSet(object):

    def __init__(self, execute, object_factory, **kwargs):
        self._execute = execute
        self._object_factory = object_factory
        self._kwargs = kwargs

    def __iter__(self):
        for values in self._execute(Select(**self._kwargs)):
            yield self._object_factory(values)

    def _aggregate(self, column):
        return self._execute(Select((column,), self._kwargs.get("tables"),
                                    self._kwargs.get("where"))).fetch_one()[0]

    def one(self):
        kwargs = self._kwargs.copy()
        kwargs["limit"] = 1
        values = self._execute(Select(**kwargs)).fetch_one()
        if values:
            return self._object_factory(values)
        return None

    def order_by(self, *args):
        kwargs = self._kwargs.copy()
        kwargs["order_by"] = args
        return self.__class__(self._execute, self._object_factory, **kwargs)

    def remove(self):
        delete = Delete(self._kwargs.get("tables")[0],
                        self._kwargs.get("where"))
        self._execute(delete)

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
