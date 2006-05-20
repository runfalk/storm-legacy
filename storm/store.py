from weakref import WeakValueDictionary

from storm.properties import get_cls_info, get_obj_info, get_info
from storm.expr import Select, Insert, Update, Delete, Undef
from storm.expr import Column, Param, Count, Max, Min, Avg, Sum


class StoreError(Exception):
    pass


PENDING_ADD = 1
PENDING_REMOVE = 2


class Store(object):

    def __init__(self, database):
        self._connection = database.connect()
        self._cache = WeakValueDictionary()
        self._ghosts = {}
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
        for obj in self._iter_ghosts():
            del get_obj_info(obj)["store"]
        for obj in self._iter_cached():
            get_obj_info(obj).save()
        self._ghosts.clear()

    def rollback(self):
        objects = {}
        for obj in self._iter_dirty():
            objects[id(obj)] = obj
        for obj in self._iter_ghosts():
            objects[id(obj)] = obj
        for obj in self._iter_cached():
            objects[id(obj)] = obj

        for obj in objects.values():
            self._remove_from_cache(obj)

            obj_info = get_obj_info(obj)
            obj_info.restore()

            if obj_info.get("store") is self:
                self._add_to_cache(obj)
                self._enable_change_notification(obj)

        self._ghosts.clear()
        self._dirty.clear()
        self._connection.rollback()

    def get(self, cls, key):
        self.flush()

        if type(key) != tuple: # XXX: Assert same size
            key = (key,)
        
        cached = self._cache.get((cls, key))
        if cached is not None:
            return cached

        cls_info = get_cls_info(cls)

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
        if store is not None and store is not self:
            raise StoreError("%r is part of another store" % obj)

        pending = obj_info.get("pending")

        if pending is PENDING_ADD:
            raise StoreError("%r is already scheduled to be added" % obj)
        elif pending is PENDING_REMOVE:
            del obj_info["pending"]
        else:
            if store is None:
                obj_info.save()
                obj_info["store"] = self
            else:
                if not self._is_ghost(obj):
                    raise StoreError("%r is already in the store" % obj)
                self._set_alive(obj)

            obj_info["pending"] = PENDING_ADD
            self._set_dirty(obj)

    def remove(self, obj):
        obj_info = get_obj_info(obj)

        store = obj_info.get("store")
        if store is None or store is not self:
            raise StoreError("%r is not in this store" % obj)

        pending = obj_info.get("pending")

        if pending is PENDING_REMOVE:
            raise StoreError("%r is already scheduled to be removed" % obj)
        elif pending is PENDING_ADD:
            del obj_info["pending"]
            self._set_ghost(obj)
            self._set_clean(obj)
        else:
            obj_info["pending"] = PENDING_REMOVE
            self._set_dirty(obj)

    def flush(self):
        if not self._dirty:
            return
        for obj in self._iter_dirty():
            obj_info, cls_info = get_info(obj)

            pending = obj_info.get("pending")

            if pending is PENDING_REMOVE:
                del obj_info["pending"]
                
                expr = Delete(self._build_where(cls_info.primary_key,
                                                obj_info["primary_values"]),
                              cls_info.table)
                self._connection.execute(expr, noresult=True)

                self._disable_change_notification(obj)
                self._set_ghost(obj)
                self._remove_from_cache(obj)

            elif pending is PENDING_ADD:
                del obj_info["pending"]

                columns = []
                values = []

                for prop in cls_info.properties:
                    value = prop.__get__(obj, accept_undef=True)
                    if value is not Undef:
                        columns.append(prop)
                        values.append(Param(value))

                expr = Insert(columns, values, cls_info.table)

                result = self._connection.execute(expr)

                self._fill_missing_values(obj, result)

                self._enable_change_notification(obj)
                self._set_alive(obj)
                self._add_to_cache(obj)

            elif obj_info.check_changed():

                changes = {}
                for column, value in obj_info.get_changes().iteritems():
                    if value is not Undef:
                        changes[column] = Param(value)

                if changes:
                    expr = Update(changes,
                                  self._build_where(cls_info.primary_key,
                                                    obj_info["primary_values"]),
                                  cls_info.table)
                    self._connection.execute(expr, noresult=True)

                    self._add_to_cache(obj)

        self._dirty.clear()

    def _fill_missing_values(self, obj, result):
        obj_info, cls_info = get_info(obj)

        missing_properties = []
        for prop in cls_info.properties:
            if not obj_info.has_prop(prop):
                missing_properties.append(prop)

        if missing_properties:
            primary_key = cls_info.primary_key
            primary_values = tuple(prop.__get__(obj, accept_undef=True)
                                   for prop in primary_key)
            if Undef in primary_values:
                where = result.get_insert_identity(primary_key, primary_values)
            else:
                where = self._build_where(primary_key, primary_values)
            select = Select(missing_properties, where)
            result = self._connection.execute(select)
            for prop, value in zip(missing_properties, result.fetch_one()):
                prop.__set__(obj, value)

    def _build_where(self, columns, values):
        where = Undef
        for prop, value in zip(columns, values):
            if where is Undef:
                where = (prop == value)
            else:
                where &= (prop == value)
        return where

    def _load_object(self, cls_info, values):
        primary_values = tuple(values[i] for i in cls_info.primary_key_pos)
        obj = self._cache.get((cls_info.cls, primary_values))
        if obj is not None:
            return obj

        obj = object.__new__(cls_info.cls)
        obj_info = get_obj_info(obj)
        obj_info["store"] = self

        for attr, value in zip(cls_info.attributes, values):
            setattr(obj, attr, value)

        obj_info.save()

        self._add_to_cache(obj)
        self._enable_change_notification(obj)

        load = getattr(obj, "__load__", None)
        if load is not None:
            load()

        obj_info.save_attributes()

        return obj


    def _is_dirty(self, obj):
        return id(obj) in self._dirty

    def _set_dirty(self, obj):
        self._dirty[id(obj)] = obj

    def _set_clean(self, obj):
        self._dirty.pop(id(obj), None)

    def _iter_dirty(self):
        return self._dirty.itervalues()


    def _is_ghost(self, obj):
        return id(obj) in self._ghosts

    def _set_ghost(self, obj):
        self._ghosts[id(obj)] = obj

    def _set_alive(self, obj):
        self._ghosts.pop(id(obj), None)

    def _iter_ghosts(self):
        return self._ghosts.itervalues()


    def _add_to_cache(self, obj):
        obj_info, cls_info = get_info(obj)
        old_primary_values = obj_info.get("primary_values")
        new_primary_values = tuple(prop.__get__(obj)
                                   for prop in cls_info.primary_key)
        if new_primary_values == old_primary_values:
            return
        if old_primary_values is not None:
            del self._cache[obj.__class__, old_primary_values]
        self._cache[obj.__class__, new_primary_values] = obj
        obj_info["primary_values"] = new_primary_values

    def _remove_from_cache(self, obj):
        obj_info = get_obj_info(obj)
        primary_values = obj_info.get("primary_values")
        if primary_values is not None:
            del self._cache[obj.__class__, primary_values]
            del obj_info["primary_values"]

    def _iter_cached(self):
        return self._cache.itervalues()


    def _enable_change_notification(self, obj):
        get_obj_info(obj).set_change_notification(self._object_changed)

    def _disable_change_notification(self, obj):
        get_obj_info(obj).set_change_notification(None)

    def _object_changed(self, obj, prop, old_value, new_value):
        if new_value is not Undef:
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
