from weakref import WeakValueDictionary

from storm.info import get_cls_info, get_obj_info, get_info
from storm.expr import Select, Insert, Update, Delete, Undef, compare_columns
from storm.expr import Column, Param, Count, Max, Min, Avg, Sum


__all__ = ["Store", "StoreError", "ResultSet"]


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
        self._order = {} # (id, id) = count

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

        if type(key) != tuple:
            key = (key,)

        cls_info = get_cls_info(cls)

        assert len(key) == len(cls_info.primary_key)

        cached = self._cache.get((cls, key))
        if cached is not None:
            return cached
        
        where = compare_columns(cls_info.primary_key, key)

        select = Select(cls_info.columns, where,
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
                         cls_info.columns, where, cls_info.table)

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

    def add_flush_order(self, before, after):
        pair = (id(before), id(after))
        try:
            self._order[pair] += 1
        except KeyError:
            self._order[pair] = 1

    def remove_flush_order(self, before, after):
        pair = (id(before), id(after))
        try:
            self._order[pair] -= 1
        except KeyError:
            pass

    def flush(self):
        predecessors = {}
        for (before, after), n in self._order.iteritems():
            if n > 0:
                before_set = predecessors.get(after)
                if before_set is None:
                    predecessors[after] = set((before,))
                else:
                    before_set.add(before)

        while self._dirty:
            for obj_id, obj in self._dirty.iteritems():
                for before in predecessors.get(obj_id, ()):
                    if before in self._dirty:
                        break # A predecessor is still dirty.
                else:
                    break # Found an item without dirty predecessors.
            else:
                raise StoreError("Can't flush due to ordering loop")
            self._flush_one(obj)

        self._order.clear()

    def _flush_one(self, obj):
        if self._dirty.pop(id(obj), None) is None:
            return

        obj_info, cls_info = get_info(obj)

        pending = obj_info.pop("pending", None)

        if pending is PENDING_REMOVE:
            expr = Delete(compare_columns(cls_info.primary_key,
                                          obj_info["primary_values"]),
                          cls_info.table)
            self._connection.execute(expr, noresult=True)

            self._disable_change_notification(obj)
            self._set_ghost(obj)
            self._remove_from_cache(obj)

        elif pending is PENDING_ADD:
            columns = []
            values = []

            for column in cls_info.columns:
                value = obj_info.get_value(column.name, Undef)
                if value is not Undef:
                    columns.append(column)
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
                              compare_columns(cls_info.primary_key,
                                              obj_info["primary_values"]),
                              cls_info.table)
                self._connection.execute(expr, noresult=True)

                self._add_to_cache(obj)

        obj_info.emit("flushed")
        

    def _fill_missing_values(self, obj, result):
        obj_info, cls_info = get_info(obj)

        missing_columns = []
        missing_attributes = []
        for column, attr in zip(cls_info.columns, cls_info.attributes):
            if not obj_info.has_value(column.name):
                missing_columns.append(column)
                missing_attributes.append(attr)

        if missing_columns:
            primary_key = cls_info.primary_key
            primary_values = tuple(obj_info.get_value(column.name, Undef)
                                   for column in primary_key)
            if Undef in primary_values:
                where = result.get_insert_identity(primary_key, primary_values)
            else:
                where = compare_columns(primary_key, primary_values)
            select = Select(missing_columns, where)
            result = self._connection.execute(select)
            for attr, value in zip(missing_attributes, result.fetch_one()):
                setattr(obj, attr, value)

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
        new_primary_values = tuple(obj_info.get_value(prop.name)
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
        get_obj_info(obj).hook("changed", self._object_changed)

    def _disable_change_notification(self, obj):
        get_obj_info(obj).unhook("changed", self._object_changed)

    def _object_changed(self, obj_info, name, old_value, new_value):
        if new_value is not Undef:
            self._dirty[id(obj_info.obj)] = obj_info.obj


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
