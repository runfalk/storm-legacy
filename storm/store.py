#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
from weakref import WeakValueDictionary

from storm.info import get_cls_info, get_obj_info, get_info
from storm.expr import Select, Insert, Update, Delete
from storm.expr import Column, Count, Max, Min, Avg, Sum, Eq, Expr, And
from storm.expr import compile_python, compare_columns, CompileError
from storm.variables import Variable
from storm.exceptions import (
    WrongStoreError, NotFlushedError, OrderLoopError, SetError)
from storm import Undef


__all__ = ["Store"]


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

        primary_vars = []
        for column, variable in zip(cls_info.primary_key, key):
            if not isinstance(variable, Variable):
                variable = column.variable_factory(value=variable)
            primary_vars.append(variable)

        cached = self._cache.get((cls, tuple(primary_vars)))
        if cached is not None:
            return cached

        where = compare_columns(cls_info.primary_key, primary_vars)

        select = Select(cls_info.columns, where,
                        default_tables=cls_info.table, limit=1)

        result = self._connection.execute(select)
        values = result.get_one()
        if values is None:
            return None
        return self._load_object(cls_info, result, values)

    def find(self, cls, *args, **kwargs):
        self.flush()
        cls_info = get_cls_info(cls)
        equals = list(args)
        if kwargs:
            for key, value in kwargs.items():
                column = getattr(cls, key)
                equals.append(Eq(column, column.variable_factory(value=value)))
        if equals:
            where = And(*equals)
        else:
            where = Undef
        return ResultSet(self, cls_info, where)

    def new(self, cls, *args, **kwargs):
        obj = cls(*args, **kwargs)
        self.add(obj)
        return obj

    def add(self, obj):
        obj_info = get_obj_info(obj)

        store = obj_info.get("store")
        if store is not None and store is not self:
            raise WrongStoreError("%r is part of another store" % obj)

        pending = obj_info.get("pending")

        if pending is PENDING_ADD:
            pass
        elif pending is PENDING_REMOVE:
            del obj_info["pending"]
            # obj_info.event.emit("added")
        else:
            if store is None:
                obj_info.save()
                obj_info["store"] = self
            elif not self._is_ghost(obj):
                return # It's fine already.
            else:
                self._set_alive(obj)

            obj_info["pending"] = PENDING_ADD
            self._set_dirty(obj)
            obj_info.event.emit("added")

    def remove(self, obj):
        obj_info = get_obj_info(obj)

        if obj_info.get("store") is not self:
            raise WrongStoreError("%r is not in this store" % obj)

        pending = obj_info.get("pending")

        if pending is PENDING_REMOVE:
            pass
        elif pending is PENDING_ADD:
            del obj_info["pending"]
            self._set_ghost(obj)
            self._set_clean(obj)
        elif not self._is_ghost(obj):
            obj_info["pending"] = PENDING_REMOVE
            self._set_dirty(obj)

    def reload(self, obj):
        obj_info, cls_info = get_info(obj)
        if obj_info.get("store") is not self or self._is_ghost(obj):
            raise WrongStoreError("%r is not in this store" % obj)
        if "primary_vars" not in obj_info:
            raise NotFlushedError("Can't reload an object if it was "
                                  "never flushed")
        where = compare_columns(cls_info.primary_key,
                                obj_info["primary_vars"])
        select = Select(cls_info.columns, where,
                        default_tables=cls_info.table, limit=1)
        result = self._connection.execute(select)
        values = result.get_one()
        self._set_values(obj_info, cls_info.columns, result, values)
        obj_info.checkpoint()
        self._set_clean(obj)

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
                raise OrderLoopError("Can't flush due to ordering loop")
            self._flush_one(obj)

        self._order.clear()

    def _flush_one(self, obj):
        if self._dirty.pop(id(obj), None) is None:
            return

        obj_info, cls_info = get_info(obj)

        pending = obj_info.pop("pending", None)

        if pending is PENDING_REMOVE:
            expr = Delete(compare_columns(cls_info.primary_key,
                                          obj_info["primary_vars"]),
                          cls_info.table)
            self._connection.execute(expr, noresult=True)

            self._disable_change_notification(obj)
            self._set_ghost(obj)
            self._remove_from_cache(obj)

        elif pending is PENDING_ADD:
            columns = []
            variables = []

            for column in cls_info.columns:
                variable = obj_info.variables[column]
                if variable.is_defined():
                    columns.append(column)
                    variables.append(variable)

            expr = Insert(columns, variables, cls_info.table)

            result = self._connection.execute(expr)

            self._fill_missing_values(obj, result)

            self._enable_change_notification(obj)
            self._set_alive(obj)
            self._add_to_cache(obj)

            obj_info.checkpoint()

        else:

            changes = {}
            for column in cls_info.columns:
                variable = obj_info.variables[column]
                if variable.has_changed() and variable.is_defined():
                    changes[column] = variable

            if changes:
                expr = Update(changes,
                              compare_columns(cls_info.primary_key,
                                              obj_info["primary_vars"]),
                              cls_info.table)
                self._connection.execute(expr, noresult=True)

                self._add_to_cache(obj)

            obj_info.checkpoint()

        obj_info.event.emit("flushed")

    def _fill_missing_values(self, obj, result):
        obj_info, cls_info = get_info(obj)

        missing_columns = []
        for column in cls_info.columns:
            if not obj_info.variables[column].is_defined():
                missing_columns.append(column)

        if missing_columns:
            primary_key = cls_info.primary_key
            primary_vars = obj_info.primary_vars

            for variable in primary_vars:
                if not variable.is_defined():
                    where = result.get_insert_identity(primary_key,
                                                       primary_vars)
                    break
            else:
                where = compare_columns(primary_key, primary_vars)

            result = self._connection.execute(Select(missing_columns, where))

            self._set_values(obj_info, missing_columns,
                             result, result.get_one())

    def _load_object(self, cls_info, result, values, obj=None):
        if obj is None:
            primary_vars = []
            columns = cls_info.columns
            for i in cls_info.primary_key_pos:
                variable = columns[i].variable_factory(value=values[i],
                                                       from_db=True)
                primary_vars.append(variable)
            obj = self._cache.get((cls_info.cls, tuple(primary_vars)))
            if obj is not None:
                return obj
            obj = object.__new__(cls_info.cls)

        obj_info = get_obj_info(obj)
        obj_info["store"] = self

        self._set_values(obj_info, cls_info.columns, result, values)

        obj_info.save()

        self._add_to_cache(obj)
        self._enable_change_notification(obj)

        load = getattr(obj, "__load__", None)
        if load is not None:
            load()

        obj_info.save_attributes()

        return obj

    def _set_values(self, obj_info, columns, result, values):
        for column, value in zip(columns, values):
            if value is None:
                obj_info.variables[column].set(value, from_db=True)
            else:
                result.set_variable(obj_info.variables[column], value)


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
        old_primary_vars = obj_info.get("primary_vars")
        if old_primary_vars == obj_info.primary_vars:
            return
        if old_primary_vars is not None:
            del self._cache[obj.__class__, old_primary_vars]
        new_primary_vars = tuple(variable.copy()
                                 for variable in obj_info.primary_vars)
        self._cache[obj.__class__, new_primary_vars] = obj
        obj_info["primary_vars"] = new_primary_vars

    def _remove_from_cache(self, obj):
        obj_info = get_obj_info(obj)
        primary_vars = obj_info.get("primary_vars")
        if primary_vars is not None:
            del self._cache[obj.__class__, primary_vars]
            del obj_info["primary_vars"]

    def _iter_cached(self):
        return self._cache.values()


    def _enable_change_notification(self, obj):
        get_obj_info(obj).event.hook("changed", self._variable_changed)

    def _disable_change_notification(self, obj):
        get_obj_info(obj).event.unhook("changed", self._variable_changed)

    def _variable_changed(self, obj_info, variable, old_value, new_value):
        if new_value is not Undef:
            self._dirty[id(obj_info.obj)] = obj_info.obj


class ResultSet(object):

    def __init__(self, store, cls_info, where, order_by=Undef):
        self._store = store
        self._cls_info = cls_info
        self._where = where
        self._order_by = order_by

    def __iter__(self):
        select = Select(self._cls_info.columns, self._where,
                        default_tables=self._cls_info.table,
                        order_by=self._order_by, distinct=True)
        result = self._store._connection.execute(select)
        for values in result:
            yield self._store._load_object(self._cls_info, result, values)

    def _aggregate(self, column):
        select = Select(column, self._where, order_by=self._order_by,
                        default_tables=self._cls_info.table, distinct=True)
        return self._store._connection.execute(select).get_one()[0]

    def one(self):
        select = Select(self._cls_info.columns, self._where,
                        default_tables=self._cls_info.table,
                        order_by=self._order_by, distinct=True)
        result = self._store._connection.execute(select)
        values = result.get_one()
        if values:
            return self._store._load_object(self._cls_info, result, values)
        return None

    def order_by(self, *args):
        return self.__class__(self._store, self._cls_info, self._where, args)

    def remove(self):
        self._store._connection.execute(Delete(self._where,
                                               self._cls_info.table),
                                        noresult=True)

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

    def set(self, *args, **kwargs):
        if not (args or kwargs):
            return

        changes = {}
        cls = self._cls_info.cls

        for expr in args:
            if (not isinstance(expr, Eq) or
                not isinstance(expr.expr1, Column) or
                not isinstance(expr.expr2, (Column, Variable))):
                raise SetError("Unsupported set expression: %r" % repr(expr))
            changes[expr.expr1] = expr.expr2

        for key, value in kwargs.items():
            column = getattr(cls, key)
            if value is None:
                changes[column] = None
            elif isinstance(value, Expr):
                if not isinstance(value, Column):
                    raise SetError("Unsupported set expression: %r" %
                                     repr(value))
                changes[column] = value
            else:
                changes[column] = column.variable_factory(value=value)

        expr = Update(changes, self._where, self._cls_info.table)
        self._store._connection.execute(expr, noresult=True)

        try:
            cached = self.cached()
        except CompileError:
            for obj in self._store._iter_cached():
                if isinstance(obj, cls):
                    self._store.reload(obj)
        else:
            changes = changes.items()
            for obj in cached:
                for column, value in changes:
                    variables = get_obj_info(obj).variables
                    if value is None:
                        pass
                    elif isinstance(value, Variable):
                        value = value.get()
                    else:
                        value = variables[value].get()
                    variables[column].set(value)
                    variables[column].checkpoint()

    def cached(self):
        if self._where is Undef:
            match = None
        else:
            match = compile_python(self._where)
            name_to_column = dict((c.name, c) for c in self._cls_info.columns)
            def get_column(name, name_to_column=name_to_column):
                return get_obj_info(obj).variables[name_to_column[name]].get()
        objects = []
        cls = self._cls_info.cls
        for obj in self._store._iter_cached():
            if isinstance(obj, cls) and (match is None or match(get_column)):
                objects.append(obj)
        return objects
