#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
from weakref import WeakValueDictionary, WeakKeyDictionary

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

    _result_set_factory = None

    def __init__(self, database):
        self._connection = database.connect()
        self._cache = WeakValueDictionary()
        self._ghosts = WeakKeyDictionary()
        self._dirty = set()
        self._order = {} # (info, info) = count

    @staticmethod
    def of(obj):
        try:
            return get_obj_info(obj).get("store")
        except AttributeError:
            return None

    def execute(self, statement, params=None, noresult=False):
        self.flush()
        return self._connection.execute(statement, params, noresult)

    def close(self):
        # XXX UNTESTED
        self._connection.close()

    def commit(self):
        self.flush()
        self._connection.commit()
        for obj_info in self._iter_ghosts():
            del obj_info["store"]
        for obj_info in self._iter_cached():
            obj_info.save()
        self._ghosts.clear()

    def rollback(self):
        infos = set()
        for obj_info in self._iter_dirty():
            infos.add(obj_info)
        for obj_info in self._iter_ghosts():
            infos.add(obj_info)
        for obj_info in self._iter_cached():
            infos.add(obj_info)

        for obj_info in infos:
            self._remove_from_cache(obj_info)

            obj_info.restore()

            if obj_info.get("store") is self:
                self._add_to_cache(obj_info)
                self._enable_change_notification(obj_info)

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

        obj_info = self._cache.get((cls, tuple(primary_vars)))
        if obj_info is not None:
            return obj_info.obj

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
        return self._result_set_factory(self, cls_info, where)

    def new(self, cls, *args, **kwargs):
        obj = cls(*args, **kwargs)
        self.add(obj)
        return obj

    def add(self, obj):
        obj_info = get_obj_info(obj)

        store = obj_info.get("store")
        if store is not None and store is not self:
            raise WrongStoreError("%s is part of another store" % repr(obj))

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
            elif not self._is_ghost(obj_info):
                return # It's fine already.
            else:
                self._set_alive(obj_info)

            obj_info["pending"] = PENDING_ADD
            self._set_dirty(obj_info)
            obj_info.event.emit("added")

    def remove(self, obj):
        obj_info = get_obj_info(obj)

        if obj_info.get("store") is not self:
            raise WrongStoreError("%s is not in this store" % repr(obj))

        pending = obj_info.get("pending")

        if pending is PENDING_REMOVE:
            pass
        elif pending is PENDING_ADD:
            del obj_info["pending"]
            self._set_ghost(obj_info)
            self._set_clean(obj_info)
        elif not self._is_ghost(obj_info):
            obj_info["pending"] = PENDING_REMOVE
            self._set_dirty(obj_info)

    def reload(self, obj):
        obj_info = get_obj_info(obj)
        cls_info = obj_info.cls_info
        if obj_info.get("store") is not self or self._is_ghost(obj_info):
            raise WrongStoreError("%s is not in this store" % repr(obj))
        if "primary_vars" not in obj_info:
            raise NotFlushedError("Can't reload an object if it was "
                                  "never flushed")
        where = compare_columns(cls_info.primary_key, obj_info["primary_vars"])
        select = Select(cls_info.columns, where,
                        default_tables=cls_info.table, limit=1)
        result = self._connection.execute(select)
        values = result.get_one()
        self._set_values(obj_info, cls_info.columns, result, values)
        obj_info.checkpoint()
        self._set_clean(obj_info)

    def add_flush_order(self, before, after):
        pair = (get_obj_info(before), get_obj_info(after))
        try:
            self._order[pair] += 1
        except KeyError:
            self._order[pair] = 1

    def remove_flush_order(self, before, after):
        pair = (get_obj_info(before), get_obj_info(after))
        try:
            self._order[pair] -= 1
        except KeyError:
            pass

    def flush(self):
        predecessors = {}
        for (before_info, after_info), n in self._order.iteritems():
            if n > 0:
                before_set = predecessors.get(after_info)
                if before_set is None:
                    predecessors[after_info] = set((before_info,))
                else:
                    before_set.add(before_info)

        while self._dirty:
            for obj_info in self._dirty:
                for before_info in predecessors.get(obj_info, ()):
                    if before_info in self._dirty:
                        break # A predecessor is still dirty.
                else:
                    break # Found an item without dirty predecessors.
            else:
                raise OrderLoopError("Can't flush due to ordering loop")
            self._dirty.discard(obj_info)
            self._flush_one(obj_info)

        self._order.clear()

    def _flush_one(self, obj_info):
        cls_info = obj_info.cls_info

        pending = obj_info.pop("pending", None)

        if pending is PENDING_REMOVE:
            expr = Delete(compare_columns(cls_info.primary_key,
                                          obj_info["primary_vars"]),
                          cls_info.table)
            self._connection.execute(expr, noresult=True)

            self._disable_change_notification(obj_info)
            self._set_ghost(obj_info)
            self._remove_from_cache(obj_info)

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

            self._fill_missing_values(obj_info, result)

            self._enable_change_notification(obj_info)
            self._set_alive(obj_info)
            self._add_to_cache(obj_info)

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

                self._add_to_cache(obj_info)

            obj_info.checkpoint()

        obj_info.event.emit("flushed")

    def _fill_missing_values(self, obj_info, result):
        cls_info = obj_info.cls_info

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
            obj_info = self._cache.get((cls_info.cls, tuple(primary_vars)))
            if obj_info is not None:
                return obj_info.obj
            obj = cls_info.cls.__new__(cls_info.cls)

        obj_info = get_obj_info(obj)
        obj_info["store"] = self

        self._set_values(obj_info, cls_info.columns, result, values)

        obj_info.save()

        self._add_to_cache(obj_info)
        self._enable_change_notification(obj_info)

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


    def _is_dirty(self, obj_info):
        return obj_info in self._dirty

    def _set_dirty(self, obj_info):
        self._dirty.add(obj_info)

    def _set_clean(self, obj_info):
        self._dirty.discard(obj_info)

    def _iter_dirty(self):
        return self._dirty


    def _is_ghost(self, obj_info):
        return obj_info in self._ghosts

    def _set_ghost(self, obj_info):
        self._ghosts[obj_info] = True

    def _set_alive(self, obj_info):
        self._ghosts.pop(obj_info, None)

    def _iter_ghosts(self):
        return self._ghosts.keys()


    def _add_to_cache(self, obj_info):
        # XXX WRITE A TEST EXPLORING THE PROBLEM.
        cls_info = obj_info.cls_info
        old_primary_vars = obj_info.get("primary_vars")
        if old_primary_vars is not None:
            self._cache.pop((cls_info.cls, old_primary_vars), None)
        new_primary_vars = tuple(variable.copy()
                                 for variable in obj_info.primary_vars)
        self._cache[cls_info.cls, new_primary_vars] = obj_info
        obj_info["primary_vars"] = new_primary_vars

    def _remove_from_cache(self, obj_info):
        primary_vars = obj_info.get("primary_vars")
        if primary_vars is not None:
            del self._cache[obj_info.cls_info.cls, primary_vars]
            del obj_info["primary_vars"]

    def _iter_cached(self):
        return self._cache.values()


    def _enable_change_notification(self, obj_info):
        obj_info.event.hook("changed", self._variable_changed)

    def _disable_change_notification(self, obj_info):
        obj_info.event.unhook("changed", self._variable_changed)

    def _variable_changed(self, obj_info, variable, old_value, new_value):
        if new_value is not Undef:
            self._dirty.add(obj_info)


class ResultSet(object):

    def __init__(self, store, cls_info, where, order_by=Undef, 
                 offset=Undef, limit=Undef):
        self._store = store
        self._cls_info = cls_info
        self._where = where
        self._order_by = order_by
        self._offset = offset
        self._limit = limit

    def __iter__(self):
        select = Select(self._cls_info.columns, self._where,
                        default_tables=self._cls_info.table,
                        order_by=self._order_by, distinct=True,
                        offset=self._offset, limit=self._limit)
        result = self._store._connection.execute(select)
        for values in result:
            yield self._store._load_object(self._cls_info, result, values)

    def __getitem__(self, fromto):
        # XXX FIXME TESTME: Add a test for order_by + offset + limit + boom!
        # XXX FIXME TESTME: Slicing a sliced resultset
        # XXX FIXME TESTME: Non-slice fromto
        if not isinstance(fromto, slice):
            raise IndexError("Can't index ResultSets with non-slices: %r"
                             % (fromto,))
        if fromto.step is not None:
            raise IndexError("Don't understand stepped slices: %r"
                             % (fromto.step,))
        if fromto.stop is not None:
            if fromto.start is not None:
                limit = fromto.stop - fromto.start
            else:
                limit = fromto.stop
        else:
            limit = Undef
        if fromto.start is not None:
            offset = fromto.start
        else:
            offset = Undef
        return self.__class__(self._store, self._cls_info, self._where,
                              self._order_by, offset, limit)

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
        return self.__class__(self._store, self._cls_info, self._where, args,
                              self._offset, self._limit)

    def remove(self):
        # XXX TODO STORM: Raise exceptions when removing a sliced resultset
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
            for obj_info in self._store._iter_cached():
                if isinstance(obj_info.obj, cls):
                    self._store.reload(obj_info.obj)
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
                return obj_info.variables[name_to_column[name]].get()
        objects = []
        cls = self._cls_info.cls
        for obj_info in self._store._iter_cached():
            if (obj_info.cls_info is self._cls_info and
                (match is None or match(get_column))):
                objects.append(obj_info.obj)
        return objects

    # FIXME Implement it meaningfully.
    first = one


    # TODO Implement last() with order_by + inverted logic of Asc/Desc.
    # TODO Add ResultSet().values(Tag.name) (or something)



Store._result_set_factory = ResultSet
