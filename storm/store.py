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

from storm.info import get_cls_info, get_obj_info, set_obj_info, get_info
from storm.variables import Variable, LazyValue
from storm.expr import (
    Expr, Select, Insert, Update, Delete, Column, JoinExpr, Count, Max, Min,
    Avg, Sum, Eq, And, Asc, Desc, compile_python, compare_columns, SQLRaw,
    Union, Except, Intersect, Alias)
from storm.exceptions import (
    WrongStoreError, NotFlushedError, OrderLoopError, UnorderedError,
    NotOneError, FeatureError, CompileError, LostObjectError, ClassInfoError)
from storm import Undef


__all__ = ["Store", "AutoReload", "EmptyResultSet"]

PENDING_ADD = 1
PENDING_REMOVE = 2


class Store(object):

    _result_set_factory = None

    def __init__(self, database):
        self._connection = database.connect()
        self._cache = WeakValueDictionary()
        self._dirty = {}
        self._order = {} # (info, info) = count

    @staticmethod
    def of(obj):
        try:
            return get_obj_info(obj).get("store")
        except (AttributeError, ClassInfoError):
            return None

    def execute(self, statement, params=None, noresult=False):
        self.flush()
        return self._connection.execute(statement, params, noresult)

    def close(self):
        self._connection.close()

    def commit(self):
        self.flush()
        self.invalidate()
        self._connection.commit()

    def rollback(self):
        for obj_info in self._dirty:
            pending = obj_info.pop("pending", None)
            if pending is PENDING_ADD:
                # Object never got in the cache, so being "in the store"
                # has no actual meaning for it.
                del obj_info["store"]
            elif pending is PENDING_REMOVE:
                # Object never got removed, so it's still in the cache,
                # and thus should continue to resolve from now on.
                self._enable_lazy_resolving(obj_info)
        self._dirty.clear()
        self.invalidate()
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

        obj_info = self._cache.get((cls_info.cls, tuple(primary_vars)))
        if obj_info is not None:
            if obj_info.get("invalidated"):
                try:
                    self._fill_missing_values(obj_info, primary_vars)
                except LostObjectError:
                    return None
            obj = obj_info.get_obj()
            if obj is None:
                return self._rebuild_deleted_object(obj_info)
            return obj

        where = compare_columns(cls_info.primary_key, primary_vars)

        select = Select(cls_info.columns, where,
                        default_tables=cls_info.table, limit=1)

        result = self._connection.execute(select)
        values = result.get_one()
        if values is None:
            return None
        return self._load_object(cls_info, result, values)

    def find(self, cls_spec, *args, **kwargs):
        self.flush()
        if type(cls_spec) is tuple:
            cls_spec_info = tuple(get_cls_info(cls) for cls in cls_spec)
            where = get_where_for_args(args, kwargs)
        else:
            cls_spec_info = get_cls_info(cls_spec)
            where = get_where_for_args(args, kwargs, cls_spec)
        return self._result_set_factory(self, cls_spec_info, where)

    def using(self, *tables):
        return self._table_set(self, tables)

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
            self._enable_lazy_resolving(obj_info)
            # obj_info.event.emit("added")
        elif store is None:
            obj_info["store"] = self
            obj_info["pending"] = PENDING_ADD
            self._set_dirty(obj_info)
            self._enable_lazy_resolving(obj_info)
            obj_info.event.emit("added")

    def remove(self, obj):
        obj_info = get_obj_info(obj)

        if obj_info.get("store") is not self:
            raise WrongStoreError("%s is not in this store" % repr(obj))

        pending = obj_info.get("pending")

        if pending is PENDING_REMOVE:
            pass
        elif pending is PENDING_ADD:
            del obj_info["store"]
            del obj_info["pending"]
            self._set_clean(obj_info)
            self._disable_lazy_resolving(obj_info)
        else:
            obj_info["pending"] = PENDING_REMOVE
            self._set_dirty(obj_info)
            self._disable_lazy_resolving(obj_info)

    def reload(self, obj):
        obj_info = get_obj_info(obj)
        cls_info = obj_info.cls_info
        if obj_info.get("store") is not self:
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

    def autoreload(self, obj=None):
        self._mark_autoreload(obj, False)

    def invalidate(self, obj=None):
        self._mark_autoreload(obj, True)

    def _mark_autoreload(self, obj=None, invalidate=False):
        if obj is None:
            obj_infos = self._iter_cached()
        else:
            obj_infos = (get_obj_info(obj),)
        for obj_info in obj_infos:
            cls_info = obj_info.cls_info
            for column in cls_info.columns:
                if id(column) not in cls_info.primary_key_idx:
                    obj_info.variables[column].set(AutoReload)
            if invalidate:
                # Marking an object with 'invalidated' means that we're
                # not sure if the object is actually in the database
                # anymore, so before the object is returned from the cache
                # (e.g. by a get()), the database should be queried to see
                # if the object's still there.
                obj_info["invalidated"] = True
                self._run_hook(obj_info, "__storm_invalidate__")

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
        for obj_info in self._iter_cached():
            obj_info.event.emit("flush")

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
            self._dirty.pop(obj_info, None)
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

            # We're sure the cache is valid at this point.
            obj_info.pop("invalidated", None)

            self._disable_change_notification(obj_info)
            self._remove_from_cache(obj_info)
            del obj_info["store"]

        elif pending is PENDING_ADD:
            columns = []
            variables = []

            for column in cls_info.columns:
                variable = obj_info.variables[column]
                if variable.is_defined():
                    columns.append(column)
                    variables.append(variable)
                else:
                    lazy_value = variable.get_lazy()
                    if isinstance(lazy_value, Expr):
                        columns.append(column)
                        variables.append(lazy_value)

            expr = Insert(columns, variables, cls_info.table)

            result = self._connection.execute(expr)

            # We're sure the cache is valid at this point. We just added
            # the object.
            obj_info.pop("invalidated", None)

            self._fill_missing_values(obj_info, obj_info.primary_vars, result,
                                      checkpoint=False)

            self._enable_change_notification(obj_info)
            self._add_to_cache(obj_info)

            obj_info.checkpoint()

        else:

            cached_primary_vars = obj_info["primary_vars"]
            primary_key_idx = cls_info.primary_key_idx

            changes = {}
            for column in cls_info.columns:
                variable = obj_info.variables[column]
                if variable.has_changed():
                    if variable.is_defined():
                        changes[column] = variable
                    else:
                        lazy_value = variable.get_lazy()
                        if isinstance(lazy_value, Expr):
                            changes[column] = lazy_value

            if changes:
                expr = Update(changes,
                              compare_columns(cls_info.primary_key,
                                              cached_primary_vars),
                              cls_info.table)
                self._connection.execute(expr, noresult=True)

                # We're sure the cache is valid at this point. We've
                # just updated the object.
                obj_info.pop("invalidated", None)

                self._fill_missing_values(obj_info, obj_info.primary_vars,
                                          checkpoint=False)

                self._add_to_cache(obj_info)


            obj_info.checkpoint()

        self._run_hook(obj_info, "__storm_flushed__")

        obj_info.event.emit("flushed")

    def _fill_missing_values(self, obj_info, primary_vars, result=None,
                             checkpoint=True):
        # XXX If there are no values which are part of the primary
        #     key missing, they might be set to a lazy value for
        #     on-demand reloading.
        cls_info = obj_info.cls_info

        cached_primary_vars = obj_info.get("primary_vars")
        primary_key_idx = cls_info.primary_key_idx

        missing_columns = []
        for column in cls_info.columns:
            variable = obj_info.variables[column]
            if not variable.is_defined():
                idx = primary_key_idx.get(id(column))
                if (idx is not None and cached_primary_vars is not None
                    and variable.get_lazy()):
                    # For auto-reloading a primary key, just get the
                    # value out of the cache.
                    variable.set(cached_primary_vars[idx].get())
                else:
                    missing_columns.append(column)

        if missing_columns:
            primary_key = cls_info.primary_key

            for variable in primary_vars:
                if not variable.is_defined():
                    if result is None:
                        raise RuntimeError("Can't find missing primary values "
                                           "without a meaningful result")
                    where = result.get_insert_identity(primary_key,
                                                       primary_vars)
                    break
            else:
                where = compare_columns(primary_key, primary_vars)

            # This procedure will also validate the cache.
            result = self._connection.execute(Select(missing_columns, where))
            self._set_values(obj_info, missing_columns,
                             result, result.get_one())

            obj_info.pop("invalidated", None)

            if checkpoint:
                for column in missing_columns:
                    obj_info.variables[column].checkpoint()

        elif obj_info.get("invalidated"):
            # In case of no explicit missing values, enforce cache validation.
            # It might happen that the primary key was autoreloaded and
            # restored from the cache.
            where = compare_columns(cls_info.primary_key, primary_vars)
            result = self._connection.execute(Select(SQLRaw("1"), where))
            if not result.get_one():
                raise LostObjectError("Object is not in the database anymore")

            obj_info.pop("invalidated", None)


    def _load_objects(self, cls_spec_info, result, values):
        if type(cls_spec_info) is not tuple:
            return self._load_object(cls_spec_info, result, values)
        else:
            objects = []
            values_start = values_end = 0
            for cls_info in cls_spec_info:
                values_end += len(cls_info.columns)
                obj = self._load_object(cls_info, result,
                                        values[values_start:values_end])
                objects.append(obj)
                values_start = values_end
            return tuple(objects)

    def _load_object(self, cls_info, result, values):
        # _set_values() need the cls_info columns for the class of the
        # actual object, not from a possible wrapper (e.g. an alias).
        cls = cls_info.cls
        cls_info = get_cls_info(cls)

        # Prepare cache key.
        primary_vars = []
        columns = cls_info.columns
        is_null = True
        for i in cls_info.primary_key_pos:
            value = values[i]
            if value is not None:
                is_null = False
            variable = columns[i].variable_factory(value=value,
                                                   from_db=True)
            primary_vars.append(variable)

        if is_null:
            # We've got a row full of NULLs, so consider that the object
            # wasn't found.  This is useful for joins, where unexistent
            # rows are reprsented like that.
            return None

        # Lookup cache.
        obj_info = self._cache.get((cls, tuple(primary_vars)))

        if obj_info is not None:
            # Found object in cache, and it must be valid since the
            # primary key was extracted from result values.
            obj_info.pop("invalidated", None)

            # We're not sure if the obj is still in memory at this point.
            obj = obj_info.get_obj()
            if obj is not None:
                # Great, the object is still in memory. Nothing to do.
                return obj
            else:
                # Object died while obj_info was still in memory.
                # Rebuild the object and maintain the obj_info.
                return self._rebuild_deleted_object(obj_info)
        
        else:
            # Nothing found in the cache. Build everything from the ground.
            obj = cls.__new__(cls)

            obj_info = get_obj_info(obj)
            obj_info["store"] = self

            self._set_values(obj_info, cls_info.columns, result, values)

            obj_info.checkpoint()

            self._add_to_cache(obj_info)
            self._enable_change_notification(obj_info)
            self._enable_lazy_resolving(obj_info)

            self._run_hook(obj_info, "__storm_loaded__")
            return obj

    def _rebuild_deleted_object(self, obj_info):
        """Rebuild a deleted object and maintain the obj_info."""
        cls = obj_info.cls_info.cls
        obj = cls.__new__(cls)
        obj_info.set_obj(obj)
        set_obj_info(obj, obj_info)
        self._run_hook(obj_info, "__storm_loaded__")
        return obj

    @staticmethod
    def _run_hook(obj_info, hook_name):
        func = getattr(obj_info.get_obj(), hook_name, None)
        if func is not None:
            func()

    def _set_values(self, obj_info, columns, result, values):
        if values is None:
            raise LostObjectError("Can't obtain values from the database "
                                  "(object got removed?)")
        for column, value in zip(columns, values):
            if value is None:
                obj_info.variables[column].set(value, from_db=True)
            else:
                result.set_variable(obj_info.variables[column], value)


    def _is_dirty(self, obj_info):
        return obj_info in self._dirty

    def _set_dirty(self, obj_info):
        self._dirty[obj_info] = obj_info.get_obj()

    def _set_clean(self, obj_info):
        self._dirty.pop(obj_info, None)

    def _iter_dirty(self):
        return self._dirty


    def _add_to_cache(self, obj_info):
        """Add an object to the cache, keyed on primary key variables.

        When an object is added to the cache, the key is built from
        a copy of the current variables that are part of the primary
        key.  This means that, when an object is retrieved from the
        database, these values may be used to get the cached object
        which is already in memory, even if it requested the primary
        key value to be changed.  For that reason, when changes to
        the primary key are flushed, the cache key should also be
        updated to reflect these changes.
        """
        cls_info = obj_info.cls_info
        old_primary_vars = obj_info.get("primary_vars")
        if old_primary_vars is not None:
            self._cache.pop((cls_info.cls, old_primary_vars), None)
        new_primary_vars = tuple(variable.copy()
                                 for variable in obj_info.primary_vars)
        self._cache[cls_info.cls, new_primary_vars] = obj_info
        obj_info["primary_vars"] = new_primary_vars

    def _remove_from_cache(self, obj_info):
        """Remove an object from the cache.

        This method is only called for objects that were explicitly
        deleted and flushed.  Objects that are unused will get removed
        from the cache dictionary automatically by their weakref callbacks.
        """
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

    def _variable_changed(self, obj_info, variable,
                          old_value, new_value, fromdb):
        # The fromdb check makes sure that values coming from the
        # database don't mark the object as dirty again.
        # XXX The fromdb check is untested. How to test it?
        if not fromdb and (new_value is not Undef and
                           new_value is not AutoReload):
            self._set_dirty(obj_info)


    def _enable_lazy_resolving(self, obj_info):
        obj_info.event.hook("resolve-lazy-value", self._resolve_lazy_value)

    def _disable_lazy_resolving(self, obj_info):
        obj_info.event.unhook("resolve-lazy-value", self._resolve_lazy_value)

    def _resolve_lazy_value(self, obj_info, variable, lazy_value):
        if lazy_value is AutoReload:
            cached_primary_vars = obj_info.get("primary_vars")
            if cached_primary_vars is None:
                # XXX See the comment on self.flush() below.
                self.flush()
            else:
                idx = obj_info.cls_info.primary_key_idx.get(id(variable.column))
                if idx is not None:
                    # No need to touch the database if auto-reloading
                    # a primary key variable.
                    variable.set(cached_primary_vars[idx].get())
                else:
                    self._fill_missing_values(obj_info, cached_primary_vars)
        else:
            # XXX This will do it for now, but it should really flush
            #     just this single object and ones that it depends on.
            #     _flush_one() doesn't consider dependencies, so it may
            #     not be used directly.
            self.flush()


class ResultSet(object):

    def __init__(self, store, cls_spec_info,
                 where=Undef, tables=Undef, select=Undef):
        self._store = store
        self._cls_spec_info = cls_spec_info
        self._where = where
        self._tables = tables
        self._select = select
        self._order_by = getattr(cls_spec_info, "default_order", Undef)
        self._offset = Undef
        self._limit = Undef
        self._distinct = False

    def copy(self):
        result_set = object.__new__(self.__class__)
        result_set.__dict__.update(self.__dict__)
        return result_set

    def config(self, distinct=None, offset=None, limit=None):
        if distinct is not None:
            self._distinct = distinct
        if offset is not None:
            self._offset = offset
        if limit is not None:
            self._limit = limit
        return self

    def _get_select(self):
        if self._select is not Undef:
            if self._order_by is not Undef:
                self._select.order_by = self._order_by
            if self._limit is not Undef: # XXX UNTESTED!
                self._select.limit = self._limit
            if self._offset is not Undef: # XXX UNTESTED!
                self._select.offset = self._offset
            return self._select
        if type(self._cls_spec_info) is tuple:
            columns = []
            default_tables = []
            for cls_info in self._cls_spec_info:
                columns.append(cls_info.columns)
                default_tables.append(cls_info.table)
        else:
            columns = self._cls_spec_info.columns
            default_tables = self._cls_spec_info.table
        return Select(columns, self._where, self._tables, default_tables,
                      self._order_by, offset=self._offset, limit=self._limit,
                      distinct=self._distinct)

    def __iter__(self):
        result = self._store._connection.execute(self._get_select())
        for values in result:
            yield self._store._load_objects(self._cls_spec_info,
                                            result, values)

    def __getitem__(self, index):
        if isinstance(index, (int, long)):
            if index == 0:
                result_set = self
            else:
                if self._offset is not Undef:
                    index += self._offset
                result_set = self.copy()
                result_set.config(offset=index, limit=1)
            obj = result_set.any()
            if obj is None:
                raise IndexError("Index out of range")
            return obj

        if not isinstance(index, slice):
            raise IndexError("Can't index ResultSets with %r" % (index,))
        if index.step is not None:
            raise IndexError("Stepped slices not yet supported: %r"
                             % (index.step,))

        offset = self._offset
        limit = self._limit

        if index.start is not None:
            if offset is Undef:
                offset = index.start
            else:
                offset += index.start
            if limit is not Undef:
                limit = max(0, limit - index.start)

        if index.stop is not None:
            if index.start is None:
                new_limit = index.stop
            else:
                new_limit = index.stop - index.start
            if limit is Undef or limit > new_limit:
                limit = new_limit

        return self.copy().config(offset=offset, limit=limit)

    def any(self):
        """Return a single item from the result set.

        See also one(), first(), and last().
        """
        select = self._get_select()
        select.limit = 1
        result = self._store._connection.execute(select)
        values = result.get_one()
        if values:
            return self._store._load_objects(self._cls_spec_info,
                                             result, values)
        return None

    def first(self):
        """Return the first item from an ordered result set.

        Will raise UnorderedError if the result set isn't ordered.

        See also last(), one(), and any().
        """
        if self._order_by is Undef:
            raise UnorderedError("Can't use first() on unordered result set")
        return self.any()

    def last(self):
        """Return the last item from an ordered result set.

        Will raise UnorderedError if the result set isn't ordered.

        See also first(), one(), and any().
        """
        if self._order_by is Undef:
            raise UnorderedError("Can't use last() on unordered result set")
        if self._limit is not Undef:
            raise FeatureError("Can't use last() with a slice "
                               "of defined stop index")
        select = self._get_select()
        select.offset = Undef
        select.limit = 1
        select.order_by = []
        for expr in self._order_by:
            if isinstance(expr, Desc):
                select.order_by.append(expr.expr)
            elif isinstance(expr, Asc):
                select.order_by.append(Desc(expr.expr))
            else:
                select.order_by.append(Desc(expr))
        result = self._store._connection.execute(select)
        values = result.get_one()
        if values:
            return self._store._load_objects(self._cls_spec_info,
                                             result, values)
        return None

    def one(self):
        """Return one item from a result set containing at most one item.

        Will raise NotOneError if the result set contains more than one item.

        See also first(), one(), and any().
        """
        select = self._get_select()
        # limit could be 1 due to slicing, for instance.
        if select.limit is not Undef and select.limit > 2:
            select.limit = 2
        result = self._store._connection.execute(select)
        values = result.get_one()
        if result.get_one():
            raise NotOneError("one() used with more than one result available")
        if values:
            return self._store._load_objects(self._cls_spec_info,
                                             result, values)
        return None

    def order_by(self, *args):
        if self._offset is not Undef or self._limit is not Undef:
            raise FeatureError("Can't reorder a sliced result set")
        self._order_by = args or Undef
        return self

    def remove(self):
        if self._offset is not Undef or self._limit is not Undef:
            raise FeatureError("Can't remove a sliced result set")
        if type(self._cls_spec_info) is tuple:
            raise FeatureError("Removing not yet supported with tuple finds")
        if self._select is not Undef:
            raise FeatureError("Removing isn't supportted with "
                               "set expressions (unions, etc)")
        self._store._connection.execute(Delete(self._where,
                                               self._cls_spec_info.table),
                                        noresult=True)

    def _aggregate(self, expr, column=None):
        if type(self._cls_spec_info) is tuple:
            default_tables = [cls_info.table
                              for cls_info in self._cls_spec_info]
        else:
            default_tables = self._cls_spec_info.table
        if self._select is Undef:
            select = Select(expr, self._where, self._tables, default_tables)
        else:
            select = Select(expr, tables=Alias(self._select))
        result = self._store._connection.execute(select)
        value = result.get_one()[0]
        if column is None:
            return value
        else:
            variable = column.variable_factory()
            result.set_variable(variable, value)
            return variable.get()

    def count(self, column=Undef, distinct=False):
        return int(self._aggregate(Count(column, distinct)))

    def max(self, column):
        return self._aggregate(Max(column), column)

    def min(self, column):
        return self._aggregate(Min(column), column)

    def avg(self, column):
        value = self._aggregate(Avg(column))
        if value is None:
            return value
        return float(value)

    def sum(self, column):
        return self._aggregate(Sum(column), column)

    def values(self, *columns):
        if not columns:
            raise FeatureError("values() takes at least one column "
                               "as argument")
        select = self._get_select()
        select.columns = columns
        result = self._store._connection.execute(select)
        if len(columns) == 1:
            variable = columns[0].variable_factory()
            for values in result:
                result.set_variable(variable, values[0])
                yield variable.get()
        else:
            variables = [column.variable_factory() for column in columns]
            for values in result:
                for variable, value in zip(variables, values):
                    result.set_variable(variable, value)
                yield tuple(variable.get() for variable in variables)

    def set(self, *args, **kwargs):
        if type(self._cls_spec_info) is tuple:
            raise FeatureError("Setting isn't supportted with tuple finds")
        if self._select is not Undef:
            raise FeatureError("Setting isn't supportted with "
                               "set expressions (unions, etc)")

        if not (args or kwargs):
            return

        changes = {}
        cls = self._cls_spec_info.cls

        for expr in args:
            if (not isinstance(expr, Eq) or
                not isinstance(expr.expr1, Column) or
                not isinstance(expr.expr2, (Column, Variable))):
                raise FeatureError("Unsupported set expression: %r" %
                                   repr(expr))
            changes[expr.expr1] = expr.expr2

        for key, value in kwargs.items():
            column = getattr(cls, key)
            if value is None:
                changes[column] = None
            elif isinstance(value, Expr):
                if not isinstance(value, Column):
                    raise FeatureError("Unsupported set expression: %r" %
                                       repr(value))
                changes[column] = value
            else:
                changes[column] = column.variable_factory(value=value)

        expr = Update(changes, self._where, self._cls_spec_info.table)
        self._store._connection.execute(expr, noresult=True)

        try:
            cached = self.cached()
        except CompileError:
            for obj_info in self._store._iter_cached():
                obj = obj_info.get_obj()
                if obj is not None and isinstance(obj, cls):
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
        if type(self._cls_spec_info) is tuple:
            raise FeatureError("Cached finds not supported with tuples")
        if self._tables is not Undef:
            raise FeatureError("Cached finds not supported with custom tables")
        if self._where is Undef:
            match = None
        else:
            match = compile_python(self._where)
            name_to_column = dict((column.name, column)
                                  for column in self._cls_spec_info.columns)
            def get_column(name, name_to_column=name_to_column):
                return obj_info.variables[name_to_column[name]].get()
        objects = []
        cls = self._cls_spec_info.cls
        for obj_info in self._store._iter_cached():
            if (obj_info.cls_info is self._cls_spec_info and
                (match is None or match(get_column))):
                obj = obj_info.get_obj()
                if obj is not None:
                    objects.append(obj)
        return objects

    def _set_expr(self, expr_cls, other, all=False):
        if self._cls_spec_info != other._cls_spec_info:
            raise FeatureError("Incompatible results for set operation")

        expr = expr_cls(self._get_select(), other._get_select(), all=all)
        return ResultSet(self._store, self._cls_spec_info, select=expr)

    def union(self, other, all=False):
        if isinstance(other, EmptyResultSet):
            return self
        return self._set_expr(Union, other, all)

    def difference(self, other, all=False):
        if isinstance(other, EmptyResultSet):
            return self
        return self._set_expr(Except, other, all)

    def intersection(self, other, all=False):
        if isinstance(other, EmptyResultSet):
            return other
        return self._set_expr(Intersect, other, all)


class EmptyResultSet(object):

    def __init__(self, ordered=False):
        self._order_by = ordered

    def _get_select(self):
        return Select(SQLRaw("1"), SQLRaw("1 = 2"))

    def copy(self):
        result = EmptyResultSet(self._order_by)
        return result

    def config(self, distinct=None, offset=None, limit=None):
        pass

    def __iter__(self):
        return
        yield None

    def __getitem__(self, index):
        return self.copy()

    def any(self):
        return None

    def first(self):
        if self._order_by:
            return None
        raise UnorderedError("Can't use first() on unordered result set")

    def last(self):
        if self._order_by:
            return None
        raise UnorderedError("Can't use last() on unordered result set")

    def one(self):
        return None

    def order_by(self, *args):
        self._order_by = True
        return self

    def remove(self):
        pass

    def count(self, column=Undef, distinct=False):
        return 0

    def max(self, column):
        return None

    def min(self, column):
        return None

    def avg(self, column):
        return None

    def sum(self, column):
        return None

    def values(self, *columns):
        if not columns:
            raise FeatureError("values() takes at least one column "
                               "as argument")
        return
        yield None

    def set(self, *args, **kwargs):
        pass

    def cached(self):
        return []

    def union(self, other):
        if isinstance(other, EmptyResultSet):
            return self
        return other.union(self)

    def difference(self, other):
        return self

    def intersection(self, other):
        return self


class TableSet(object):
    
    def __init__(self, store, tables):
        self._store = store
        self._tables = tables

    def find(self, cls_spec, *args, **kwargs):
        self._store.flush()
        if type(cls_spec) is tuple:
            cls_spec_info = tuple(get_cls_info(cls) for cls in cls_spec)
            where = get_where_for_args(args, kwargs)
        else:
            cls_spec_info = get_cls_info(cls_spec)
            where = get_where_for_args(args, kwargs, cls_spec)
        return self._store._result_set_factory(self._store, cls_spec_info,
                                               where, self._tables)


Store._result_set_factory = ResultSet
Store._table_set = TableSet


def get_where_for_args(args, kwargs, cls=None):
    equals = list(args)
    if kwargs:
        if cls is None:
            raise FeatureError("Can't determine class that keyword "
                               "arguments are associated with")
        for key, value in kwargs.items():
            equals.append(getattr(cls, key) == value)
    if equals:
        return And(*equals)
    return Undef


class AutoReload(LazyValue):
    pass

AutoReload = AutoReload()
