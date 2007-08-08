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

"""The Store interface to a database.

This module contains the highest-level ORM interface in Storm.
"""


from weakref import WeakValueDictionary

from storm.info import get_cls_info, get_obj_info, set_obj_info
from storm.variables import Variable, LazyValue
from storm.expr import (
    Expr, Select, Insert, Update, Delete, Column, Count, Max, Min,
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
    """The Storm Store.

    This is the highest-level interface to a database. It manages
    transactions with L{commit} and L{rollback}, caching, high-level
    querying with L{find}, and more.

    Note that Store objects are not threadsafe. You should create one
    Store per thread in your application, passing them the same
    backend L{Database<storm.store.Database>} object.
    """

    _result_set_factory = None

    def __init__(self, database):
        """
        @param database: The L{storm.database.Database} instance to use.
        """
        self._connection = database.connect()
        self._cache = WeakValueDictionary()
        self._dirty = {}
        self._order = {} # (info, info) = count

    @staticmethod
    def of(obj):
        """Get the Store that the object is associated with.

        If the given object has not yet been associated with a store,
        return None.
        """
        try:
            return get_obj_info(obj).get("store")
        except (AttributeError, ClassInfoError):
            return None

    def execute(self, statement, params=None, noresult=False):
        """Execute a basic query.

        This is just like L{storm.database.Database.execute}, except
        that a flush is performed first.
        """
        self.flush()
        return self._connection.execute(statement, params, noresult)

    def close(self):
        """Close the connection."""
        self._connection.close()

    def commit(self):
        """Commit all changes to the database.

        This invalidates the cache, so all live objects will have data
        reloaded next time they are touched.
        """
        self.flush()
        self.invalidate()
        self._connection.commit()

    def rollback(self):
        """Roll back all outstanding changes, reverting to database state."""
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
        """Get object of type cls with the given primary key from the database.

        If the object is cached the database won't be touched.

        @param cls: Class of the object to be retrieved.
        @param key: Primary key of object. May be a tuple for composed keys.
        """

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
            return self._get_object(obj_info)

        where = compare_columns(cls_info.primary_key, primary_vars)

        select = Select(cls_info.columns, where,
                        default_tables=cls_info.table, limit=1)

        result = self._connection.execute(select)
        values = result.get_one()
        if values is None:
            return None
        return self._load_object(cls_info, result, values)

    def find(self, cls_spec, *args, **kwargs):
        """Perform a query.

        Some examples::

            store.find(Person, Person.name == u"Joe") --> all Persons named Joe
            store.find(Person, name=u"Joe") --> same
            store.find((Company, Person), Person.company_id == Company.id) -->
                iterator of tuples of Company and Person instances which are
                associated via the company_id -> Company relation.

        @param cls_spec: The class or tuple of classes whose
            associated tables will be queried.
        @param args: Instances of L{Expr}.
        @param kwargs: Mapping of simple column names to values or
            expressions to query for.

        @return: A L{ResultSet} of instances C{cls_spec}. If C{cls_spec}
            was a tuple, then an iterator of tuples of such instances.
        """
        self.flush()
        if type(cls_spec) is tuple:
            cls_spec_info = tuple(get_cls_info(cls) for cls in cls_spec)
            where = get_where_for_args(args, kwargs)
        else:
            cls_spec_info = get_cls_info(cls_spec)
            where = get_where_for_args(args, kwargs, cls_spec)
        return self._result_set_factory(self, cls_spec_info, where)

    def using(self, *tables):
        """Specify tables to use explicitly.

        The L{find} method generally does a good job at figuring out
        the tables to query by itself, but in some cases it's useful
        to specify them explicitly.

        This is most often necessary when an explicit SQL join is
        required. An example follows::

            join = LeftJoin(Person, Person.id == Company.person_id)
            print list(store.using(Company, join).find((Company, Person)))

        The previous code snippet will produce an SQL statement
        somewhat similar to this, depending on your backend::

            SELECT company.id, employee.company_id, employee.id
            FROM company
            LEFT JOIN employee ON employee.company_id = company.id;

        @return: A L{TableSet}, which has a C{find} method similar to
            L{Store.find}.
        """
        return self._table_set(self, tables)

    def add(self, obj):
        """Add the given object to the store.

        The object will be inserted into the database if it has not
        yet been added.

        The C{added} event will be fired on the object info's event system.
        """
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

        return obj

    def remove(self, obj):
        """Remove the given object from the store.

        The associated row will be deleted from the database.
        """
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
        """Reload the given object.

        The object will immediately have all of its data reset from
        the database. Any pending changes will be thrown away.
        """
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
        """Set an object or all objects to be reloaded automatically on access.

        When a database-backed attribute of one of the objects is
        accessed, the object will be reloaded entirely from the database.

        @param obj: If passed, only mark the given object for
            autoreload. Otherwise, all cached objects will be marked for
            autoreload.
        """
        self._mark_autoreload(obj, False)

    def invalidate(self, obj=None):
        """Set an object or all objects to be invalidated.

        This prevents Storm from returning the cached object without
        first verifying that the object is still available in the
        database.

        This should almost never be called by application code; it is
        only necessary if it is possible that an object has
        disappeared through some mechanism that Storm was unable to
        detect, like direct SQL statements within the current
        transaction that bypassed the ORM layer. The Store
        automatically invalidates all cached objects on transaction
        boundaries.
        """
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
        # We want to make sure we've marked all objects as invalidated and set
        # up their autoreloads before calling the invalidated hook on *any* of
        # them, because an invalidated hook might use other objects and we want
        # to prevent invalidation ordering issues.
        if invalidate:
            for obj_info in obj_infos:
                self._run_hook(obj_info, "__storm_invalidated__")

    def add_flush_order(self, before, after):
        """Explicitly specify the order of flushing two objects.

        When the next database flush occurs, the order of data
        modification statements will be ensured.

        @param before: The object to flush first.
        @param after: The object to flush after C{before}.
        """
        pair = (get_obj_info(before), get_obj_info(after))
        try:
            self._order[pair] += 1
        except KeyError:
            self._order[pair] = 1

    def remove_flush_order(self, before, after):
        """Cancel an explicit flush order specified with L{add_flush_order}.

        @param before: The C{before} object previously specified in a
            call to L{add_flush_order}.
        @param after: The C{after} object previously specified in a
            call to L{add_flush_order}.
        """
        pair = (get_obj_info(before), get_obj_info(after))
        try:
            self._order[pair] -= 1
        except KeyError:
            pass

    def flush(self):
        """Flush all dirty objects in cache to database.

        This method will first call the __storm_pre_flush__ hook of all dirty
        objects.  If more objects become dirty as a result of executing code
        in the hooks, the hook is also called on them.  The hook is only
        called once for each object.

        It will then flush each dirty object to the database, that is,
        execute the SQL code to insert/delete/update them.  After each
        object is flushed, the hook __storm_flushed__ is called on it,
        and if changes are made to the object it will get back to the
        dirty list, and be flushed again.

        Note that Storm will flush objects for you automatically, so you'll
        only need to call this method explicitly in very rare cases where
        normal flushing times are insufficient, such as when you want to
        make sure a database trigger gets run at a particular time.
        """
        for obj_info in self._iter_cached():
            obj_info.event.emit("flush")

        # The _dirty list may change under us while we're running
        # the flush hooks, so we cannot just simply loop over it
        # once.  To prevent infinite looping we keep track of which
        # objects we've called the hook for using a `flushing` dict.
        flushing = {}
        while self._dirty:
            (obj_info, obj) = self._dirty.popitem()
            if obj_info not in flushing:
                flushing[obj_info] = obj
                self._run_hook(obj_info, "__storm_pre_flush__")
        self._dirty = flushing

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

            # Give a chance to the backend to process primary variables.
            self._connection.preset_primary_key(cls_info.primary_key,
                                                obj_info.primary_vars)

            changes = self._get_changes_map(obj_info, True)

            expr = Insert(changes, cls_info.table,
                          primary_columns=cls_info.primary_key,
                          primary_variables=obj_info.primary_vars)

            result = self._connection.execute(expr)

            # We're sure the cache is valid at this point. We just added
            # the object.
            obj_info.pop("invalidated", None)

            self._fill_missing_values(obj_info, obj_info.primary_vars, result,
                                      checkpoint=False, replace_lazy=True)

            self._enable_change_notification(obj_info)
            self._add_to_cache(obj_info)

            obj_info.checkpoint()

        else:

            cached_primary_vars = obj_info["primary_vars"]
            primary_key_idx = cls_info.primary_key_idx

            changes = self._get_changes_map(obj_info)

            if changes:
                expr = Update(changes,
                              compare_columns(cls_info.primary_key,
                                              cached_primary_vars),
                              cls_info.table)
                self._connection.execute(expr, noresult=True)

                self._fill_missing_values(obj_info, obj_info.primary_vars,
                                          checkpoint=False, replace_lazy=True)

                self._add_to_cache(obj_info)


            obj_info.checkpoint()

        self._run_hook(obj_info, "__storm_flushed__")

        obj_info.event.emit("flushed")

    def _get_changes_map(self, obj_info, adding=False):
        """Return a {column: variable} dictionary suitable for inserts/updates.

        @param obj_info: ObjectInfo to inspect for changes.
        @param adding: If true, any defined variables will be considered
                       a change and included in the returned map.
        """
        cls_info = obj_info.cls_info
        changes = {}
        select_variables = []
        for column in cls_info.columns:
            variable = obj_info.variables[column]
            if adding or variable.has_changed():
                if variable.is_defined():
                    changes[column] = variable
                else:
                    lazy_value = variable.get_lazy()
                    if isinstance(lazy_value, Expr):
                        if id(column) in cls_info.primary_key_idx:
                            select_variables.append(variable) # See below.
                            changes[column] = variable
                        else:
                            changes[column] = lazy_value

        # If we have any expressions in the primary variables, we
        # have to resolve them now so that we have the identity of
        # the inserted object available later.
        if select_variables:
            resolve_expr = Select([variable.get_lazy()
                                   for variable in select_variables])
            result = self._connection.execute(resolve_expr)
            for variable, value in zip(select_variables, result.get_one()):
                result.set_variable(variable, value)

        return changes

    def _fill_missing_values(self, obj_info, primary_vars, result=None,
                             checkpoint=True, replace_lazy=False):
        """Query retrieve from the database any missing values in obj_info.

        This method will verify which values are unset in obj_info,
        and will retrieve them from the database to set them.

        @param obj_info: ObjectInfo to have its values filled.
        @param primary_vars: Variables composing the primary key with
            up-to-date values (cached variables may be out-of-date when
            this method is called).
        @param result: If some value in the set of primary variables
            isn't defined, it must be retrieved from the database
            using database-dependent logic, which is provided by the
            backend in the result of the query which inserted the object.
        @param checkpoint: If true, variables will be checkpointed so that
            they are aware that the value just set is the value currently
            in the database.  Generally this will be false only when
            checkpointing is being done at the calling place.
        @param replace_lazy: If true, lazy values are handled as if they
            were missing, and are replaced by values returned from the
            database.
        """
        cls_info = obj_info.cls_info

        cached_primary_vars = obj_info.get("primary_vars")
        primary_key_idx = cls_info.primary_key_idx

        missing_columns = []
        for column in cls_info.columns:
            variable = obj_info.variables[column]
            if not variable.is_defined():
                idx = primary_key_idx.get(id(column))
                lazy_value = variable.get_lazy()
                if (idx is not None and cached_primary_vars is not None
                    and lazy_value is AutoReload):
                    # For auto-reloading a primary key, just get the
                    # value out of the cache.
                    variable.set(cached_primary_vars[idx].get())
                elif (replace_lazy or lazy_value is None or
                      lazy_value is AutoReload):
                    missing_columns.append(column)

        if missing_columns:
            primary_key = cls_info.primary_key

            for variable in primary_vars:
                if not variable.is_defined():
                    # XXX Think about the case where the primary key is set
                    #     to a lazy value which isn't AutoReload.
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
            variable = columns[i].variable_factory(value=value, from_db=True)
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

            # We're not sure if the obj is still in memory at this
            # point.  This will rebuild it if needed.
            obj = self._get_object(obj_info)
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

    def _get_object(self, obj_info):
        """Return object for obj_info, rebuilding it if it's dead."""
        obj = obj_info.get_obj()
        if obj is None:
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
        if not fromdb:
            if new_value is not Undef and new_value is not AutoReload:
                if obj_info.get("invalidated"):
                    # This might be a previously cached object being
                    # updated.  Let's validate it now to improve debugging.
                    # This will raise LostObjectError if the object is gone.
                    self._fill_missing_values(obj_info,
                                              obj_info["primary_vars"])
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
    """The representation of the results of a query.

    Note that having an instance of this class does not indicate that
    a database query has necessarily been made. Database queries are
    put off until absolutely necessary.

    Generally these should not be constructed directly, but instead
    retrieved from calls to L{Store.find}.
    """
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
        """Return a copy of this ResultSet object, with the same configuration.
        """
        result_set = object.__new__(self.__class__)
        result_set.__dict__.update(self.__dict__)
        return result_set

    def config(self, distinct=None, offset=None, limit=None):
        """Configure this result object in-place. All parameters are optional.

        @param distinct: Boolean enabling/disabling usage of the DISTINCT
            keyword in the query made.
        @param offset: Offset where results will start to be retrieved
            from the result set.
        @param limit: Limit the number of objects retrieved from the
            result set.

        @return: self (not a copy).
        """

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

    def _load_objects(self, result, values):
        return self._store._load_objects(self._cls_spec_info, result, values)

    def __iter__(self):
        """Iterate the results of the query.
        """
        result = self._store._connection.execute(self._get_select())
        for values in result:
            yield self._load_objects(result, values)

    def __getitem__(self, index):
        """Get an individual item by offset, or a range of items by slice.

        If a slice is used, a new L{ResultSet} will be return
        appropriately modified with OFFSET and LIMIT clauses.
        """
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
            return self._load_objects(result, values)
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
            return self._load_objects(result, values)
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
            return self._load_objects(result, values)
        return None

    def order_by(self, *args):
        """Specify the ordering of the results.

        The query will be modified appropriately with an ORDER BY clause.

        Ascending and descending order can be specified by wrapping
        the columns in L{Asc} and L{Desc}.

        @param args: One or more L{storm.expr.Column} objects.
        """
        if self._offset is not Undef or self._limit is not Undef:
            raise FeatureError("Can't reorder a sliced result set")
        self._order_by = args or Undef
        return self

    def remove(self):
        """Remove all rows represented by this ResultSet from the database.

        This is done efficiently with a DELETE statement, so objects
        are not actually loaded into Python.
        """
        if self._offset is not Undef or self._limit is not Undef:
            raise FeatureError("Can't remove a sliced result set")
        if type(self._cls_spec_info) is tuple:
            raise FeatureError("Removing not yet supported with tuple finds")
        if self._select is not Undef:
            raise FeatureError("Removing isn't supported with "
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
        variable_factory = getattr(column, "variable_factory", None)
        if variable_factory:
            variable = variable_factory()
            result.set_variable(variable, value)
            return variable.get()
        return value

    def count(self, expr=Undef, distinct=False):
        """Get the number of objects represented by this ResultSet."""
        return int(self._aggregate(Count(expr, distinct)))

    def max(self, expr):
        """Get the highest value from an expression."""
        return self._aggregate(Max(expr), expr)

    def min(self, expr):
        """Get the lowest value from an expression."""
        return self._aggregate(Min(expr), expr)

    def avg(self, expr):
        """Get the average value from an expression."""
        value = self._aggregate(Avg(expr))
        if value is None:
            return value
        return float(value)

    def sum(self, expr):
        """Get the sum of all values in an expression."""
        return self._aggregate(Sum(expr), expr)

    def values(self, *columns):
        """Retrieve only the specified columns.

        This does not load full objects from the database into Python.

        @param columns: One or more L{storm.expr.Column} objects whose
            values will be fetched.
        @return: An iterator of tuples of the values for each column
            from each matching row in the database.
        """
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
        """Update objects in the result set with the given arguments.

        This method will update all objects in the current result set
        to match expressions given as equalities or keyword arguments.
        These objects may still be in the database (an UPDATE is issued)
        or may be cached.

        For instance, C{result.set(Class.attr1 == 1, attr2=2)} will set
        C{attr1} to 1 and C{attr2} to 2, on all matching objects.
        """

        if type(self._cls_spec_info) is tuple:
            raise FeatureError("Setting isn't supported with tuple finds")
        if self._select is not Undef:
            raise FeatureError("Setting isn't supported with "
                               "set expressions (unions, etc)")

        if not (args or kwargs):
            return

        changes = {}
        cls = self._cls_spec_info.cls

        # For now only "Class.attr == var" is supported in args.
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
        self._store.execute(expr, noresult=True)

        try:
            cached = self.cached()
        except CompileError:
            for obj_info in self._store._iter_cached():
                for column in changes:
                    obj_info.variables[column].set(AutoReload)
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
        """Return matching objects from the cache for the current query."""
        if type(self._cls_spec_info) is tuple:
            raise FeatureError("Cached finds not supported with tuples")
        if self._tables is not Undef:
            raise FeatureError("Cached finds not supported with custom tables")
        if self._where is Undef:
            match = None
        else:
            match = compile_python.get_matcher(self._where)
            name_to_column = dict((column.name, column)
                                  for column in self._cls_spec_info.columns)
            def get_column(name, name_to_column=name_to_column):
                return obj_info.variables[name_to_column[name]].get()
        objects = []
        cls = self._cls_spec_info.cls
        for obj_info in self._store._iter_cached():
            try:
                if (obj_info.cls_info is self._cls_spec_info and
                    (match is None or match(get_column))):
                    objects.append(self._store._get_object(obj_info))
            except LostObjectError:
                pass # This may happen when resolving lazy values
                     # in get_column().
        return objects

    def _set_expr(self, expr_cls, other, all=False):
        if self._cls_spec_info != other._cls_spec_info:
            raise FeatureError("Incompatible results for set operation")

        expr = expr_cls(self._get_select(), other._get_select(), all=all)
        return ResultSet(self._store, self._cls_spec_info, select=expr)

    def union(self, other, all=False):
        """Get the L{Union} of this result set and another.

        @param all: If True, include duplicates.
        """
        if isinstance(other, EmptyResultSet):
            return self
        return self._set_expr(Union, other, all)

    def difference(self, other, all=False):
        """Get the difference, using L{Except}, of this result set and another.

        @param all: If True, include duplicates.
        """
        if isinstance(other, EmptyResultSet):
            return self
        return self._set_expr(Except, other, all)

    def intersection(self, other, all=False):
        """Get the L{Intersection} of this result set and another.

        @param all: If True, include duplicates.
        """
        if isinstance(other, EmptyResultSet):
            return other
        return self._set_expr(Intersect, other, all)


class EmptyResultSet(object):
    """An object that looks like a L{ResultSet} but represents no rows.

    This is convenient for application developers who want to provide
    a method which is guaranteed to return a L{ResultSet}-like object
    but which, in certain cases, knows there is no point in querying
    the database. For example::

        def get_people(self, ids):
            if not ids:
                return EmptyResultSet()
            return store.find(People, People.id.is_in(ids))

    The methods on EmptyResultSet (L{one}, L{config}, L{union}, etc)
    are meant to emulate a L{ResultSet} which has matched no rows.
    """

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
    """The representation of a set of tables which can be queried at once.

    This will typically be constructed by a call to L{Store.using}.
    """

    def __init__(self, store, tables):
        self._store = store
        self._tables = tables

    def find(self, cls_spec, *args, **kwargs):
        """Perform a query on the previously specified tables.

        This is identical to L{Store.find} except that the tables are
        explicitly specified instead of relying on inference.

        @return: A L{ResultSet}.
        """
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
    """A marker for reloading a single value.

    Often this will be used to specify that a specific attribute
    should be loaded from the database on the next access, like so::

        storm_object.property = AutoReload

    On the next access to C{storm_object.property}, the value will be
    loaded from the database.

    It is also often used as a default value for a property::

        class Person(object):
            __storm_table__ = "person"
            id = Int(allow_none=False, default=AutoReload)

        person = store.add(Person)
        person.id # gets the attribute from the database.
    """
    pass

AutoReload = AutoReload()
