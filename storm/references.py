from storm.store import Store
from storm.expr import Undef, compare_columns
from storm.info import *


class Reference(object):

    def __init__(self, local_key, remote_key, on_remote=False):
        self._relation = Relation(local_key, remote_key, False, on_remote)

    def __get__(self, local, cls=None):
        if local is None:
            return self

        remote = self._relation.get_remote(local)
        if remote is not None:
            return remote

        store = Store.of(local)
        if store is None:
            return None

        if self._relation.remote_key_is_primary:
            remote = store.get(self._relation.remote_cls,
                               self._relation.get_local_values(local))
        else:
            where = self._relation.get_where_for_remote(local)
            remote = store.find(self._relation.remote_cls, where).one()

        if remote is not None:
            self._relation.link(local, remote)

        return remote

    def __set__(self, local, remote):
        self._relation.link(local, remote, True)


class ReferenceSet(object):

    def __init__(self, local_key, remote_key):
        self._relation = Relation(local_key, remote_key, True, True)

    def __get__(self, local, cls=None):
        if local is None:
            return self
        store = Store.of(local)
        if store is None:
            return None
        return BoundReferenceSet(self._relation, local, store)


class BoundReferenceSet(object):

    def __init__(self, relation, local, store):
        self._relation = relation
        self._local = local
        self._store = store

    def __iter__(self):
        where = self._relation.get_where_for_remote(self._local)
        return self._store.find(self._relation.remote_cls, where).__iter__()

    def find(self, *args, **kwargs):
        where = self._relation.get_where_for_remote(self._local)
        return self._store.find(self._relation.remote_cls, where,
                                *args, **kwargs)

    def clear(self):
        set_kwargs = {}
        for remote_column in self._relation.remote_key:
            set_kwargs[remote_column.name] = None
        where = self._relation.get_where_for_remote(self._local)
        self._store.find(self._relation.remote_cls, where).set(**set_kwargs)

    def count(self):
        where = self._relation.get_where_for_remote(self._local)
        return self._store.find(self._relation.remote_cls, where).count()

    def order_by(self, *args):
        where = self._relation.get_where_for_remote(self._local)
        result = self._store.find(self._relation.remote_cls, where)
        return result.order_by(*args)

    def add(self, remote):
        self._relation.link(self._local, remote, True)

    def remove(self, remote):
        self._relation.unlink(self._local, remote, True)


class Relation(object):

    def __init__(self, local_key, remote_key, many, on_remote):
        if type(local_key) is tuple:
            self.local_key = local_key
        else:
            self.local_key = (local_key,)
        if type(remote_key) is tuple:
            self.remote_key = remote_key
        else:
            self.remote_key = (remote_key,)

        self.remote_cls = self.remote_key[0].cls
        self.remote_key_is_primary = False

        primary_key = get_cls_info(self.remote_cls).primary_key
        if len(primary_key) == len(self.remote_key):
            for column1, column2 in zip(self.remote_key, primary_key):
                if column1.name != column2.name:
                    break
            else:
                self.remote_key_is_primary = True

        self._many = many
        self._on_remote = on_remote
        self._local_columns = {}

    def get_remote(self, local):
        return get_obj_info(local).get(self)

    def get_where_for_remote(self, local):
        local_values = self.get_local_values(local)
        if Undef in local_values:
            Store.of(local).flush()
            local_values = self.get_local_values(local)
        return compare_columns(self.remote_key, local_values)

    def get_local_values(self, local):
        return tuple(prop.__get__(local, default=Undef)
                     for prop in self.local_key)

    def link(self, local, remote, set=False):
        store = Store.of(local) or Store.of(remote)
        assert store is not None

        local_info = get_obj_info(local)
        remote_info = get_obj_info(remote)

        if self._many:
            local_info.get(self, factory=dict)[id(remote)] = remote
        else:
            old_remote = local_info.get(self)
            if old_remote is not None:
                self.unlink(local, old_remote)
            local_info[self] = remote

        track_changes = False
        if set:
            pairs = zip(self.local_key, self.remote_key)
            if self._on_remote:
                for local_prop, remote_prop in pairs:
                    local_value = local_prop.__get__(local, default=Undef)
                    if local_value is Undef:
                        track_changes = True
                    else:
                        remote_prop.__set__(remote, local_value)

                if track_changes:
                    store.add_flush_order(local, remote)
                    local_info.hook("changed",
                                    self._track_local_changes, remote)
                    local_info.hook("flushed",
                                    self._break_on_local_flushed, remote)
            else:
                for local_prop, remote_prop in pairs:
                    remote_value = remote_prop.__get__(remote, default=Undef)
                    if remote_value is Undef:
                        track_changes = True
                    else:
                        local_prop.__set__(local, remote_value)

                if track_changes:
                    store.add_flush_order(remote, local)
                    remote_info.hook("changed",
                                     self._track_remote_changes, local)
                    remote_info.hook("flushed",
                                     self._break_on_remote_flushed, local)

        local_info.hook("changed", self._break_on_local_diverged, remote)
        if not track_changes:
            remote_info.hook("changed", self._break_on_remote_diverged, local)

    def unlink(self, local, remote, set=False):

        local_info = get_obj_info(local)

        unhook = False
        if self._many:
            relations = local_info.get(self)
            if (relations is not None and
                relations.pop(id(remote), None) is not None):
                unhook = True
        elif local_info.pop(self, None) is not None:
            unhook = True
        
        if unhook:
            remote_info = get_obj_info(remote)

            local_info.unhook("changed",
                              self._track_local_changes, remote)
            local_info.unhook("changed",
                              self._break_on_local_diverged, remote)
            local_info.unhook("flushed",
                              self._break_on_local_flushed, remote)

            remote_info.unhook("changed",
                               self._track_remote_changes, local)
            remote_info.unhook("changed",
                               self._break_on_remote_diverged, local)
            remote_info.unhook("flushed",
                               self._break_on_remote_flushed, local)

            store = Store.of(local) or Store.of(remote)
            if self._on_remote:
                store.remove_flush_order(local, remote)
            else:
                store.remove_flush_order(remote, local)

        if set:
            if self._on_remote:
                for remote_prop in self.remote_key:
                    remote_prop.__set__(remote, None)
            else:
                for local_prop in self.local_key:
                    local_prop.__set__(local, None)

    def _track_local_changes(self, local_info,
                              name, old_value, new_value, remote):
        """Deliver changes in local to remote.

        This hook ensures that the remote object will keep track of
        changes done in the local object, either manually or at
        flushing time.
        """
        local_columns = self._get_local_columns(local_info.obj.__class__)
        for remote_prop, local_column in zip(self.remote_key, local_columns):
            if local_column.name == name:
                remote_prop.__set__(remote, new_value)
                break

    def _track_remote_changes(self, remote_info,
                              name, old_value, new_value, local):
        """Deliver changes in remote to local.

        This hook ensures that the local object will keep track of
        changes done in the remote object, either manually or at
        flushing time.
        """
        for local_prop, remote_column in zip(self.local_key, self.remote_key):
            if remote_column.name == name:
                local_prop.__set__(local, new_value)
                break

    def _break_on_local_diverged(self, local_info,
                                 name, old_value, new_value, remote):
        """Break the remote/local relationship on diverging changes.

        This hook ensures that if the local object has an attribute
        changed by hand in a way that diverges from the remote object,
        it stops tracking changes.
        """
        local = local_info.obj
        local_columns = self._get_local_columns(local.__class__)
        for local_column, remote_column in zip(local_columns, self.remote_key):
            if local_column.name == name:
                if (new_value != get_obj_info(remote).
                                 get_value(remote_column.name, Undef)):
                    self.unlink(local, remote)
                break

    def _break_on_remote_diverged(self, remote_info,
                                  name, old_value, new_value, local):
        """Break the remote/local relationship on diverging changes.

        This hook ensures that if the remote object has an attribute
        changed by hand in a way that diverges from the local object,
        the relationship is undone.
        """
        remote = remote_info.obj
        local_columns = self._get_local_columns(local.__class__)
        for remote_column, local_column in zip(self.remote_key, local_columns):
            if remote_column.name == name:
                if (new_value != get_obj_info(local).
                                 get_value(local_column.name, Undef)):
                    self.unlink(local, remote)
                break

    def _break_on_local_flushed(self, local_info, remote):
        """Break the remote/local relationship on flush."""
        self.unlink(local_info.obj, remote)

    def _break_on_remote_flushed(self, remote_info, local):
        """Break the remote/local relationship on flush."""
        self.unlink(local, remote_info.obj)

    def _get_local_columns(self, cls):
        try:
            return self._local_columns[cls]
        except KeyError:
            columns = [prop.__get__(None, cls) for prop in self.local_key]
            self._local_columns[cls] = columns
            return columns
