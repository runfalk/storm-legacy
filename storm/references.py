from storm.store import Store
from storm.expr import Undef, compare_columns
from storm.info import *


class Reference(object):

    def __init__(self, local_key, remote_key, on_remote=False):
        if type(local_key) is tuple:
            self._local_key = local_key
        else:
            self._local_key = (local_key,)
        if type(remote_key) is tuple:
            self._remote_key = remote_key
        else:
            self._remote_key = (remote_key,)
        self._local_data = {}
        self._remote_data = {}
        self._on_remote = on_remote

    def __get__(self, local, cls=None):
        if local is None:
            return self

        # If a reference was already established, use it.
        references = get_obj_info(local).get("references")
        if references is not None:
            remote = references.get(self)
            if remote is not None:
                return remote

        store = Store.of(local)
        if store is None:
            return None

        local_values = tuple(prop.__get__(local, default=Undef)
                             for prop in self._local_key)
        if Undef in local_values:
            return None

        remote_cls = getattr(self._remote_key[0], "cls", None)
        remote_data = self._get_remote_data(remote_cls)
        if remote_data.is_primary:
            remote = store.get(remote_cls, local_values)
        else:
            where = compare_columns(remote_data.columns, local_values)
            remote = store.find(remote_cls, where).one()

        if remote is not None:
            self._make_reference(local, remote)

        return remote

    def __set__(self, local, remote):
        store = Store.of(local) or Store.of(remote)
        assert store is not None

        track_changes = False
        pairs = zip(self._local_key, self._remote_key)

        if not self._on_remote:
            store.add_flush_order(remote, local)
            for local_prop, remote_prop in pairs:
                remote_value = remote_prop.__get__(remote, default=Undef)
                if remote_value is Undef:
                    track_changes = True
                else:
                    local_prop.__set__(local, remote_value)
            self._make_reference(local, remote,
                                 track_remote_changes=track_changes)
        else:
            store.add_flush_order(local, remote)
            for local_prop, remote_prop in pairs:
                local_value = local_prop.__get__(local, default=Undef)
                if local_value is Undef:
                    track_changes = True
                else:
                    remote_prop.__set__(remote, local_value)
            self._make_reference(local, remote,
                                 track_local_changes=track_changes)

    def _make_reference(self, local, remote,
                        track_local_changes=False,
                        track_remote_changes=False):
        local_info = get_obj_info(local)
        remote_info = get_obj_info(remote)

        references = local_info.get("references", factory=dict)

        old_remote = references.get(self)
        if old_remote is not None:
            self._break_reference(local_info, local,
                                  get_obj_info(old_remote), old_remote)

        references[self] = remote

        if track_local_changes:
            local_info.hook("changed", self._track_local_changes, remote)
            local_info.hook("flushed", self._break_on_local_flushed, remote)
        elif track_remote_changes:
            remote_info.hook("changed", self._track_remote_changes, local)
            remote_info.hook("flushed", self._break_on_remote_flushed, local)
        else:
            remote_info.hook("changed", self._break_on_remote_diverged, local)

        local_info.hook("changed", self._break_on_local_diverged, remote)

    def _break_reference(self, local_info, local, remote_info, remote):
        remote_info.unhook("changed", self._track_remote_changes, local)
        remote_info.unhook("changed", self._break_on_remote_diverged, local)
        remote_info.unhook("flushed", self._break_on_remote_flushed, local)
        local_info.unhook("changed", self._track_local_changes, remote)
        local_info.unhook("changed", self._break_on_local_diverged, remote)
        local_info.unhook("flushed", self._break_on_local_flushed, remote)
        del local_info["references"][self]

        store = Store.of(local) or Store.of(remote)
        if self._on_remote:
            store.remove_flush_order(local, remote)
        else:
            store.remove_flush_order(remote, local)

    def _track_local_changes(self, local_info,
                              name, old_value, new_value, remote):
        """Deliver local changes to the remote object.

        This hook ensures that the remote object will keep track of
        changes done in the local object, either manually or at
        flushing time.
        """
        local_data = self._get_local_data(local_info.obj.__class__)
        for remote_prop, local_column in zip(self._remote_key,   
                                             local_data.columns):
            if local_column.name == name:
                remote_prop.__set__(remote, new_value)
                break

    def _track_remote_changes(self, remote_info,
                              name, old_value, new_value, local):
        """Deliver remote changes to the local object.

        This hook ensures that the local object will keep track of
        changes done in the remote object, either manually or at
        flushing time.
        """
        remote_data = self._get_remote_data(remote_info.obj.__class__)
        for local_prop, remote_column in zip(self._local_key,   
                                             remote_data.columns):
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
        local_data = self._get_local_data(local.__class__)
        remote_data = self._get_remote_data(remote.__class__)
        for local_column, remote_column in zip(local_data.columns,
                                               remote_data.columns):
            if local_column.name != name:
                continue

            remote_info = get_obj_info(remote)
            if new_value != remote_info.get_value(remote_column.name, Undef):
                self._break_reference(local_info, local, remote_info, remote)
            break

    def _break_on_remote_diverged(self, remote_info,
                                  name, old_value, new_value, local):
        """Break the remote/local relationship on diverging changes.

        This hook ensures that if the remote object has an attribute
        changed by hand in a way that diverges from the local object,
        the relationship is undone.
        """
        remote = remote_info.obj
        remote_data = self._get_remote_data(remote.__class__)
        local_data = self._get_local_data(local.__class__)
        for remote_column, local_column in zip(remote_data.columns,
                                               local_data.columns):
            if remote_column.name != name:
                continue

            local_info = get_obj_info(local)
            if new_value != local_info.get_value(local_column.name, Undef):
                self._break_reference(local_info, local, remote_info, remote)
            break

    def _break_on_local_flushed(self, local_info, remote):
        """Break the remote/local relationship on flush."""
        self._break_reference(local_info, local_info.obj,
                              get_obj_info(remote), remote)

    def _break_on_remote_flushed(self, remote_info, local):
        """Break the remote/local relationship on flush."""
        self._break_reference(get_obj_info(local), local,
                              remote_info, remote_info.obj)

    def _get_local_data(self, cls):
        cls_data = self._local_data.get(cls)
        if cls_data is None:
            cls_data = ReferenceClassData(self._local_key, cls)
            self._local_data[cls] = cls_data
        return cls_data

    def _get_remote_data(self, cls):
        cls_data = self._remote_data.get(cls)
        if cls_data is None:
            cls_data = ReferenceClassData(self._remote_key, cls)
            self._remote_data[cls] = cls_data
        return cls_data


class ReferenceClassData(object):

    def __init__(self, key, cls):
        self.columns = []
        self.is_primary = False

        for prop in key:
            self.columns.append(prop.__get__(None, cls))

        cls_info = get_cls_info(cls)
        if len(cls_info.primary_key) != len(key):
            self.is_primary = False
        else:
            primary_key = cls_info.primary_key
            for column, key_column in zip(self.columns, cls_info.primary_key):
                if column.name != key_column.name:
                    self.is_primary = False
                    break
            else:
                self.is_primary = True
