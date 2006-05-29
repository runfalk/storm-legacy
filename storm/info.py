#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
from storm.expr import Column, Undef


__all__ = ["get_obj_info", "get_cls_info", "get_info",
           "ClassInfo", "ObjectInfo"]


def get_obj_info(obj):
    try:
        return obj.__object_info
    except AttributeError:
        return obj.__dict__.setdefault("__object_info", ObjectInfo(obj))

def get_cls_info(cls):
    try:
        # Can't use attribute access here, otherwise subclassing won't work.
        return cls.__dict__["__class_info"]
    except KeyError:
        cls.__class_info = ClassInfo(cls)
        return cls.__class_info

def get_info(obj):
    try:
        return obj.__object_info, obj.__class__.__class__info
    except AttributeError:
        return get_obj_info(obj), get_cls_info(obj.__class__)


class ClassInfo(dict):

    def __init__(self, cls):
        __table__ = getattr(cls, "__table__", ())
        if len(__table__) != 2:
            raise RuntimeError("%s.__table__ must be (<table name>, "
                               "<primary key(s)>) tuple." % repr(cls))

        pairs = []
        for attr in dir(cls):
            column = getattr(cls, attr)
            if isinstance(column, Column):
                pairs.append((attr, column))
        pairs.sort()

        self.cls = cls
        self.columns = tuple(pair[1] for pair in pairs)
        self.table = __table__[0]

        name_positions = dict((prop.name, i)
                              for i, prop in enumerate(self.columns))

        primary_key_names = __table__[1]
        if type(primary_key_names) not in (list, tuple):
            primary_key_names = (primary_key_names,)

        self.primary_key_pos = tuple(name_positions[name]
                                     for name in primary_key_names)
        self.primary_key = tuple(self.columns[i]
                                 for i in self.primary_key_pos)


class ObjectInfo(dict):

    def __init__(self, obj):
        self.obj = obj
        self._values = {}
        self._hooks = {}

        self._saved_values = None
        self._saved_hooks = None
        self._saved_attrs = None
        self._saved_self = None

        self._checkpoint_values = None

    def get(self, key, default=None, factory=None):
        try:
            return self[key]
        except KeyError:
            if factory is not None:
                return self.setdefault(key, factory())
            return default

    def has_value(self, name):
        return name in self._values

    def get_value(self, name, default=None):
        return self._values.get(name, default)

    def set_value(self, name, value, checkpoint=False):
        old_value = self._values.get(name, Undef)
        self._values[name] = value
        if checkpoint:
            self._checkpoint_values[name] = value
        if old_value != value:
            self.emit("changed", name, old_value, value)

    def del_value(self, name):
        old_value = self._values.pop(name, Undef)
        if old_value is not Undef:
            self.emit("changed", name, old_value, Undef)

    def save(self):
        self._saved_values = self._values.copy()
        self._checkpoint_values = self._values.copy()
        self._saved_hooks = self._copy_object(self._hooks)
        self._saved_attrs = self.obj.__dict__.copy()
        self._saved_self = self._copy_object(self)

    def save_attributes(self):
        self._saved_attrs = self.obj.__dict__.copy()

    def checkpoint(self):
        self._checkpoint_values = self._values.copy()

    def restore(self):
        self._values = self._saved_values.copy()
        self._checkpoint_values = self._saved_values.copy()
        self._hooks = self._copy_object(self._saved_hooks)
        self.obj.__dict__ = self._saved_attrs.copy()
        self.clear()
        self.update(self._saved_self)

    def check_changed(self):
        return self._values != self._checkpoint_values

    def get_changes(self):
        changes = {}
        old_values = self._checkpoint_values
        new_values = self._values
        for name in old_values:
            new_value = new_values.get(name, Undef)
            if new_value is Undef:
                changes[name] = Undef
            elif old_values[name] != new_value:
                changes[name] = new_value
        for name in new_values:
            if name not in old_values:
                changes[name] = new_values[name]
        return changes

    def hook(self, name, callback, *data):
        callbacks = self._hooks.get(name)
        if callbacks is None:
            self._hooks.setdefault(name, set()).add((callback, data))
        else:
            callbacks.add((callback, data))

    def unhook(self, name, callback, *data):
        callbacks = self._hooks.get(name)
        if callbacks is not None:
            callbacks.discard((callback, data))

    def emit(self, name, *args):
        callbacks = self._hooks.get(name)
        if callbacks is not None:
            for callback, data in callbacks.copy():
                if callback(self, *(args+data)) is False:
                    callbacks.discard((callback, data))

    def _copy_object(self, obj):
        obj_type = type(obj)
        if obj_type in (dict, ObjectInfo):
            return dict(((self._copy_object(key), self._copy_object(value))
                         for key, value in obj.iteritems()))
        elif obj_type in (tuple, set, list):
            return obj_type(self._copy_object(subobj) for subobj in obj)
        else:
            return obj

