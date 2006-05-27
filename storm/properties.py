from datetime import datetime

from storm.info import get_obj_info
from storm.expr import Column, Undef
from storm.kinds import *


__all__ = ["Property", "Bool", "Int", "Float", "Str", "Unicode",
           "DateTime", "Date", "Time"]


def Bool(name=None):
    return Property(name, BoolKind())
    
def Int(name=None):
    return Property(name, IntKind())

def Float(name=None):
    return Property(name, FloatKind())

def Str(name=None):
    return Property(name, StrKind())

def Unicode(name=None):
    return Property(name, UnicodeKind())

def DateTime(name=None):
    return Property(name, DateTimeKind())

def Date(name=None):
    return Property(name, DateKind())

def Time(name=None):
    return Property(name, TimeKind())


class Property(object):

    def __init__(self, name=None, kind=None):
        self._name = name
        self._kind = kind or AnyKind()
        self._columns = {}

        self._to_python = self._kind.to_python
        self._from_python = self._kind.from_python

    def __get__(self, obj, cls=None, default=None):
        if obj is None:
            return self._get_column(cls)
        if self._name is None:
            self._detect_name(obj.__class__)
        value = get_obj_info(obj).get_value(self._name, Undef)
        if value is Undef:
            return default
        return self._to_python(value)

    def __set__(self, obj, value):
        if self._name is None:
            self._detect_name(obj.__class__)
        # XXX The following if is covered by tests, but not properties ones.
        if value is None:
            get_obj_info(obj).set_value(self._name, None)
        else:
            get_obj_info(obj).set_value(self._name, self._from_python(value))

    def __delete__(self, obj):
        if self._name is None:
            self._detect_name(obj.__class__)
        get_obj_info(obj).del_value(self._name)

    def _detect_name(self, used_cls):
        self_id = id(self)
        for cls in used_cls.__mro__:
            for attr, prop in cls.__dict__.iteritems():
                if id(prop) == self_id:
                    self._name = attr
                    return
        raise RuntimeError("Property used in an unknown class")

    def _get_column(self, cls):
        column = self._columns.get(cls)
        if column is None:
            if self._name is None:
                self._detect_name(cls)
            column = PropertyColumn(self, cls,
                                    self._name, cls.__table__[0], self._kind)
            self._columns[cls] = column
        return column


class PropertyColumn(Column):

    def __init__(self, prop, cls, name, table, kind):
        Column.__init__(self, name, table, kind)
        self.cls = cls

        # Copy attributes from the property to avoid one additional
        # function call on each access.
        for attr in ["__get__", "__set__", "__delete__"]:
            setattr(self, attr, getattr(prop, attr))
