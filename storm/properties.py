#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
from datetime import datetime

from storm.info import get_obj_info
from storm.expr import Column, Undef
from storm.kinds import *


__all__ = ["NullableError", "Property", "Bool", "Int", "Float",
           "Str", "Unicode", "DateTime", "Date", "Time", "Pickle"]


class NullableError(ValueError):
    pass


def Bool(name=None, default=Undef, nullable=True):
    return Property(name, BoolKind(), default, nullable)
    
def Int(name=None, default=Undef, nullable=True):
    return Property(name, IntKind(), default, nullable)

def Float(name=None, default=Undef, nullable=True):
    return Property(name, FloatKind(), default, nullable)

def Str(name=None, default=Undef, nullable=True):
    return Property(name, StrKind(), default, nullable)

def Unicode(name=None, default=Undef, nullable=True):
    return Property(name, UnicodeKind(), default, nullable)

def DateTime(name=None, default=Undef, nullable=True):
    return Property(name, DateTimeKind(), default, nullable)

def Date(name=None, default=Undef, nullable=True):
    return Property(name, DateKind(), default, nullable)

def Time(name=None, default=Undef, nullable=True):
    return Property(name, TimeKind(), default, nullable)

def Pickle(name=None, default=Undef, nullable=True):
    return Property(name, PickleKind(), default, nullable)


class Property(object):

    def __init__(self, name=None, kind=None, default=Undef, nullable=True):
        self._name = name
        self._kind = kind or AnyKind()
        self._columns = {}

        self._to_python = self._kind.to_python
        self._from_python = self._kind.from_python

        if default is Undef:
            self._default = Undef
        else:
            self._default = self._from_python(default)

        self._nullable = bool(nullable)

    def __get__(self, obj, cls=None, default=None):
        if obj is None:
            return self._get_column(cls)
        if self._name is None:
            self._detect_name(obj.__class__)
        value = get_obj_info(obj).get_value(self._name, self._default)
        if value is Undef:
            return default
        elif value is None:
            return value
        return self._to_python(value)

    def __set__(self, obj, value):
        if value is None and self._nullable is False:
            raise NullableError("None isn't acceptable for that attribute")
        if self._name is None:
            self._detect_name(obj.__class__)
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
            column = PropertyColumn(self, cls, self._name, cls.__table__[0],
                                    self._kind, self._default, self._nullable)
            self._columns[cls] = column
        return column


class PropertyColumn(Column):

    def __init__(self, prop, cls, name, table, kind, default, nullable):
        if default is not Undef:
            default = kind.to_database(default)
        Column.__init__(self, name, table, kind, default, nullable)
        self.cls = cls

        # Copy attributes from the property to avoid one additional
        # function call on each access.
        for attr in ["__get__", "__set__", "__delete__"]:
            setattr(self, attr, getattr(prop, attr))
