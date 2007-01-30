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

from storm.info import get_obj_info, get_cls_info
from storm.expr import Column, Undef
from storm.variables import *
from storm import Undef


__all__ = ["Property", "SimpleProperty",
           "Bool", "Int", "Float", "Str", "Unicode",
           "DateTime", "Date", "Time", "Pickle"]


class Property(object):

    def __init__(self, name=None, variable_class=Variable, variable_kwargs={}):
        self._name = name
        self._variable_class = variable_class
        self._variable_kwargs = variable_kwargs
        self._columns = {}

    def __get__(self, obj, cls=None):
        if obj is None:
            return self._get_column(cls)
        column = self._get_column(obj.__class__)
        return get_obj_info(obj).variables[column].get()

    def __set__(self, obj, value):
        column = self._get_column(obj.__class__)
        get_obj_info(obj).variables[column].set(value)

    def __delete__(self, obj):
        column = self._get_column(obj.__class__)
        get_obj_info(obj).variables[column].delete()

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
            column = PropertyColumn(self, cls, self._name,
                                    self._variable_class,
                                    self._variable_kwargs)
            self._columns[cls] = column
        return column


class PropertyColumn(Column):

    def __init__(self, prop, cls, name, variable_class, variable_kwargs):
        Column.__init__(self, name, cls,
                        VariableFactory(variable_class, column=self,
                                        **variable_kwargs))

        self.cls = cls # Used by references

        # Copy attributes from the property to avoid one additional
        # function call on each access.
        for attr in ["__get__", "__set__", "__delete__"]:
            setattr(self, attr, getattr(prop, attr))


class SimpleProperty(Property):

    variable_class = None

    def __init__(self, name=None, default=Undef,
                 default_factory=Undef, not_none=False):
        variable_kwargs = {"not_none": not_none,
                           "value": default,
                           "value_factory": default_factory}
        Property.__init__(self, name, self.variable_class, variable_kwargs)


class Bool(SimpleProperty):
    variable_class = BoolVariable
 
class Int(SimpleProperty):
    variable_class = IntVariable

class Float(SimpleProperty):
    variable_class = FloatVariable

class Str(SimpleProperty):
    variable_class = StrVariable

class Unicode(SimpleProperty):
    variable_class = UnicodeVariable

class DateTime(SimpleProperty):
    variable_class = DateTimeVariable

class Date(SimpleProperty):
    variable_class = DateVariable

class Time(SimpleProperty):
    variable_class = TimeVariable

class Pickle(SimpleProperty):
    variable_class = PickleVariable
