#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
from bisect import insort_left, bisect_left
from datetime import datetime
from types import ClassType
import weakref
import sys

from storm.exceptions import PropertyPathError
from storm.info import get_obj_info, get_cls_info
from storm.expr import Column, Undef
from storm.variables import *
from storm import Undef


__all__ = ["Property", "SimpleProperty",
           "Bool", "Int", "Float", "Str", "Unicode",
           "DateTime", "Date", "Time", "TimeDelta", "Enum", "Pickle", "List",
           "PropertyRegistry"]


class Property(object):

    def __init__(self, name=None, variable_class=Variable, variable_kwargs={}):
        self._name = name
        self._variable_class = variable_class
        self._variable_kwargs = variable_kwargs

    def __get__(self, obj, cls=None):
        if obj is None:
            return self._get_column(cls)
        obj_info = get_obj_info(obj)
        if cls is None:
            # Don't get obj.__class__ because we don't trust it
            # (might be proxied or whatever). # XXX UNTESTED!
            cls = obj_info.cls_info.cls
        column = self._get_column(cls)
        return obj_info.variables[column].get()

    def __set__(self, obj, value):
        obj_info = get_obj_info(obj)
        # Don't get obj.__class__ because we don't trust it
        # (might be proxied or whatever). # XXX UNTESTED!
        column = self._get_column(obj_info.cls_info.cls)
        obj_info.variables[column].set(value)

    def __delete__(self, obj):
        obj_info = get_obj_info(obj)
        # Don't get obj.__class__ because we don't trust it
        # (might be proxied or whatever). # XXX UNTESTED!
        column = self._get_column(obj_info.cls_info.cls)
        obj_info.variables[column].delete()

    def _detect_name(self, used_cls):
        self_id = id(self)
        for cls in used_cls.__mro__:
            for attr, prop in cls.__dict__.iteritems():
                if id(prop) == self_id:
                    self._name = attr
                    return
        raise RuntimeError("Property used in an unknown class")

    def _get_column(self, cls):
        # Cache per-class column values in the class itself, to avoid
        # holding a strong reference to it here, and thus rendering
        # classes uncollectable in certain situations (e.g. subclasses
        # where the property is stored in the base).
        try:
            # Use class dictionary explicitly to get sensible
            # results on subclasses.
            column = cls.__dict__["_storm_columns_"].get(self)
        except KeyError:
            cls._storm_columns_ = {}
            column = None
        if column is None:
            if self._name is None:
                self._detect_name(cls)
            column = PropertyColumn(self, cls, self._name,
                                    self._variable_class,
                                    self._variable_kwargs)
            cls._storm_columns_[self] = column
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

    def __init__(self, name=None, **kwargs):
        kwargs["value"] = kwargs.pop("default", Undef)
        kwargs["value_factory"] = kwargs.pop("default_factory", Undef)
        Property.__init__(self, name, self.variable_class, kwargs)


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

class TimeDelta(SimpleProperty):
    variable_class = TimeDeltaVariable

class Enum(SimpleProperty):
    variable_class = EnumVariable

class Pickle(SimpleProperty):
    variable_class = PickleVariable


class List(SimpleProperty):

    variable_class = ListVariable

    def __init__(self, name=None, **kwargs):
        if "default" in kwargs:
            raise ValueError("'default' not allowed for List. "
                             "Use 'default_factory' instead.")
        type = kwargs.pop("type", None)
        if type is None:
            type = Property()
        kwargs["item_factory"] = VariableFactory(type._variable_class,
                                                 **type._variable_kwargs)
        SimpleProperty.__init__(self, name, **kwargs)


class PropertyRegistry(object):

    def __init__(self):
        self._properties = []

    def get(self, name, namespace=None):
        key = ".".join(reversed(name.split(".")))+"."
        i = bisect_left(self._properties, (key,))
        l = len(self._properties)
        best_props = []
        if namespace is None:
            while i < l and self._properties[i][0].startswith(key):
                path, prop_ref = self._properties[i]
                prop = prop_ref()
                if prop is not None:
                    best_props.append((path, prop))
                i += 1
        else:
            namespace_parts = ("." + namespace).split(".")
            best_path_info = (0, sys.maxint)
            while i < l and self._properties[i][0].startswith(key):
                path, prop_ref = self._properties[i]
                prop = prop_ref()
                if prop is None:
                    i += 1
                    continue
                path_parts = path.split(".")
                path_parts.reverse()
                common_prefix = 0
                for part, ns_part in zip(path_parts, namespace_parts):
                    if part == ns_part:
                        common_prefix += 1
                    else:
                        break
                path_info = (-common_prefix, len(path_parts)-common_prefix)
                if path_info < best_path_info:
                    best_path_info = path_info
                    best_props = [(path, prop)]
                elif path_info == best_path_info:
                    best_props.append((path, prop))
                i += 1
        if not best_props:
            raise PropertyPathError("Path '%s' matches no known property."
                                    % name)
        elif len(best_props) > 1:
            paths = [".".join(reversed(path.split(".")[:-1]))
                     for path, prop in best_props]
            raise PropertyPathError("Path '%s' matches multiple "
                                    "properties: %s" %
                                    (name, ", ".join(paths)))
        return best_props[0][1]

    def add_class(self, cls):
        suffix = cls.__module__.split(".")
        suffix.append(cls.__name__)
        suffix.reverse()
        suffix = ".%s." % ".".join(suffix)
        cls_info = get_cls_info(cls)
        for attr in cls_info.attributes:
            prop = cls_info.attributes[attr]
            prop_ref = weakref.KeyedRef(prop, self._remove, None)
            pair = (attr+suffix, prop_ref)
            prop_ref.key = pair
            insort_left(self._properties, pair)

    def add_property(self, cls, prop, attr_name):
        suffix = cls.__module__.split(".")
        suffix.append(cls.__name__)
        suffix.reverse()
        suffix = ".%s." % ".".join(suffix)
        prop_ref = weakref.KeyedRef(prop, self._remove, None)
        pair = (attr_name+suffix, prop_ref)
        prop_ref.key = pair
        insort_left(self._properties, pair)

    def clear(self):
        del self._properties[:]

    def _remove(self, ref):
        self._properties.remove(ref.key)


global_property_registry = PropertyRegistry()


class PropertyPublisherMeta(type):

    def __init__(self, name, bases, dict):
        if not hasattr(self, "_storm_property_registry"):
            self._storm_property_registry = PropertyRegistry()
        elif hasattr(self, "__table__"):
            self._storm_property_registry.add_class(self)
