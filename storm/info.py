#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
import weakref

from storm.expr import Expr, FromExpr, Column, Desc
from storm.expr import SQLToken, CompileError, compile
from storm.event import EventSystem
from storm import Undef


__all__ = ["get_obj_info", "set_obj_info", "get_cls_info", "get_info",
           "ClassInfo", "ObjectInfo", "ClassAlias"]


def get_obj_info(obj):
    try:
        return obj.__object_info
    except AttributeError:
        return obj.__dict__.setdefault("__object_info", ObjectInfo(obj))

def set_obj_info(obj, obj_info):
    obj.__dict__["__object_info"] = obj_info

def get_cls_info(cls):
    try:
        # Can't use attribute access here, otherwise subclassing won't work.
        return cls.__dict__["__class_info"]
    except KeyError:
        cls.__class_info = ClassInfo(cls)
        return cls.__class_info

def get_info(obj):
    try:
        obj_info = obj.__object_info
    except AttributeError:
        obj_info = get_obj_info(obj)
    return obj_info, obj_info.cls_info


class ClassInfo(dict):
    """Persistent storm-related information of a class.

    The following attributes are defined:

              table - Expression from where columns will be looked up.
                cls - Class which should be used to build objects.
            columns - Tuple of column properties found in the class.
        primary_key - Tuple of column properties used to form the primary key
    primary_key_pos - Position of primary_key items in the columns tuple.

    """

    def __init__(self, cls):
        __table__ = getattr(cls, "__table__", ())
        if len(__table__) != 2:
            raise RuntimeError("%s.__table__ must be (<table name>, "
                               "<primary key(s)>) tuple." % repr(cls))

        self.table = __table__[0]
        self.cls = cls

        if not isinstance(self.table, Expr):
            self.table = SQLToken(self.table)

        pairs = []
        for attr in dir(cls):
            column = getattr(cls, attr, None)
            if isinstance(column, Column):
                pairs.append((attr, column))
        pairs.sort()

        self.columns = tuple(pair[1] for pair in pairs)
        self.attributes = dict(pairs)

        name_positions = dict((prop.name, i)
                              for i, prop in enumerate(self.columns))

        primary_key_names = __table__[1]
        if type(primary_key_names) not in (list, tuple):
            primary_key_names = (primary_key_names,)

        self.primary_key_pos = tuple(name_positions[name]
                                     for name in primary_key_names)
        self.primary_key = tuple(self.columns[i]
                                 for i in self.primary_key_pos)
        self.primary_key_idx = dict((id(column), i)
                                    for i, column in
                                    enumerate(self.primary_key))

        __order__ = getattr(cls, "__order__", None)
        if __order__ is None:
            self.default_order = Undef
        else:
            if type(__order__) is not tuple:
                __order__ = (__order__,)
            self.default_order = []
            for item in __order__:
                if isinstance(item, basestring):
                    if item.startswith("-"):
                        prop = Desc(getattr(cls, item[1:]))
                    else:
                        prop = getattr(cls, item)
                else:
                    prop = item
                self.default_order.append(prop)

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return self is not other


class ObjectInfo(dict):

    __hash__ = object.__hash__

    def __init__(self, obj):
        self.set_obj(obj)

        self.event = EventSystem(self)
        self.variables = {}

        self._saved_self = None
        self._saved_attrs = None

        self.cls_info = get_cls_info(obj.__class__)

        variables = self.variables
        for column in self.cls_info.columns:
            variables[column] = column.variable_factory(column=column,
                                                        event=self.event)
        
        self.primary_vars = tuple(variables[column]
                                  for column in self.cls_info.primary_key)

        self._variable_sequence = self.variables.values()

    def set_obj(self, obj):
        self.get_obj = weakref.ref(obj, self._emit_object_deleted)

    def save(self):
        for variable in self._variable_sequence:
            variable.save()
        self.event.save()
        self._saved_self = self._copy_object(self.items())
        obj = self.get_obj()
        if obj is None:
            self._saved_attrs = None
        else:
            self._saved_attrs = obj.__dict__.copy()
            self._saved_attrs.pop("__object_info", None) # Circular reference.

    def save_attributes(self):
        obj = self.get_obj()
        if obj is None:
            self._saved_attrs = None
        else:
            self._saved_attrs = obj.__dict__.copy()
            self._saved_attrs.pop("__object_info", None) # Circular reference.

    def checkpoint(self):
        for variable in self._variable_sequence:
            variable.checkpoint()

    def restore(self):
        for variable in self._variable_sequence:
            variable.restore()
        self.event.restore()
        self.clear()
        self.update(self._saved_self)
        obj = self.get_obj()
        if obj is not None:
            attrs = self._saved_attrs.copy()
            try:
                attrs["__object_info"] = obj.__dict__["__object_info"]
            except KeyError:
                pass
            obj.__dict__ = attrs

    def _copy_object(self, obj):
        obj_type = type(obj)
        if obj_type is dict:
            return dict(((self._copy_object(key), self._copy_object(value))
                         for key, value in obj.iteritems()))
        elif obj_type in (tuple, set, list):
            return obj_type(self._copy_object(subobj) for subobj in obj)
        else:
            return obj

    def _emit_object_deleted(self, obj_ref):
        self.event.emit("object-deleted")


# For get_obj_info(), an ObjectInfo is its own obj_info.
ObjectInfo.__object_info = property(lambda self: self)


class ClassAlias(FromExpr):

    alias_count = 0

    def __new__(self_cls, cls, name=Undef):
        if name is Undef:
            ClassAlias.alias_count += 1
            name = "_%x" % ClassAlias.alias_count
        cls_info = get_cls_info(cls)
        alias_cls = type(cls.__name__+"Alias", (cls, self_cls),
                         {"__table__": (name, cls.__table__[1])})
        alias_cls_info = get_cls_info(alias_cls)
        alias_cls_info.cls = cls
        return alias_cls


@compile.when(type)
def compile_type(compile, state, expr):
    table = getattr(expr, "__table__", None)
    if table is None:
        raise CompileError("Don't know how to compile %r" % expr)
    if not state.column_prefix and issubclass(expr, ClassAlias):
        cls_info = get_cls_info(expr)
        return "%s AS %s" % (compile(state, cls_info.cls), table[0])
    if isinstance(table[0], basestring):
        return table[0]
    return compile(state, table[0])
