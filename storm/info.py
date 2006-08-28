#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
from storm.expr import Column, FromExpr, CompileError, compile
from storm.event import EventSystem
from storm import Undef


__all__ = ["get_obj_info", "get_cls_info", "get_info",
           "ClassInfo", "ObjectInfo", "ClassAlias"]


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

        pairs = []
        for attr in dir(cls):
            column = getattr(cls, attr, None)
            if isinstance(column, Column):
                pairs.append((attr, column))
        pairs.sort()

        self.columns = tuple(pair[1] for pair in pairs)

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

    __hash__ = object.__hash__

    def __init__(self, obj):
        self.obj = obj
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

    def save(self):
        for variable in self._variable_sequence:
            variable.save()
        self.event.save()
        self._saved_attrs = self.obj.__dict__.copy()
        self._saved_self = self._copy_object(self.items())

    def save_attributes(self):
        self._saved_attrs = self.obj.__dict__.copy()

    def checkpoint(self):
        for variable in self._variable_sequence:
            variable.checkpoint()

    def restore(self):
        for variable in self._variable_sequence:
            variable.restore()
        self.event.restore()
        self.obj.__dict__ = self._saved_attrs.copy()
        self.clear()
        self.update(self._saved_self)

    def _copy_object(self, obj):
        obj_type = type(obj)
        if obj_type is dict:
            return dict(((self._copy_object(key), self._copy_object(value))
                         for key, value in obj.iteritems()))
        elif obj_type in (tuple, set, list):
            return obj_type(self._copy_object(subobj) for subobj in obj)
        else:
            return obj


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
    return compile(state, table[0])
