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
from weakref import ref

from storm.exceptions import ClassInfoError
from storm.expr import Expr, FromExpr, Column, Desc, TABLE
from storm.expr import SQLToken, CompileError, compile
from storm.event import EventSystem
from storm import Undef


__all__ = ["get_obj_info", "set_obj_info", "get_cls_info",
           "ClassInfo", "ObjectInfo", "ClassAlias"]


def get_obj_info(obj):
    try:
        return obj.__storm_object_info__
    except AttributeError:
        # Instantiate ObjectInfo first, so that it breaks gracefully,
        # in case the object isn't a storm object.
        obj_info = ObjectInfo(obj)
        return obj.__dict__.setdefault("__storm_object_info__", obj_info)

def set_obj_info(obj, obj_info):
    obj.__dict__["__storm_object_info__"] = obj_info

def get_cls_info(cls):
    if "__storm_class_info__" in cls.__dict__:
        # Can't use attribute access here, otherwise subclassing won't work.
        return cls.__dict__["__storm_class_info__"]
    else:
        cls.__storm_class_info__ = ClassInfo(cls)
        return cls.__storm_class_info__

class ClassInfo(dict):
    """Persistent storm-related information of a class.

    The following attributes are defined:

    @ivar table: Expression from where columns will be looked up.
    @ivar cls: Class which should be used to build objects.
    @ivar columns: Tuple of column properties found in the class.
    @ivar primary_key: Tuple of column properties used to form the primary key
    @ivar primary_key_pos: Position of primary_key items in the columns tuple.
    """

    def __init__(self, cls):
        self.table = getattr(cls, "__storm_table__", None)
        if self.table is None:
            raise ClassInfoError("%s.__storm_table__ missing" % repr(cls))

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

        storm_primary = getattr(cls, "__storm_primary__", None)
        if storm_primary is not None:
            if type(storm_primary) is not tuple:
                storm_primary = (storm_primary,)
            self.primary_key = tuple(self.attributes[attr]
                                     for attr in storm_primary)
        else:
            primary = []
            primary_attrs = {}
            for attr, column in pairs:
                if column.primary != 0:
                    if column.primary in primary_attrs:
                        raise ClassInfoError(
                            "%s has two columns with the same primary id: "
                            "%s and %s" %
                            (repr(cls), attr, primary_attrs[column.primary]))
                    primary.append((column.primary, column))
                    primary_attrs[column.primary] = attr
            primary.sort()
            self.primary_key = tuple(column for i, column in primary)

        if not self.primary_key:
            raise ClassInfoError("%s has no primary key information" %
                                 repr(cls))

        # columns have __eq__ implementations that do things we don't want - we
        # want to look these up in a dict and use identity semantics
        id_positions = dict((id(column), i)
                             for i, column in enumerate(self.columns))

        self.primary_key_idx = dict((id(column), i)
                                    for i, column in
                                    enumerate(self.primary_key))
        self.primary_key_pos = tuple(id_positions[id(column)]
                                     for column in self.primary_key)


        __order__ = getattr(cls, "__storm_order__", None)
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

    # For get_obj_info(), an ObjectInfo is its own obj_info.
    __storm_object_info__ = property(lambda self:self)

    def __init__(self, obj):
        # FASTPATH This method is part of the fast path.  Be careful when
        #          changing it (try to profile any changes).

        # First thing, try to create a ClassInfo for the object's class.
        # This ensures that obj is the kind of object we expect.
        self.cls_info = get_cls_info(type(obj))

        self.set_obj(obj)

        self.event = event = EventSystem(self)
        self.variables = variables = {}

        for column in self.cls_info.columns:
            variables[column] = column.variable_factory(column=column,
                                                        event=event)
 
        self.primary_vars = tuple(variables[column]
                                  for column in self.cls_info.primary_key)

    def set_obj(self, obj):
        self.get_obj = ref(obj, self._emit_object_deleted)

    def _emit_object_deleted(self, obj_ref):
        self.event.emit("object-deleted")

    def checkpoint(self):
        for variable in self.variables.itervalues():
            variable.checkpoint()

try:
    from storm.cextensions import ObjectInfo, get_obj_info
except ImportError, e:
    if "cextensions" not in str(e):
        raise


class ClassAlias(FromExpr):

    alias_count = 0

    def __new__(self_cls, cls, name=Undef):
        if name is Undef:
            ClassAlias.alias_count += 1
            name = "_%x" % ClassAlias.alias_count
        cls_info = get_cls_info(cls)
        alias_cls = type(cls.__name__+"Alias", (cls, self_cls),
                         {"__storm_table__": name})
        alias_cls_info = get_cls_info(alias_cls)
        alias_cls_info.cls = cls
        return alias_cls


@compile.when(type)
def compile_type(compile, expr, state):
    table = getattr(expr, "__storm_table__", None)
    if table is None:
        raise CompileError("Don't know how to compile %r" % expr)
    if state.context is TABLE and issubclass(expr, ClassAlias):
        cls_info = get_cls_info(expr)
        return "%s AS %s" % (compile(cls_info.cls, state), table)
    if isinstance(table, basestring):
        return table
    return compile(table, state)
