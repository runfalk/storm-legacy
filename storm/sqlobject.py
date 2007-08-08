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
"""A SQLObject emulation layer for Storm.

L{SQLObjectBase} is the central point of compatibility.
"""

import re

from storm.properties import (
    RawStr, Int, Bool, Float, DateTime, Date, TimeDelta)
from storm.references import Reference, ReferenceSet
from storm.properties import SimpleProperty, PropertyPublisherMeta
from storm.variables import Variable
from storm.exceptions import StormError
from storm.info import get_cls_info
from storm.store import Store
from storm.base import Storm
from storm.expr import SQL, SQLRaw, Desc, And, Or, Not, In, Like
from storm.tz import tzutc
from storm import Undef


__all__ = ["SQLObjectBase", "StringCol", "IntCol", "BoolCol", "FloatCol",
           "DateCol", "UtcDateTimeCol", "IntervalCol", "ForeignKey",
           "SQLMultipleJoin", "SQLRelatedJoin", "DESC", "AND", "OR",
           "NOT", "IN", "LIKE", "SQLConstant", "SQLObjectNotFound",
           "CONTAINSSTRING"]


DESC, AND, OR, NOT, IN, LIKE, SQLConstant = Desc, And, Or, Not, In, Like, SQL

_IGNORED = object()


class SQLObjectNotFound(StormError):
    pass


class SQLObjectStyle(object):

    longID = False

    def idForTable(self, table_name):
        if self.longID:
            return self.tableReference(table_name)
        else:
            return 'id'

    def pythonClassToAttr(self, class_name):
        return self._lowerword(class_name)

    def instanceAttrToIDAttr(self, attr_name):
        return attr_name + "ID"

    def pythonAttrToDBColumn(self, attr_name):
        return self._mixed_to_under(attr_name)

    def dbColumnToPythonAttr(self, column_name):
        return self._under_to_mixed(column_name)

    def pythonClassToDBTable(self, class_name):
        return class_name[0].lower()+self._mixed_to_under(class_name[1:])

    def dbTableToPythonClass(self, table_name):
        return table_name[0].upper()+self._under_to_mixed(table_name[1:])

    def pythonClassToDBTableReference(self, class_name):
        return self.tableReference(self.pythonClassToDBTable(class_name))

    def tableReference(self, table_name):
        return table_name+"_id"

    def _mixed_to_under(self, name, _re=re.compile(r'[A-Z]+')):
        if name.endswith('ID'):
            return self._mixed_to_under(name[:-2]+"_id")
        name = _re.sub(self._mixed_to_under_sub, name)
        if name.startswith('_'):
            return name[1:]
        return name

    def _mixed_to_under_sub(self, match):
        m = match.group(0).lower()
        if len(m) > 1:
            return '_%s_%s' % (m[:-1], m[-1])
        else:
            return '_%s' % m

    def _under_to_mixed(self, name, _re=re.compile('_.')):
        if name.endswith('_id'):
            return self._under_to_mixed(name[:-3] + "ID")
        return _re.sub(self._under_to_mixed_sub, name)

    def _under_to_mixed_sub(self, match):
        return match.group(0)[1].upper()

    @staticmethod
    def _capword(s):
        return s[0].upper() + s[1:]

    @staticmethod
    def _lowerword(s):
        return s[0].lower() + s[1:]


class SQLObjectMeta(PropertyPublisherMeta):

    @staticmethod
    def _get_attr(attr, bases, dict):
        value = dict.get(attr)
        if value is None:
            for base in bases:
                value = getattr(base, attr, None)
                if value is not None:
                    break
        return value

    def __new__(cls, name, bases, dict):
        if Storm in bases or SQLObjectBase in bases:
            # Do not parse abstract base classes.
            return type.__new__(cls, name, bases, dict)

        style = cls._get_attr("_style", bases, dict)
        if style is None:
            dict["_style"] = style = SQLObjectStyle()

        table_name = cls._get_attr("_table", bases, dict)
        if table_name is None:
            table_name = style.pythonClassToDBTable(name)

        id_name = cls._get_attr("_idName", bases, dict)
        if id_name is None:
            id_name = style.idForTable(table_name)

        # Handle this later to call _parse_orderBy() on the created class.
        default_order = cls._get_attr("_defaultOrder", bases, dict)

        dict["__storm_table__"] = table_name

        attr_to_prop = {}
        for attr, prop in dict.items():
            attr_to_prop[attr] = attr
            if isinstance(prop, ForeignKey):
                db_name = prop.kwargs.get("dbName", attr)
                local_prop_name = style.instanceAttrToIDAttr(attr)
                dict[local_prop_name] = local_prop = Int(db_name)
                dict[attr] = Reference(local_prop,
                                       "%s.<primary key>" % prop.foreignKey)
                attr_to_prop[attr] = local_prop_name
            elif isinstance(prop, PropertyAdapter):
                db_name = prop.dbName or attr
                method_name = prop.alternateMethodName
                if method_name is None and prop.alternateID:
                    method_name = "by" + db_name[0].upper() + db_name[1:]
                if method_name is not None:
                    def func(cls, key, attr=attr):
                        store = cls._get_store()
                        obj = store.find(cls, getattr(cls, attr) == key).one()
                        if obj is None:
                            raise SQLObjectNotFound
                        return obj
                    func.func_name = method_name
                    dict[method_name] = classmethod(func)


        id_type = dict.get("_idType", int)
        id_cls = {int: Int, str: RawStr, unicode: AutoUnicode}[id_type]
        dict[id_name] = id_cls(primary=True)

        # Notice that obj is the class since this is the metaclass.
        obj = super(SQLObjectMeta, cls).__new__(cls, name, bases, dict)

        property_registry = obj._storm_property_registry

        property_registry.add_property(obj, getattr(obj, id_name),
                                       "<primary key>")

        for fake_name, real_name in attr_to_prop.items():
            prop = getattr(obj, real_name)
            if fake_name != real_name:
                property_registry.add_property(obj, prop, fake_name)
            attr_to_prop[fake_name] = prop

        obj._attr_to_prop = attr_to_prop

        if default_order is not None:
            cls_info = get_cls_info(obj)
            cls_info.default_order = obj._parse_orderBy(default_order)

        return obj


class DotQ(object):
    """A descriptor that mimics the SQLObject 'Table.q' syntax"""

    def __get__(self, obj, cls=None):
        return BoundDotQ(cls)


class BoundDotQ(object):

    def __init__(self, cls):
        self._cls = cls

    def __getattr__(self, attr):
        if attr.startswith('__'):
            raise AttributeError(attr)
        elif attr == 'id':
            cls_info = get_cls_info(self._cls)
            return cls_info.primary_key[0]
        else:
            return getattr(self._cls, attr)


class SQLObjectBase(Storm):
    """The root class of all SQLObject-emulating classes in your application.

    The general strategy for using Storm's SQLObject emulation layer
    is to create an application-specific subclass of SQLObjectBase
    (probably named "SQLObject") that provides an implementation of
    _get_store to return an instance of L{storm.store.Store}. It may
    even be implemented as returning a global L{Store} instance. Then
    all database classes should subclass that class.
    """
    __metaclass__ = SQLObjectMeta

    q = DotQ()

    def __init__(self, *args, **kwargs):
        self._get_store().add(self)
        self._create(None, **kwargs)

    def __storm_loaded__(self):
        self._init(None)

    def _init(self, id, *args, **kwargs):
        pass

    def _create(self, _id_, **kwargs):
        self.set(**kwargs)
        self._init(None)

    def set(self, **kwargs):
        for attr, value in kwargs.iteritems():
            setattr(self, attr, value)

    def destroySelf(self):
        Store.of(self).remove(self)

    @staticmethod
    def _get_store():
        raise NotImplementedError("SQLObjectBase._get_store() "
                                  "must be implemented")

    @classmethod
    def delete(cls, id):
        # destroySelf() should be extended to support cascading, so
        # we'll mimic what SQLObject does here, even if more expensive.
        obj = cls.get(id)
        obj.destroySelf()

    @classmethod
    def get(cls, id):
        store = cls._get_store()
        obj = store.get(cls, id)
        if obj is None:
            raise SQLObjectNotFound("Object not found")
        return obj

    @classmethod
    def _parse_orderBy(cls, orderBy):
        result = []
        if not isinstance(orderBy, (tuple, list)):
            orderBy = (orderBy,)
        for item in orderBy:
            if isinstance(item, basestring):
                desc = item.startswith("-")
                if desc:
                    item = item[1:]
                item = cls._attr_to_prop.get(item, item)
                if desc:
                    item = Desc(item)
            result.append(item)
        return tuple(result)

    @classmethod
    def _find(cls, clause=None, clauseTables=None, orderBy=None,
              limit=None, distinct=None, prejoins=_IGNORED,
              prejoinClauseTables=_IGNORED, _by={}):
        store = cls._get_store()
        if clause is None:
            args = ()
        else:
            args = (clause,)
        if clauseTables is not None:
            clauseTables = set(table.lower() for table in clauseTables)
            clauseTables.add(cls.__storm_table__.lower())
            store = store.using(*clauseTables)
        result = store.find(cls, *args, **_by)
        if orderBy is not None:
            result.order_by(*cls._parse_orderBy(orderBy))
        result.config(limit=limit, distinct=distinct)
        return result

    @classmethod
    def select(cls, *args, **kwargs):
        result = cls._find(*args, **kwargs)
        return SQLObjectResultSet(result, cls)

    @classmethod
    def selectBy(cls, orderBy=None, **kwargs):
        result = cls._find(orderBy=orderBy, _by=kwargs)
        return SQLObjectResultSet(result, cls)

    @classmethod
    def selectOne(cls, *args, **kwargs):
        return cls._find(*args, **kwargs).one()

    @classmethod
    def selectOneBy(cls, **kwargs):
        return cls._find(_by=kwargs).one()

    @classmethod
    def selectFirst(cls, *args, **kwargs):
        return cls._find(*args, **kwargs).first()

    @classmethod
    def selectFirstBy(cls, orderBy=None, **kwargs):
        return cls._find(orderBy=orderBy, _by=kwargs).first()

    # Dummy methods.
    def sync(self): pass
    def syncUpdate(self): pass


class SQLObjectResultSet(object):

    def __init__(self, result_set, cls):
        self._result_set = result_set
        self._cls = cls

    def count(self):
        return self._result_set.count()

    def __iter__(self):
        return self._result_set.__iter__()

    def __getitem__(self, index):
        result_set = self._result_set[index]
        if isinstance(index, slice):
            return self.__class__(result_set, self._cls)
        return result_set

    def __nonzero__(self):
        return self._result_set.any() is not None

    def orderBy(self, orderBy):
        result_set = self._result_set.copy()
        result_set.order_by(*self._cls._parse_orderBy(orderBy))
        return self.__class__(result_set, self._cls)

    def limit(self, limit):
        result_set = self._result_set.copy().config(limit=limit)
        return self.__class__(result_set, self._cls)

    def distinct(self):
        result_set = self._result_set.copy().config(distinct=True)
        result_set.order_by() # Remove default order.
        return self.__class__(result_set, self._cls)

    def union(self, otherSelect, unionAll=False, orderBy=None):
        result_set = self._result_set.union(otherSelect._result_set,
                                            all=unionAll)
        result_set.order_by() # Remove default order.
        new = self.__class__(result_set, self._cls)
        if orderBy is not None:
            return new.orderBy(orderBy)
        return new

    def except_(self, otherSelect, exceptAll=False, orderBy=None):
        result_set = self._result_set.difference(otherSelect._result_set,
                                                 all=exceptAll)
        result_set.order_by() # Remove default order.
        new = self.__class__(result_set, self._cls)
        if orderBy is not None:
            return new.orderBy(orderBy)
        return new

    def intersect(self, otherSelect, intersectAll=False, orderBy=None):
        result_set = self._result_set.intersection(otherSelect._result_set,
                                                   all=intersectAll)
        new = self.__class__(result_set, self._cls)
        if orderBy is not None:
            return new.orderBy(orderBy)
        return new

    def prejoin(self, prejoins):
        return self

    def prejoinClauseTables(self, prejoinClauseTables):
        return self


class PropertyAdapter(object):

    _kwargs = {}

    def __init__(self, dbName=None, notNull=False, default=Undef,
                 alternateID=None, unique=_IGNORED, name=_IGNORED,
                 alternateMethodName=None, length=_IGNORED, immutable=None,
                 prejoins=_IGNORED):
        if default is None and notNull:
            raise RuntimeError("Can't use default=None and notNull=True")

        self.dbName = dbName
        self.alternateID = alternateID
        self.alternateMethodName = alternateMethodName

        # XXX Implement handler for:
        #
        #   - immutable (causes setting the attribute to fail)
        #
        # XXX Implement tests for ignored parameters:
        #
        #   - unique (for tablebuilder)
        #   - length (for tablebuilder for StringCol)
        #   - name (for _columns stuff)
        #   - prejoins

        if callable(default):
            default_factory = default
            default = Undef
        else:
            default_factory = Undef
        super(PropertyAdapter, self).__init__(dbName, allow_none=not notNull,
                                              default_factory=default_factory,
                                              default=default, **self._kwargs)


class AutoUnicodeVariable(Variable):
    """Unlike UnicodeVariable, this will try to convert str to unicode."""

    def parse_set(self, value, from_db):
        if not isinstance(value, basestring):
            raise TypeError("Expected basestring, found %s" % repr(type(value)))
        return unicode(value)

class AutoUnicode(SimpleProperty):
    variable_class = AutoUnicodeVariable


class StringCol(PropertyAdapter, AutoUnicode):
    pass

class IntCol(PropertyAdapter, Int):
    pass

class BoolCol(PropertyAdapter, Bool):
    pass

class FloatCol(PropertyAdapter, Float):
    pass

class UtcDateTimeCol(PropertyAdapter, DateTime):
    _kwargs = {"tzinfo": tzutc()}

class DateCol(PropertyAdapter, Date):
    pass

class IntervalCol(PropertyAdapter, TimeDelta):
    pass


class ForeignKey(object):

    def __init__(self, foreignKey, **kwargs):
        self.foreignKey = foreignKey
        self.kwargs = kwargs


class SQLMultipleJoin(ReferenceSet):

    def __init__(self, otherClass=None, joinColumn=None,
                 intermediateTable=None, otherColumn=None, orderBy=None,
                 prejoins=_IGNORED):
        if intermediateTable:
            args = ("<primary key>",
                    "%s.%s" % (intermediateTable, joinColumn),
                    "%s.%s" % (intermediateTable, otherColumn),
                    "%s.<primary key>" % otherClass)
        else:
            args = ("<primary key>", "%s.%s" % (otherClass, joinColumn))
        ReferenceSet.__init__(self, *args)
        self._orderBy = orderBy

    def __get__(self, obj, cls=None):
        if obj is None:
            return self
        bound_reference_set = ReferenceSet.__get__(self, obj)
        target_cls = bound_reference_set._target_cls
        result_set = bound_reference_set.find()
        if self._orderBy:
            result_set.order_by(*target_cls._parse_orderBy(self._orderBy))
        return SQLObjectResultSet(result_set, target_cls)

SQLRelatedJoin = SQLMultipleJoin


class CONTAINSSTRING(Like):

    def __init__(self, expr, string):
        string = string.replace("!", "!!") \
                       .replace("_", "!_") \
                       .replace("%", "!%")
        Like.__init__(self, expr, "%"+string+"%", SQLRaw("'!'"))
