from datetime import datetime

from storm.info import get_obj_info
from storm.expr import Column, Undef


__all__ = ["Property", "Bool", "Int", "Float", "Str", "Unicode", "DateTime"]


class PropertyColumn(Column):

    def __init__(self, prop, name=Undef, table=Undef):
        Column.__init__(self, name, table)
        self._prop = prop

    def __get__(self, obj, cls=None):
        return self._prop.__get__(obj, cls)

    def __set__(self, obj, value):
        self._prop.__set__(obj, value)

    def __delete__(self, obj):
        self._prop.__delete__(obj)


class Property(object):

    def __init__(self, name=None):
        self._name = name
        self._columns = {}

    def __get__(self, obj, cls=None):
        if obj is None:
            return self._get_column(cls)
        if self._name is None:
            self._detect_name(obj.__class__)
        return get_obj_info(obj).get_value(self._name)

    def __set__(self, obj, value):
        if self._name is None:
            self._detect_name(obj.__class__)
        get_obj_info(obj).set_value(self._name, value)

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
            column = PropertyColumn(self, self._name, cls.__table__[0])
            self._columns[cls] = column
        return column


class Bool(Property):

    def __set__(self, obj, value):
        if self._name is None:
            self._detect_name(obj.__class__)
        get_obj_info(obj).set_value(self._name, bool(value))

class Int(Property):

    def __set__(self, obj, value):
        if self._name is None:
            self._detect_name(obj.__class__)
        get_obj_info(obj).set_value(self._name, int(value))

class Float(Property):

    def __set__(self, obj, value):
        if self._name is None:
            self._detect_name(obj.__class__)
        get_obj_info(obj).set_value(self._name, float(value))

class Str(Property):

    def __set__(self, obj, value):
        if self._name is None:
            self._detect_name(obj.__class__)
        get_obj_info(obj).set_value(self._name, str(value))

class Unicode(Property):

    def __set__(self, obj, value):
        if self._name is None:
            self._detect_name(obj.__class__)
        get_obj_info(obj).set_value(self._name, unicode(value))

class DateTime(Property):

    def __set__(self, obj, value):
        if self._name is None:
            self._detect_name(obj.__class__)
        if type(value) in (int, long, float):
            value = datetime.utcfromtimestamp(value)
        elif not isinstance(value, datetime):
            raise TypeError("Expected datetime, found %s" % repr(value))
        get_obj_info(obj).set_value(self._name, value)
