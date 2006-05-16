from datetime import datetime

from storm.expr import Column


class ClassInfo(object):

    def __new__(class_, cls):
        try:
            return cls.__class_info
        except AttributeError:
            pass

        info = cls.__class_info = object.__new__(class_)

        info.properties = []
        for name in cls.__dict__:
            attr = getattr(cls, name)
            if isinstance(attr, Property):
                info.properties.append((name, attr))
        info.properties.sort()

        return cls.__class_info # Reduce race condition.


class ObjectInfo(object):

    def __new__(class_, obj):
        info = obj.__dict__.get("__object_info")
        if info is not None:
            return info
        info = object.__new__(class_)
        info._obj = obj
        info._props = {}
        info._states = []
        return obj.__dict__.setdefault("__object_info", info)

    def push_state(self):
        self._states.append((self._props.copy(), self._obj.__dict__.copy()))

    def pop_state(self):
        self._props, self._obj.__dict__ = self._states.pop(-1)

    def check_changed(self, props=False, attrs=False):
        if not self._states:
            return None
        last_props, last_attrs = self._states[-1]
        return (props and last_props != self._props or
                attrs and last_attrs != self._obj.__dict__)

    def set_prop(self, prop, value):
        if value is None:
            self._props.pop(prop, None)
        else:
            self._props[prop] = value

    def get_prop(self, prop):
        return self._props.get(prop)


class Property(Column):

    def __init__(self, name=None):
        Column.__init__(self, name)

    def __get__(self, obj, cls=None):
        if obj is None:
            if self.table is None:
                self.table = getattr(cls, "__table__", None)
            if self.name is None:
                self.name = object()
                for name in cls.__dict__:
                    if getattr(cls, name) is self:
                        self.name = name
                        break
            return self
        return ObjectInfo(obj).get_prop(self)

    def __set__(self, obj, value):
        ObjectInfo(obj).set_prop(self, value)


class Bool(Property):

    def __set__(self, obj, value):
        ObjectInfo(obj).set_prop(self, bool(value))

class Int(Property):

    def __set__(self, obj, value):
        ObjectInfo(obj).set_prop(self, int(value))

class Float(Property):

    def __set__(self, obj, value):
        ObjectInfo(obj).set_prop(self, float(value))

class Str(Property):

    def __set__(self, obj, value):
        ObjectInfo(obj).set_prop(self, str(value))

class Unicode(Property):

    def __set__(self, obj, value):
        ObjectInfo(obj).set_prop(self, unicode(value))

class DateTime(Property):

    def __set__(self, obj, value):
        if type(value) in (int, long, float):
            value = datetime.utcfromtimestamp(value)
        elif not isinstance(value, datetime):
            raise TypeError("Expected datetime, found %s" % repr(value))
        ObjectInfo(obj).set_prop(self, value)
