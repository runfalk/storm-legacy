from datetime import datetime

from storm.expr import Column


class ClassInfo(object):

    def __new__(class_, cls):
        try:
            return cls.__class_info
        except AttributeError:
            pass

        __table__ = getattr(cls, "__table__", ())
        if len(__table__) != 2:
            raise RuntimeError("%s.__table__ must be (<table name>, "
                               "<primary key(s)>) tuple." % repr(cls))

        pairs = []
        names = {}
        for name in dir(cls):
            attr = getattr(cls, name)
            if isinstance(attr, Property):
                pairs.append((name, attr))
                names[attr.name] = attr
        pairs.sort()

        info = object.__new__(class_)
        info.cls = cls
        info.prop_insts = tuple(pair[1] for pair in pairs)
        info.prop_names = tuple(prop.name for prop in info.prop_insts)
        info.attr_names = tuple(pair[0] for pair in pairs)
        info.attr_dict = dict(pairs)
        info.columns = tuple(Column(name) for name in info.prop_names)
        info.table = __table__[0]
        info.tables = (__table__[0],)
        if type(__table__[1]) in (list, tuple):
            info.primary_key = tuple(names[name] for name in __table__[1])
        else:
            info.primary_key = (names[__table__[1]],)

        cls.__class_info = info
        return info


class ObjectInfo(object):

    def __new__(class_, obj):
        info = obj.__dict__.get("__object_info")
        if info is not None:
            return info
        info = object.__new__(class_)
        info.obj = obj
        info._values = {}
        info._states = []
        return obj.__dict__.setdefault("__object_info", info)

    def push_state(self):
        self._states.append((self._values.copy(), self.obj.__dict__.copy()))

    def pop_state(self):
        self._values, self.obj.__dict__ = self._states.pop(-1)

    def check_changed(self, props=False, attrs=False):
        if not self._states:
            return None
        last_props, last_attrs = self._states[-1]
        return (props and last_props != self._values or
                attrs and last_attrs != self.obj.__dict__)

    def set_prop(self, prop, value):
        if value is None:
            self._values.pop(prop, None)
        else:
            self._values[prop] = value

    def get_prop(self, prop):
        return self._values.get(prop)


class Property(Column):

    def __init__(self, name=None):
        Column.__init__(self, name)

    def __get__(self, obj, cls=None):
        if obj is None:
            if self.table is None:
                self.table = getattr(cls, "__table__", (None,))[0]
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
