from datetime import datetime

from storm.expr import Column, Undef


class ClassInfo(object):

    def __new__(class_, cls):
        info = cls.__dict__.get("__class_info")
        if info is not None:
            return info

        __table__ = getattr(cls, "__table__", ())
        if len(__table__) != 2:
            raise RuntimeError("%s.__table__ must be (<table name>, "
                               "<primary key(s)>) tuple." % repr(cls))

        pairs = []
        for attr in dir(cls):
            prop = getattr(cls, attr)
            if isinstance(prop, Property):
                pairs.append((attr, prop))
        pairs.sort()

        info = object.__new__(class_)
        info.cls = cls
        info.attributes = tuple(pair[0] for pair in pairs)
        info.properties = tuple(pair[1] for pair in pairs)
        info.table = __table__[0]

        name_positions = dict((prop.name, i)
                              for i, prop in enumerate(info.properties))

        primary_key_names = __table__[1]
        if type(primary_key_names) not in (list, tuple):
            primary_key_names = (primary_key_names,)

        info.primary_key_pos = tuple(name_positions[name]
                                     for name in primary_key_names)
        info.primary_key = tuple(info.properties[i]
                                 for i in info.primary_key_pos)

        setattr(cls, "__class_info", info)
        return info


class ObjectInfo(object):

    def __new__(class_, obj):
        info = obj.__dict__.get("__object_info")
        if info is not None:
            return info
        info = object.__new__(class_)
        info.obj = obj
        info._saved = None
        info._values = {}
        info._notify_change = None
        return obj.__dict__.setdefault("__object_info", info)

    def set(self, prop, value):
        if value is None:
            old_value = self._values.pop(prop, None)
        else:
            old_value = self._values.get(prop)
            self._values[prop] = value
        if self._notify_change is not None and old_value != value:
            self._notify_change(self.obj, self, old_value, value)

    def get(self, prop):
        return self._values.get(prop)

    def save(self):
        self._saved = self._values.copy(), self.obj.__dict__.copy()

    def restore(self):
        self._values, self.obj.__dict__ = self._saved

    def check_changed(self):
        return self._saved[0] != self._values

    def get_changes(self):
        changes = {}
        old_values = self._saved[0]
        new_values = self._values
        for prop in old_values:
            new_value = new_values.get(prop)
            if new_value is None:
                changes[prop] = None
            elif old_values[prop] != new_value:
                changes[prop] = new_value
        for prop in new_values:
            if prop not in old_values:
                changes[prop] = new_values[prop]
        return changes

    def set_change_notification(self, callback=None):
        self._notify_change = callback


class Property(Column):

    def __init__(self, name=None):
        Column.__init__(self, name)

    def __get__(self, obj, cls=None):
        if obj is None:
            if self.table is Undef:
                self.table = getattr(cls, "__table__", (None,))[0]
            if self.name is None:
                self.name = object()
                for name in cls.__dict__:
                    if getattr(cls, name) is self:
                        self.name = name
                        break
            return self
        return ObjectInfo(obj).get(self)

    def __set__(self, obj, value):
        ObjectInfo(obj).set(self, value)


class Bool(Property):

    def __set__(self, obj, value):
        ObjectInfo(obj).set(self, bool(value))

class Int(Property):

    def __set__(self, obj, value):
        ObjectInfo(obj).set(self, int(value))

class Float(Property):

    def __set__(self, obj, value):
        ObjectInfo(obj).set(self, float(value))

class Str(Property):

    def __set__(self, obj, value):
        ObjectInfo(obj).set(self, str(value))

class Unicode(Property):

    def __set__(self, obj, value):
        ObjectInfo(obj).set(self, unicode(value))

class DateTime(Property):

    def __set__(self, obj, value):
        if type(value) in (int, long, float):
            value = datetime.utcfromtimestamp(value)
        elif not isinstance(value, datetime):
            raise TypeError("Expected datetime, found %s" % repr(value))
        ObjectInfo(obj).set(self, value)
