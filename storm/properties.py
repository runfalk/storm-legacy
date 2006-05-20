from datetime import datetime

from storm.expr import Column, Undef


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

    def __init__(self, cls):
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

        self.cls = cls
        self.attributes = tuple(pair[0] for pair in pairs)
        self.properties = tuple(pair[1] for pair in pairs)
        self.table = __table__[0]

        name_positions = dict((prop.name, i)
                              for i, prop in enumerate(self.properties))

        primary_key_names = __table__[1]
        if type(primary_key_names) not in (list, tuple):
            primary_key_names = (primary_key_names,)

        self.primary_key_pos = tuple(name_positions[name]
                                     for name in primary_key_names)
        self.primary_key = tuple(self.properties[i]
                                 for i in self.primary_key_pos)


class ObjectInfo(dict):

    def __init__(self, obj):
        self.obj = obj
        self._values = {}
        self._notify_change = None
        self._saved_values = None
        self._saved_obj_dict = None
        self._saved_self = None

    def has_prop(self, prop):
        return prop in self._values

    def get_prop(self, prop, accept_undef=False):
        if accept_undef:
            return self._values.get(prop, Undef)
        else:
            return self._values.get(prop)

    def set_prop(self, prop, value):
        old_value = self._values.get(prop, Undef)
        self._values[prop] = value
        if self._notify_change is not None and old_value != value:
            self._notify_change(self.obj, self, old_value, value)

    def del_prop(self, prop):
        old_value = self._values.pop(prop, Undef)
        if self._notify_change is not None and old_value != Undef:
            self._notify_change(self.obj, self, old_value, Undef)

    def save(self):
        self._saved_values = self._values.copy()
        self._saved_obj_dict = self.obj.__dict__.copy()
        self._saved_self = self.copy()

    def save_attributes(self):
        self._saved_obj_dict = self.obj.__dict__.copy()

    def restore(self):
        self._values = self._saved_values.copy()
        self.obj.__dict__ = self._saved_obj_dict.copy()
        self.clear()
        self.update(self._saved_self)

    def check_changed(self):
        return self._saved_values != self._values

    def get_changes(self):
        changes = {}
        old_values = self._saved_values
        new_values = self._values
        for prop in old_values:
            new_value = new_values.get(prop, Undef)
            if new_value is Undef:
                changes[prop] = Undef
            elif old_values[prop] != new_value:
                changes[prop] = new_value
        for prop in new_values:
            if prop not in old_values:
                changes[prop] = new_values[prop]
        return changes

    def set_change_notification(self, callback=None):
        self._notify_change = callback


class Property(Column):

    def __init__(self, name=Undef):
        Column.__init__(self, name)
        self._cls = None
        self._attr = None

    def __get__(self, obj, cls=None, accept_undef=False):
        self = self._for(cls or obj.__class__)
        if obj is None:
            return self
        return get_obj_info(obj).get_prop(self, accept_undef)

    def __set__(self, obj, value):
        get_obj_info(obj).set_prop(self._for(obj.__class__), value)

    def __delete__(self, obj):
        get_obj_info(obj).del_prop(self._for(obj.__class__))

    def _find_real_owner(self, used_cls):
        self_id = id(self)
        for cls in used_cls.__mro__:
            for attr, prop in cls.__dict__.iteritems():
                if id(prop) == self_id:
                    break
            else:
                continue
            break
        else:
            raise RuntimeError("Property used in an unknown class")

        self._cls = cls
        self._attr = attr

    def _copy_to(self, cls):
        prop = self.__class__(self.name)
        setattr(cls, self._attr, prop)
        return prop
    
    def _for(self, cls):
        if self._cls is None:
            self._find_real_owner(cls)
            if self.name is Undef:
                self.name = self._attr
            if self.table is Undef:
                self.table = self._cls.__table__[0]
        if cls is not self._cls:
            return self._copy_to(cls)._for(cls)
        return self


class Bool(Property):

    def __set__(self, obj, value):
        get_obj_info(obj).set_prop(self._for(obj.__class__), bool(value))

class Int(Property):

    def __set__(self, obj, value):
        get_obj_info(obj).set_prop(self._for(obj.__class__), int(value))

class Float(Property):

    def __set__(self, obj, value):
        get_obj_info(obj).set_prop(self._for(obj.__class__), float(value))

class Str(Property):

    def __set__(self, obj, value):
        get_obj_info(obj).set_prop(self._for(obj.__class__), str(value))

class Unicode(Property):

    def __set__(self, obj, value):
        get_obj_info(obj).set_prop(self._for(obj.__class__), unicode(value))

class DateTime(Property):

    def __set__(self, obj, value):
        self = self._for(obj.__class__)
        if type(value) in (int, long, float):
            value = datetime.utcfromtimestamp(value)
        elif not isinstance(value, datetime):
            raise TypeError("Expected datetime, found %s" % repr(value))
        get_obj_info(obj).set_prop(self, value)
