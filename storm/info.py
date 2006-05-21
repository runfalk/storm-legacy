from storm.expr import Column, Undef


__all__ = ["get_obj_info", "get_cls_info", "get_info",
           "ClassInfo", "ObjectInfo"]


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
            column = getattr(cls, attr)
            if isinstance(column, Column):
                pairs.append((attr, column))
        pairs.sort()

        self.cls = cls
        self.attributes = tuple(pair[0] for pair in pairs)
        self.columns = tuple(pair[1] for pair in pairs)
        self.table = __table__[0]

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

    def __init__(self, obj):
        self.obj = obj
        self._values = {}
        self._notify_change = None
        self._saved_values = None
        self._saved_obj_dict = None
        self._saved_self = None

    def has_value(self, name):
        return name in self._values

    def get_value(self, name, default=None):
        return self._values.get(name, default)

    def set_value(self, name, value):
        old_value = self._values.get(name, Undef)
        self._values[name] = value
        if self._notify_change is not None and old_value != value:
            self._notify_change(self, name, old_value, value)

    def del_value(self, name):
        old_value = self._values.pop(name, Undef)
        if self._notify_change is not None and old_value != Undef:
            self._notify_change(self, name, old_value, Undef)

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
        for name in old_values:
            new_value = new_values.get(name, Undef)
            if new_value is Undef:
                changes[name] = Undef
            elif old_values[name] != new_value:
                changes[name] = new_value
        for name in new_values:
            if name not in old_values:
                changes[name] = new_values[name]
        return changes

    def set_change_notification(self, callback=None):
        self._notify_change = callback
