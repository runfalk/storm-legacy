from storm.store import Store
from storm.expr import Undef
from storm.info import *


class Reference(object):

    def __init__(self, cls, foreign_key):
        cls_info = get_cls_info(cls)
        if type(foreign_key) not in (tuple, list):
            foreign_key = (foreign_key,)

        assert len(foreign_key) == len(cls_info.primary_key)

        self._cls = cls
        self._foreign_key = foreign_key
        self._primary_key = cls_info.primary_key
        self._pairs = zip(self._foreign_key, self._primary_key)

    def __get__(self, obj, cls=None):
        if obj is None:
            return self

        foreign_values = tuple(prop.__get__(obj, default=Undef)
                               for prop in self._foreign_key)
        if Undef in foreign_values:
            references = get_obj_info(obj).get("references")
            if references is None:
                return None
            return references.get(self)

        store = Store.of(obj)
        if store is None:
            return None

        return store.get(self._cls, foreign_values)

    def __set__(self, obj, other):
        track_changes = False
        for foreign_prop, primary_prop in self._pairs:
            value = primary_prop.__get__(other, default=Undef)
            if value is Undef:
                track_changes = True
            else:
                foreign_prop.__set__(obj, value)

        obj_info = get_obj_info(obj)

        references = obj_info.get("references")

        if references is None:
            references = obj_info.setdefault("references", {})
        else:
            old_other = references.get(self)
            if old_other is not None:
                old_other_info = get_obj_info(old_other)
                old_other_info.unhook("changed", self._other_changed, obj)
                old_other_info.unhook("flushed", self._other_flushed, obj)
                obj_info.unhook("changed", self._obj_changed, old_other)

        if track_changes:
            other_info = get_obj_info(other)
            other_info.hook("changed", self._other_changed, obj)
            other_info.hook("flushed", self._other_flushed, obj)
            obj_info.hook("changed", self._obj_changed, other)

        references[self] = other

    def _obj_changed(self, obj_info, name, old_value, new_value, other):
        """Hook for the parent object when tracking changes.

        This hook ensures that if the parent object has an attribute which
        is part of the foreign key changed by hand in a way that diverges
        from the child object, it stops tracking changes in the child.

        This is only used when the child object has an incomplete key.
        """
        obj = obj_info.obj
        for foreign_prop, primary_prop in self._pairs:
            column = foreign_prop.__get__(None, obj.__class__)
            if column.name == name:
                other_obj_info = get_obj_info(other)
                if new_value != other_obj_info.get_value(primary_prop.name,
                                                         Undef):
                    other_obj_info.unhook("changed", self._other_changed, obj)
                    other_obj_info.unhook("flushed", self._other_flushed, obj)
                    del obj_info["references"][self]
                    return False
                break

    def _other_changed(self, obj_info, name, old_value, new_value, obj):
        """Hook for the child object when tracking changes.

        This hook ensures that if the parent will keep track of changes
        done in the child object, either manually or at flushing time.

        This is only used when the child object has an incomplete key.
        """
        for foreign_prop, primary_prop in self._pairs:
            if primary_prop.name == name:
                foreign_prop.__set__(obj, new_value)
                break

    def _other_flushed(self, obj_info, obj):
        """Hook for the child object when tracking changes.

        When flushing, unlink the child object.

        This is only used when the child object has an incomplete key.
        """
        obj_info.unhook("changed", self._other_changed, obj)
        get_obj_info(obj).unhook("changed", self._obj_changed, obj_info.obj)
        return False
