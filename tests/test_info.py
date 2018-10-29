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
import gc
import pytest

from storm.compat import add_metaclass, iter_zip
from storm.exceptions import ClassInfoError
from storm.properties import Property
from storm.variables import Variable
from storm.expr import Undef, Select, compile
from storm.info import *

from tests.helper import TestHelper


@pytest.fixture
def Class():
    # We create the class here since it gets modified by get_cls_info
    class Class(object):
        __storm_table__ = "table"
        prop1 = Property("column1", primary=True)
        prop2 = Property("column2")
    return Class


@pytest.fixture
def cls_info(Class):
    return get_cls_info(Class)


@pytest.fixture
def obj(Class):
    return Class()


@pytest.fixture
def obj_info(obj):
    return get_obj_info(obj)


@pytest.fixture
def variable1(Class, obj_info):
    return obj_info.variables[Class.prop1]


@pytest.fixture
def variable2(Class, obj_info):
    return obj_info.variables[Class.prop2]



def test_get_cls_info(Class):
    cls_info = get_cls_info(Class)
    assert isinstance(cls_info, ClassInfo)
    assert cls_info is get_cls_info(Class)


def test_get_obj_info(obj):
    obj_info = get_obj_info(obj)
    assert isinstance(obj_info, ObjectInfo)
    assert obj_info is get_obj_info(obj)


def test_get_obj_info_on_obj_info(obj):
    obj_info = get_obj_info(obj)
    assert get_obj_info(obj_info) is obj_info


def test_set_obj_info(obj):
    obj_info1 = get_obj_info(obj)
    obj_info2 = ObjectInfo(obj)
    assert get_obj_info(obj) == obj_info1
    set_obj_info(obj, obj_info2)
    assert get_obj_info(obj) == obj_info2


def test_cls_info_invalid_class():
    class Class(object): pass
    with pytest.raises(ClassInfoError):
        ClassInfo(Class)


def test_cls_info_cls(Class, cls_info):
    assert cls_info.cls is Class


def test_cls_info_columns(Class, cls_info):
    assert cls_info.columns == (Class.prop1, Class.prop2)


def test_cls_info_table(cls_info):
    assert cls_info.table.name == "table"


def test_cls_info_primary_key(Class, cls_info):
    # Can't use == for props.
    assert cls_info.primary_key[0] is Class.prop1
    assert len(cls_info.primary_key) == 1


def test_cls_info_primary_key_with_attribute(Class, cls_info):
    class SubClass(Class):
        __storm_primary__ = "prop2"

    assert get_cls_info(SubClass).primary_key[0] is SubClass.prop2
    assert len(cls_info.primary_key) == 1


def test_cls_info_primary_key_composed():
    class Class(object):
        __storm_table__ = "table"
        prop1 = Property("column1", primary=2)
        prop2 = Property("column2", primary=1)
    cls_info = ClassInfo(Class)

    # Can't use == for props, since they're columns.
    assert cls_info.primary_key[0] is Class.prop2
    assert cls_info.primary_key[1] is Class.prop1
    assert len(cls_info.primary_key) == 2


def test_cls_info_primary_key_composed_with_attribute():
    class Class(object):
        __storm_table__ = "table"
        __storm_primary__ = "prop2", "prop1"
        # Define primary=True to ensure that the attribute
        # has precedence.
        prop1 = Property("column1", primary=True)
        prop2 = Property("column2")
    cls_info = ClassInfo(Class)

    # Can't use == for props, since they're columns.
    assert cls_info.primary_key[0] is Class.prop2
    assert cls_info.primary_key[1] is Class.prop1
    assert len(cls_info.primary_key) == 2


def test_cls_info_primary_key_composed_duplicated():
    class Class(object):
        __storm_table__ = "table"
        prop1 = Property("column1", primary=True)
        prop2 = Property("column2", primary=True)

    with pytest.raises(ClassInfoError):
        ClassInfo(Class)


def test_cls_info_primary_key_missing():
    class Class(object):
        __storm_table__ = "table"
        prop1 = Property("column1")
        prop2 = Property("column2")

    with pytest.raises(ClassInfoError):
        ClassInfo(Class)


def test_cls_info_primary_key_attribute_missing():
    class Class(object):
        __storm_table__ = "table"
        __storm_primary__ = ()
        prop1 = Property("column1", primary=True)
        prop2 = Property("column2")

    with pytest.raises(ClassInfoError):
        ClassInfo(Class)


def test_cls_info_primary_key_pos():
    class Class(object):
        __storm_table__ = "table"
        prop1 = Property("column1", primary=2)
        prop2 = Property("column2")
        prop3 = Property("column3", primary=1)
    cls_info = ClassInfo(Class)
    assert cls_info.primary_key_pos == (2, 0)


class ObjectInfoTest(TestHelper):

    def setUp():
        TestHelper.setUp(self)
        class Class(object):
            __storm_table__ = "table"
            prop1 = Property("column1", primary=True)
            prop2 = Property("column2")
        self.Class = Class
        self.obj = Class()
        self.obj_info = get_obj_info(self.obj)
        self.cls_info = get_cls_info(Class)
        self.variable1 = self.obj_info.variables[Class.prop1]
        self.variable2 = self.obj_info.variables[Class.prop2]


def test_obj_info_hashing(obj_info):
    assert hash(obj_info) == hash(obj_info)


def test_obj_info_equals(Class, obj_info):
    obj_info1 = obj_info
    obj_info2 = get_obj_info(Class())
    assert not obj_info1 == obj_info2


def test_obj_info_not_equals(Class, obj_info):
    obj_info1 = obj_info
    obj_info2 = get_obj_info(Class())
    assert obj_info1 != obj_info2


def test_obj_info_dict_subclass(obj_info):
    assert isinstance(obj_info, dict)


def test_obj_info_variables(cls_info, obj_info):
    assert isinstance(obj_info.variables, dict)

    for column in cls_info.columns:
        variable = obj_info.variables.get(column)
        assert isinstance(variable, Variable)
        assert variable.column is column

    assert len(obj_info.variables) == len(cls_info.columns)


def test_obj_info_variable_has_validator_object_factory():
    args = []
    def validator(obj, attr, value):
        args.append((obj, attr, value))
    class Class(object):
        __storm_table__ = "table"
        prop = Property(primary=True,
                        variable_kwargs={"validator": validator})

    obj = Class()
    get_obj_info(obj).variables[Class.prop].set(123)

    assert args == [(obj, "prop", 123)]


def test_obj_info_primary_vars(cls_info, obj_info):
    assert isinstance(obj_info.primary_vars, tuple)
    for column, variable in iter_zip(cls_info.primary_key,
                                     obj_info.primary_vars):
        assert obj_info.variables.get(column) == variable

    assert len(obj_info.primary_vars) == len(cls_info.primary_key)


def test_obj_info_checkpoint(obj, obj_info, variable1):
    obj.prop1 = 10
    obj_info.checkpoint()
    assert obj.prop1 == 10
    assert variable1.has_changed() == False
    obj.prop1 = 20
    assert obj.prop1 == 20
    assert variable1.has_changed() == True
    obj_info.checkpoint()
    assert obj.prop1 == 20
    assert variable1.has_changed() == False
    obj.prop1 = 20
    assert obj.prop1 == 20
    assert variable1.has_changed() == False


def test_obj_info_add_change_notification(obj, obj_info, variable1, variable2):
    changes1 = []
    changes2 = []
    def object_changed1(obj_info, variable, old_value, new_value, fromdb):
        changes1.append((1, obj_info, variable, old_value, new_value, fromdb))
    def object_changed2(obj_info, variable, old_value, new_value, fromdb):
        changes2.append((2, obj_info, variable, old_value, new_value, fromdb))

    obj_info.checkpoint()
    obj_info.event.hook("changed", object_changed1)
    obj_info.event.hook("changed", object_changed2)

    obj.prop2 = 10
    obj.prop1 = 20

    assert changes1 == [
        (1, obj_info, variable2, Undef, 10, False),
        (1, obj_info, variable1, Undef, 20, False),
    ]
    assert changes2 == [
        (2, obj_info, variable2, Undef, 10, False),
        (2, obj_info, variable1, Undef, 20, False),
    ]

    del changes1[:]
    del changes2[:]

    obj.prop1 = None
    obj.prop2 = None

    assert changes1 == [
        (1, obj_info, variable1, 20, None, False),
        (1, obj_info, variable2, 10, None, False),
    ]
    assert changes2 == [
        (2, obj_info, variable1, 20, None, False),
        (2, obj_info, variable2, 10, None, False),
    ]

    del changes1[:]
    del changes2[:]

    del obj.prop1
    del obj.prop2

    assert changes1 == [
        (1, obj_info, variable1, None, Undef, False),
        (1, obj_info, variable2, None, Undef, False),
    ]
    assert changes2 == [
        (2, obj_info, variable1, None, Undef, False),
        (2, obj_info, variable2, None, Undef, False),
    ]


def test_obj_info_add_change_notification_with_arg(obj, obj_info, variable1, variable2):
    changes1 = []
    changes2 = []
    def object_changed1(obj_info, variable, old_value, new_value, fromdb, arg):
        changes1.append(
            (1, obj_info, variable, old_value, new_value, fromdb, arg))
    def object_changed2(obj_info, variable, old_value, new_value, fromdb, arg):
        changes2.append(
            (2, obj_info, variable, old_value, new_value, fromdb, arg))

    obj_info.checkpoint()

    arg = object()
    obj_info.event.hook("changed", object_changed1, arg)
    obj_info.event.hook("changed", object_changed2, arg)

    obj.prop2 = 10
    obj.prop1 = 20

    assert changes1 == [
        (1, obj_info, variable2, Undef, 10, False, arg),
        (1, obj_info, variable1, Undef, 20, False, arg),
    ]
    assert changes2 == [
        (2, obj_info, variable2, Undef, 10, False, arg),
        (2, obj_info, variable1, Undef, 20, False, arg),
    ]

    del changes1[:]
    del changes2[:]

    obj.prop1 = None
    obj.prop2 = None

    assert changes1 == [
        (1, obj_info, variable1, 20, None, False, arg),
        (1, obj_info, variable2, 10, None, False, arg),
    ]
    assert changes2 == [
        (2, obj_info, variable1, 20, None, False, arg),
        (2, obj_info, variable2, 10, None, False, arg),
    ]

    del changes1[:]
    del changes2[:]

    del obj.prop1
    del obj.prop2

    assert changes1 == [
        (1, obj_info, variable1, None, Undef, False, arg),
        (1, obj_info, variable2, None, Undef, False, arg),
    ]
    assert changes2 == [
        (2, obj_info, variable1, None, Undef, False, arg),
        (2, obj_info, variable2, None, Undef, False, arg),
    ]


def test_obj_info_remove_change_notification(obj, obj_info, variable1, variable2):
    changes1 = []
    changes2 = []
    def object_changed1(obj_info, variable, old_value, new_value, fromdb):
        changes1.append((1, obj_info, variable, old_value, new_value, fromdb))
    def object_changed2(obj_info, variable, old_value, new_value, fromdb):
        changes2.append((2, obj_info, variable, old_value, new_value, fromdb))

    obj_info.checkpoint()

    obj_info.event.hook("changed", object_changed1)
    obj_info.event.hook("changed", object_changed2)
    obj_info.event.unhook("changed", object_changed1)

    obj.prop2 = 20
    obj.prop1 = 10

    assert changes1 == []
    assert changes2 == [
        (2, obj_info, variable2, Undef, 20, False),
        (2, obj_info, variable1, Undef, 10, False),
    ]


def test_obj_info_remove_change_notification_with_arg(obj, obj_info, variable1, variable2):
    changes1 = []
    changes2 = []
    def object_changed1(obj_info, variable, old_value, new_value, fromdb, arg):
        changes1.append(
            (1, obj_info, variable, old_value, new_value, fromdb, arg))
    def object_changed2(obj_info, variable, old_value, new_value, fromdb, arg):
        changes2.append(
            (2, obj_info, variable, old_value, new_value, fromdb, arg))

    obj_info.checkpoint()

    arg = object()
    obj_info.event.hook("changed", object_changed1, arg)
    obj_info.event.hook("changed", object_changed2, arg)
    obj_info.event.unhook("changed", object_changed1, arg)

    obj.prop2 = 20
    obj.prop1 = 10

    assert changes1 == []
    assert changes2 == [
        (2, obj_info, variable2, Undef, 20, False, arg),
        (2, obj_info, variable1, Undef, 10, False, arg),
    ]


def test_obj_info_auto_remove_change_notification(obj, obj_info, variable1, variable2):
    changes1 = []
    changes2 = []
    def object_changed1(obj_info, variable, old_value, new_value, fromdb):
        changes1.append((1, obj_info, variable, old_value, new_value, fromdb))
        return False
    def object_changed2(obj_info, variable, old_value, new_value, fromdb):
        changes2.append((2, obj_info, variable, old_value, new_value, fromdb))
        return False

    obj_info.checkpoint()

    obj_info.event.hook("changed", object_changed1)
    obj_info.event.hook("changed", object_changed2)

    obj.prop2 = 20
    obj.prop1 = 10

    assert changes1 == [(1, obj_info, variable2, Undef, 20, False)]
    assert changes2 == [(2, obj_info, variable2, Undef, 20, False)]


def test_obj_info_auto_remove_change_notification_with_arg(obj, obj_info, variable1, variable2):
    changes1 = []
    changes2 = []
    def object_changed1(obj_info, variable, old_value, new_value, fromdb, arg):
        changes1.append(
            (1, obj_info, variable, old_value, new_value, fromdb, arg))
        return False
    def object_changed2(obj_info, variable, old_value, new_value, fromdb, arg):
        changes2.append(
            (2, obj_info, variable, old_value, new_value, fromdb, arg))
        return False

    obj_info.checkpoint()

    arg = object()
    obj_info.event.hook("changed", object_changed1, arg)
    obj_info.event.hook("changed", object_changed2, arg)

    obj.prop2 = 20
    obj.prop1 = 10

    assert changes1 == [(1, obj_info, variable2, Undef, 20, False, arg)]
    assert changes2 == [(2, obj_info, variable2, Undef, 20, False, arg)]


def test_obj_info_get_obj(obj, obj_info):
    assert obj_info.get_obj() is obj


def test_obj_info_get_obj_reference(Class, obj, obj_info):
    """
    We used to assign the get_obj() manually. This breaks stored
    references to the method (IOW, what we do in the test below).

    It was a bit faster, but in exchange for the danger of introducing
    subtle bugs which are super hard to debug.
    """
    get_obj = obj_info.get_obj
    assert get_obj() is obj
    another_obj = Class()
    obj_info.set_obj(another_obj)
    assert get_obj() is another_obj


def test_obj_info_set_obj(Class, obj_info):
    obj = Class()
    obj_info.set_obj(obj)
    assert obj_info.get_obj() is obj


def test_obj_info_weak_reference(Class):
    obj = Class()
    obj_info = get_obj_info(obj)
    del obj
    assert obj_info.get_obj() == None


def test_obj_info_object_deleted_notification(Class):
    obj = Class()
    obj_info = get_obj_info(obj)
    obj_info["tainted"] = True
    deleted = []
    def object_deleted(obj_info):
        deleted.append(obj_info)
    obj_info.event.hook("object-deleted", object_deleted)
    del obj_info
    del obj
    assert len(deleted) == 1
    assert "tainted" in deleted[0]


def test_obj_info_object_deleted_notification_after_set_obj(Class):
    obj = Class()
    obj_info = get_obj_info(obj)
    obj_info["tainted"] = True

    obj = Class()
    obj_info.set_obj(obj)
    deleted = []

    def object_deleted(obj_info):
        deleted.append(obj_info)
    obj_info.event.hook("object-deleted", object_deleted)

    del obj_info
    del obj

    assert len(deleted) == 1
    assert "tainted" in deleted[0]


def test_cls_alias_cls_info_cls(Class):
    Alias = ClassAlias(Class, "alias")
    cls_info = get_cls_info(Alias)
    assert cls_info.cls == Class
    assert cls_info.table.name == "alias"
    assert Alias.prop1.name == "column1"
    assert Alias.prop1.table == Alias


def test_cls_alias_compile(Class):
    Alias = ClassAlias(Class, "alias")
    statement = compile(Alias)
    assert statement == "alias"


def test_cls_alias_compile_with_reserved_keyword(Class):
    Alias = ClassAlias(Class, "select")
    statement = compile(Alias)
    assert statement == '"select"'


def test_cls_alias_compile_in_select(Class):
    Alias = ClassAlias(Class, "alias")
    expr = Select(Alias.prop1, Alias.prop1 == 1,
                  Alias)
    statement = compile(expr)
    assert statement == (
        'SELECT alias.column1 FROM "table" AS alias '
        'WHERE alias.column1 = ?'
    )


def test_cls_alias_compile_in_select_with_reserved_keyword(Class):
    Alias = ClassAlias(Class, "select")
    expr = Select(Alias.prop1, Alias.prop1 == 1, Alias)
    statement = compile(expr)
    assert statement == (
        'SELECT "select".column1 FROM "table" AS "select" '
        'WHERE "select".column1 = ?'
    )


def test_cls_alias_crazy_metaclass():
    """We don't want metaclasses playing around when we build an alias."""
    class MetaClass(type):
        def __new__(meta_cls, name, bases, dict):
            cls = type.__new__(meta_cls, name, bases, dict)
            cls.__storm_table__ = "HAH! GOTCH YA!"
            return cls

    @add_metaclass(MetaClass)
    class Class(object):
        __storm_table__ = "table"
        prop1 = Property("column1", primary=True)
    Alias = ClassAlias(Class, "USE_THIS")
    assert Alias.__storm_table__ == "USE_THIS"


def test_cls_alias_cached_aliases(Class):
    """
    Class aliases are cached such that multiple invocations of
    C{ClassAlias} return the same object.
    """
    alias1 = ClassAlias(Class, "something_unlikely")
    alias2 = ClassAlias(Class, "something_unlikely")
    assert alias1 is alias2
    alias3 = ClassAlias(Class, "something_unlikely2")
    assert alias1 is not alias3
    alias4 = ClassAlias(Class, "something_unlikely2")
    assert alias3 is alias4


def test_cls_alias_unnamed_aliases_not_cached(Class):
    alias1 = ClassAlias(Class)
    alias2 = ClassAlias(Class)
    assert alias1 is not alias2


def test_cls_alias_alias_cache_is_per_class(Class):
    """
    The cache of class aliases is not as bad as it once was.
    """
    class LocalClass(Class):
        pass
    alias = ClassAlias(Class, "something_unlikely")
    alias2 = ClassAlias(LocalClass, "something_unlikely")
    assert alias is not alias2


def test_cls_alias_aliases_only_last_as_long_as_class(Class):
    """
    The cached ClassAliases only last for as long as the class is alive.
    """
    class LocalClass(Class):
        pass
    alias = ClassAlias(LocalClass, "something_unlikely3")
    alias_ref = ref(alias)
    class_ref = ref(LocalClass)
    del alias
    del LocalClass

    # Wat?
    gc.collect(); gc.collect(); gc.collect()

    assert class_ref() is None
    assert alias_ref() is None


def test_type_compiler_nested_classes():
    """Convoluted case checking that the model is right."""
    class Class1(object):
        __storm_table__ = "class1"
        id = Property(primary=True)
    class Class2(object):
        __storm_table__ = Class1
        id = Property(primary=True)
    statement = compile(Class2)
    assert statement == "class1"
    alias = ClassAlias(Class2, "alias")
    statement = compile(Select(alias.id))
    assert statement == "SELECT alias.id FROM class1 AS alias"
