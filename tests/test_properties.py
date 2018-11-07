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
import gc
import json
import pytest
import uuid

from datetime import datetime, date, time, timedelta
from decimal import Decimal as decimal
from mock import ANY, call, Mock
from pytest_lazyfixture import lazy_fixture
from storm.compat import add_metaclass
from storm.exceptions import NoneError, PropertyPathError
from storm.properties import PropertyPublisherMeta
from storm.properties import *
from storm.variables import *
from storm.info import get_obj_info
from storm.expr import Column, Select, compile, State, SQLRaw
from tests.helper import assert_variables_equal


class Wrapper(object):
    def __init__(self, obj):
        self.obj = obj

    __storm_object_info__ = property(lambda self:
                                     self.obj.__storm_object_info__)


class CustomVariable(Variable):
    pass


class Custom(SimpleProperty):
    variable_class = CustomVariable


@pytest.fixture
def registry():
    return PropertyRegistry()


@pytest.fixture
def Class(registry):
    @registry.add_class
    class Class(object):
        __storm_table__ = "mytable"
        prop1 = Custom("column1", primary=True)
        prop2 = Custom()
        prop3 = Custom("column3", default=50, allow_none=False)
    return Class


@pytest.fixture
def SubClass(Class, registry):
    @registry.add_class
    class SubClass(Class):
        __storm_table__ = "mysubtable"
    return SubClass


def test_column(Class):
    assert isinstance(Class.prop1, Column)


@pytest.mark.parametrize("cls1, cls2", [
    (lazy_fixture("Class"), lazy_fixture("SubClass")),
    (lazy_fixture("SubClass"), lazy_fixture("Class")),
])
def test_cls(cls1, cls2):
    assert cls1.prop1.cls == cls1
    assert cls1.prop2.cls == cls1
    assert cls2.prop1.cls == cls2
    assert cls2.prop2.cls == cls2
    assert cls1.prop1.cls == cls1
    assert cls1.prop2.cls == cls1


def test_name(Class):
    assert Class.prop1.name == "column1"


def test_auto_name(Class):
    assert Class.prop2.name == "prop2"


@pytest.mark.parametrize("cls1, cls2", [
    (lazy_fixture("Class"), None),
    (lazy_fixture("Class"), lazy_fixture("SubClass")),
    (lazy_fixture("SubClass"), lazy_fixture("Class")),
])
def test_auto_table(cls1, cls2):
    assert cls1.prop1.table == cls1
    assert cls1.prop2.table == cls1

    if cls2 is not None:
        assert cls2.prop1.table == cls2
        assert cls2.prop2.table == cls2


@pytest.mark.parametrize("prop, is_defined", [
    ("prop1", False),
    ("prop3", True),
])
def test_variable_factory(Class, is_defined, prop):
    variable = getattr(Class, prop).variable_factory()
    assert isinstance(variable, CustomVariable)
    assert variable.is_defined() == is_defined


def test_variable_factory_validator_attribute():
    # Should work even if we make things harder by reusing properties.
    prop = Custom()
    class Class1(object):
        __storm_table__ = "table1"
        prop1 = prop
    class Class2(object):
        __storm_table__ = "table2"
        prop2 = prop
    args = []
    def validator(obj, attr, value):
        args.append((obj, attr, value))
    variable1 = Class1.prop1.variable_factory(validator=validator)
    variable2 = Class2.prop2.variable_factory(validator=validator)
    variable1.set(1)
    variable2.set(2)
    assert args == [(None, "prop1", 1), (None, "prop2", 2)]


def test_default(SubClass):
    obj = SubClass()
    assert obj.prop1 == None
    assert obj.prop2 == None
    assert obj.prop3 == 50


def test_set_get(Class):
    obj = Class()
    obj.prop1 = 10
    obj.prop2 = 20
    obj.prop3 = 30
    assert obj.prop1 == 10
    assert obj.prop2 == 20
    assert obj.prop3 == 30


def test_set_get_none(Class):
    obj = Class()
    obj.prop1 = None
    obj.prop2 = None
    assert obj.prop1 == None
    assert obj.prop2 == None

    # prop3 has allow_none=False
    with pytest.raises(NoneError):
        setattr(obj, "prop3", None)


def test_set_with_validator():
    args = []
    def validator(obj, attr, value):
        args[:] = obj, attr, value
        return 42

    class Class(object):
        __storm_table__ = "mytable"
        prop = Custom("column", primary=True, validator=validator)

    obj = Class()
    obj.prop = 21

    assert args == [obj, "prop", 21]
    assert obj.prop == 42


def test_set_get_subclass(SubClass):
    obj = SubClass()
    obj.prop1 = 10
    obj.prop2 = 20
    obj.prop3 = 30
    assert obj.prop1 == 10
    assert obj.prop2 == 20
    assert obj.prop3 == 30


@pytest.mark.parametrize("cls", [
    lazy_fixture("Class"),
    lazy_fixture("SubClass"),
])
def test_set_get_explicitly(cls):
    obj = cls()
    prop1 = cls.prop1
    prop2 = cls.prop2
    prop3 = cls.prop3
    prop1.__set__(obj, 10)
    prop2.__set__(obj, 20)
    prop3.__set__(obj, 30)
    assert prop1.__get__(obj) == 10
    assert prop2.__get__(obj) == 20
    assert prop3.__get__(obj) == 30


def test_set_get_subclass_explicitly(Class, SubClass):
    obj = SubClass()
    prop1 = Class.prop1
    prop2 = Class.prop2
    prop3 = Class.prop3
    prop1.__set__(obj, 10)
    prop2.__set__(obj, 20)
    prop3.__set__(obj, 30)
    assert prop1.__get__(obj) == 10
    assert prop2.__get__(obj) == 20
    assert prop3.__get__(obj) == 30


@pytest.mark.parametrize("cls", [
    lazy_fixture("Class"),
    lazy_fixture("SubClass"),
])
def test_delete(cls):
    obj = cls()
    obj.prop1 = 10
    obj.prop2 = 20
    obj.prop3 = 30
    del obj.prop1
    del obj.prop2
    del obj.prop3
    assert obj.prop1 == None
    assert obj.prop2 == None
    assert obj.prop3 == None


@pytest.mark.parametrize("cls", [
    lazy_fixture("Class"),
    lazy_fixture("SubClass"),
])
def test_delete_explicitly(cls):
    obj = cls()
    obj.prop1 = 10
    obj.prop2 = 20
    obj.prop3 = 30
    cls.prop1.__delete__(obj)
    cls.prop2.__delete__(obj)
    cls.prop3.__delete__(obj)
    assert obj.prop1 == None
    assert obj.prop2 == None
    assert obj.prop3 == None


@pytest.mark.parametrize("cls, table", [
    (lazy_fixture("Class"), "mytable"),
    (lazy_fixture("SubClass"), "mysubtable"),
])
def test_comparable_expr(cls, table):
    prop1 = cls.prop1
    prop2 = cls.prop2
    prop3 = cls.prop3
    expr = Select(SQLRaw("*"), (prop1 == "value1") &
                               (prop2 == "value2") &
                               (prop3 == "value3"))
    state = State()
    statement = compile(expr, state)
    assert statement == (
        "SELECT * FROM {table} WHERE "
        "{table}.column1 = ? AND "
        "{table}.prop2 = ? AND "
        "{table}.column3 = ?"
    ).format(table=table)

    assert_variables_equal(
        state.parameters,
        [CustomVariable("value1"),
         CustomVariable("value2"),
         CustomVariable("value3")])


def test_set_get_delete_with_wrapper(Class):
    obj = Class()
    get_obj_info(obj) # Ensure the obj_info exists for obj.
    Class.prop1.__set__(Wrapper(obj), 10)
    assert Class.prop1.__get__(Wrapper(obj)) == 10
    Class.prop1.__delete__(Wrapper(obj))
    assert Class.prop1.__get__(Wrapper(obj)) == None


def test_reuse_of_instance():
    """Properties are dynamically bound to the class where they're used.

    It basically means that the property may be instantiated
    independently from the class itself, and reused in any number of
    classes.  It's not something we should announce as granted, but
    right now it works, and we should try not to break it.
    """
    prop = Custom()
    class Class1(object):
        __storm_table__ = "table1"
        prop1 = prop
    class Class2(object):
        __storm_table__ = "table2"
        prop2 = prop
    assert Class1.prop1.name == "prop1"
    assert Class1.prop1.table == Class1
    assert Class2.prop2.name == "prop2"
    assert Class2.prop2.table == Class2


@pytest.mark.parametrize("prop_cls, kwargs", [
    (Bool, dict(default=True)),
    (Int, dict(default=50)),
    (Float, dict(default=50.5)),
    (Decimal, dict(default=decimal("50.5"))),
    (RawStr, dict(default=b"default")),
    (DateTime, dict(default=datetime.utcfromtimestamp(0))),
    (Date, dict(default=date(2006, 1, 1))),
    (Time, dict(default=time(12, 34))),
    (TimeDelta, dict(default=timedelta(days=1, seconds=2, microseconds=3))),
    (UUID, dict(default=uuid.UUID("{0609f76b-878f-4546-baf5-c1b135e8de72}"))),
    (Enum, dict(default="foo", map={"foo": 1, "bar": 2})),
    (Enum, dict(
        default="biz",
        default_value_override="foo",
        map={"foo": 1, "bar": 2},
        set_map={"biz": 1, "baz": 2}
    )),
    (JSON, dict(default_factory=dict)),
])
def test_simple_property(prop_cls, kwargs):
    # We must either have a default or a default factory but not both
    assert ("default" in kwargs) ^ ("default_factory" in kwargs)
    if "default" in kwargs:
        default = kwargs["default"]
    else:
        default = kwargs["default_factory"]()

    # For some types, like Enum, what we set is not what we get
    if "default_value_override" in kwargs:
        default = kwargs.pop("default_value_override")

    assert "allow_none" not in kwargs
    assert "primary" not in kwargs

    class Class(object):
        __storm_table__ = "mytable"
        prop1 = prop_cls(
            allow_none=False,
            primary=True,
            **kwargs
        )
        prop2 = prop_cls(
            "real_column_name",
            **kwargs
        )

    class SubClass(Class):
        pass

    assert isinstance(SubClass.prop1, Column)
    assert isinstance(SubClass.prop2, Column)
    assert SubClass.prop1.name == "prop1"
    assert SubClass.prop1.table == SubClass
    assert SubClass.prop2.name == "real_column_name"
    assert SubClass.prop2.table == SubClass

    obj = SubClass()
    obj_info = get_obj_info(obj)
    var_cls = prop_cls.variable_class
    assert isinstance(obj_info.variables[SubClass.prop1], var_cls)
    assert isinstance(obj_info.variables[SubClass.prop2], var_cls)

    assert obj.prop1 == default
    with pytest.raises(NoneError):
        obj.prop1 = None

    obj.prop2 = None
    assert obj.prop2 is None


def test_bool():
    class Class(object):
        __storm_table__ = "mytable"
        prop = Bool(primary=True)

    obj = Class()
    obj.prop = 1
    assert obj.prop is True
    obj.prop = 0
    assert obj.prop is False


def test_int():
    class Class(object):
        __storm_table__ = "mytable"
        prop = Int(primary=True)

    obj = Class()
    obj.prop = True
    assert obj.prop == 1
    obj.prop = False
    assert obj.prop == 0


def test_float():
    class Class(object):
        __storm_table__ = "mytable"
        prop = Float(primary=True)

    obj = Class()
    obj.prop = 1
    assert isinstance(obj.prop, float)


def test_decimal():
    class Class(object):
        __storm_table__ = "mytable"
        prop = Decimal(primary=True)

    obj = Class()
    obj.prop = 1
    assert isinstance(obj.prop, decimal)

    with pytest.raises(TypeError):
        obj.prop = 0.5


def test_raw_str_unicode_raises():
    class Class(object):
        __storm_table__ = "mytable"
        prop = RawStr(primary=True)

    obj = Class()
    with pytest.raises(TypeError):
        obj.prop = u"unicode"


def test_datetime():
    class Class(object):
        __storm_table__ = "mytable"
        prop = DateTime(primary=True)

    obj = Class()

    obj.prop = 0.0
    assert obj.prop == datetime.utcfromtimestamp(0)

    dt = datetime(2006, 1, 1, 12, 34)
    obj.prop = dt
    assert obj.prop == dt

    with pytest.raises(TypeError):
        obj.prop = object()


def test_date():
    class Class(object):
        __storm_table__ = "mytable"
        prop = Date(primary=True)

    obj = Class()

    dt = datetime(2006, 1, 1, 12, 34, 56)
    d = dt.date()
    obj.prop = d
    assert obj.prop == d
    obj.prop = dt
    assert obj.prop == d

    with pytest.raises(TypeError):
        obj.prop = object()


def test_time():
    class Class(object):
        __storm_table__ = "mytable"
        prop = Time(primary=True)

    obj = Class()

    dt = datetime(2006, 1, 1, 12, 34, 56)
    t = dt.time()
    obj.prop = t
    assert obj.prop == t
    obj.prop = dt
    assert obj.prop == t

    with pytest.raises(TypeError):
        obj.prop = object()


def test_timedelta():
    class Class(object):
        __storm_table__ = "mytable"
        prop = TimeDelta(primary=True)

    obj = Class()

    td = timedelta(days=42, seconds=42, microseconds=42)
    obj.prop = td
    assert obj.prop == td

    with pytest.raises(TypeError):
        obj.prop = object()


def test_uuid():
    class Class(object):
        __storm_table__ = "mytable"
        prop = UUID(primary=True)

    obj = Class()

    value1 = uuid.UUID("{0609f76b-878f-4546-baf5-c1b135e8de72}")
    value2 = uuid.UUID("{c9703f9d-0abb-47d7-a793-8f90f1b98d5e}")

    obj.prop = value1
    assert obj.prop == value1
    obj.prop = value2
    assert obj.prop == value2

    with pytest.raises(TypeError):
        obj.prop = "{0609f76b-878f-4546-baf5-c1b135e8de72}"


def test_enum():
    class Class(object):
        __storm_table__ = "mytable"
        prop = Enum(map={"foo": 1, "bar": 2}, primary=True)

    obj = Class()
    obj.prop = "foo"
    assert obj.prop == "foo"
    obj.prop = "bar"
    assert obj.prop == "bar"

    with pytest.raises(ValueError):
        obj.prop = "baz"
    with pytest.raises(ValueError):
        obj.prop = 1


def test_enum_with_set_map():
    class Class(object):
        __storm_table__ = "mytable"
        prop = Enum(
            map={"foo": 1, "bar": 2},
            primary=True,
            set_map={"biz": 1, "baz": 2},
        )

    obj = Class()
    obj.prop = "biz"
    assert obj.prop == "foo"
    obj.prop = "baz"
    assert obj.prop == "bar"

    with pytest.raises(ValueError):
        obj.prop = "foo"
    with pytest.raises(ValueError):
        obj.prop = 1


def test_json():
    class Class(object):
        __storm_table__ = "mytable"
        prop = JSON(default_factory=dict, primary=True)

    obj = Class()
    obj.prop = []
    assert obj.prop == []
    obj.prop.append(1)
    obj.prop.append("a")
    assert obj.prop == [1, "a"]


def test_list():
    class Class(object):
        __storm_table__ = "mytable"
        prop = List(default_factory=list, primary=True)

    obj = Class()
    assert obj.prop == []
    obj.prop.append("a")
    obj.prop.append("b")
    assert obj.prop == ["a", "b"]


@pytest.mark.parametrize("prop_cls", [
    List,
    JSON,
])
def test_events(prop_cls):
    class Class(object):
        __storm_table__ = "mytable"
        prop = prop_cls(default_factory=list, primary=True)

    obj = Class()
    obj_info = get_obj_info(obj)

    change_callback = Mock()
    obj_info.checkpoint()
    obj_info.event.emit("start-tracking-changes", obj_info.event)
    obj_info.event.hook("changed", change_callback)

    # Events shouldn't trigger until we flush
    assert obj.prop == []
    assert change_callback.call_args_list == []
    obj.prop.append("a")
    assert change_callback.call_args_list == []

    # Check "flush" event. Notice that the other variable wasn't
    # listed, since it wasn't changed.
    obj_info.event.emit("flush")
    assert change_callback.call_args_list == [
        call(ANY, Class.prop, None, ["a"], False),
    ]

    change_callback.reset_mock()

    # Check "object-deleted" event. Notice that the other variable
    # wasn't listed again, since it wasn't changed.
    del obj
    assert change_callback.call_args_list == [
        call(ANY, Class.prop, None, ["a"], False),
    ]


@pytest.mark.parametrize("func, cls, value", [
    (Bool, BoolVariable, True),
    (Int, IntVariable, 1),
    (Float, FloatVariable, 1.1),
    (RawStr, RawStrVariable, b"str"),
    (Unicode, UnicodeVariable, u"unicode"),
    (DateTime, DateTimeVariable, datetime.now()),
    (Date, DateVariable, date.today()),
    (Time, TimeVariable, datetime.now().time()),
])
def test_variable_factory_arguments(func, cls, value):
    class Class(object):
        __storm_table__ = "test"
        id = Int(primary=True)

    validator_args = []
    def validator(obj, attr, value):
        validator_args[:] = obj, attr, value
        return value

    # Test no default and allow_none=True.
    Class.prop = func(name="name")
    column = Class.prop.__get__(None, Class)
    assert column.name == "name"
    assert column.table == Class

    variable = column.variable_factory()
    assert isinstance(variable, cls)
    assert variable.get() == None
    variable.set(None)
    assert variable.get() == None

    # Test default and allow_none=False.
    Class.prop = func(name="name", default=value, allow_none=False)
    column = Class.prop.__get__(None, Class)
    assert column.name == "name"
    assert column.table == Class

    variable = column.variable_factory()
    assert isinstance(variable, cls)
    with pytest.raises(NoneError):
        variable.set(None)
    assert variable.get() == value

    # Test default_factory.
    Class.prop = func(name="name", default_factory=lambda:value)
    column = Class.prop.__get__(None, Class)
    assert column.name == "name"
    assert column.table == Class

    variable = column.variable_factory()
    assert isinstance(variable, cls)
    assert variable.get() == value

    # Test validator.
    Class.prop = func(name="name", validator=validator, default=value)
    column = Class.prop.__get__(None, Class)
    assert column.name == "name"
    assert column.table == Class

    del validator_args[:]
    variable = column.variable_factory()
    assert isinstance(variable, cls)
    # Validator is not called on instantiation.
    assert validator_args == []
    # But is when setting the variable.
    variable.set(value)
    assert validator_args == [None, "prop", value]


def test_registry_get_empty(registry):
    with pytest.raises(PropertyPathError):
        registry.get("unexistent")


def test_registry_get(Class, registry):
    prop1 = registry.get("prop1")
    prop2 = registry.get("prop2")
    assert prop1 is Class.prop1
    assert prop2 is Class.prop2


def test_registry_get_with_class_name(Class, registry):
    prop1 = registry.get("Class.prop1")
    prop2 = registry.get("Class.prop2")
    assert prop1 is Class.prop1
    assert prop2 is Class.prop2


def test_registry_get_with_two_classes(Class, registry, SubClass):
    prop1 = registry.get("Class.prop1")
    prop2 = registry.get("Class.prop2")
    assert prop1 is Class.prop1
    assert prop2 is Class.prop2
    prop1 = registry.get("SubClass.prop1")
    prop2 = registry.get("SubClass.prop2")
    assert prop1 is SubClass.prop1
    assert prop2 is SubClass.prop2


def test_registry_get_ambiguous(Class, registry, SubClass):
    AnotherClass = type("Class", (Class,), {})
    AnotherClass.__module__ += ".foo"
    registry.add_class(AnotherClass)

    with pytest.raises(PropertyPathError):
        registry.get("Class.prop1")
    with pytest.raises(PropertyPathError):
        registry.get("Class.prop2")

    prop1 = registry.get("SubClass.prop1")
    prop2 = registry.get("SubClass.prop2")
    assert prop1 is SubClass.prop1
    assert prop2 is SubClass.prop2


def test_registry_get_ambiguous_but_different_path(Class, registry, SubClass):
    AnotherClass = type("Class", (Class,), {})
    AnotherClass.__module__ += ".foo"
    registry.add_class(AnotherClass)

    prop1 = registry.get("test_properties.Class.prop1")
    prop2 = registry.get("test_properties.Class.prop2")
    assert prop1 is Class.prop1
    assert prop2 is Class.prop2

    prop1 = registry.get("SubClass.prop1")
    prop2 = registry.get("SubClass.prop2")
    assert prop1 is SubClass.prop1
    assert prop2 is SubClass.prop2

    prop1 = registry.get("foo.Class.prop1")
    prop2 = registry.get("foo.Class.prop2")
    assert prop1 is AnotherClass.prop1
    assert prop2 is AnotherClass.prop2


def test_registry_get_ambiguous_but_different_path_with_namespace(Class, registry):
    AnotherClass = type("Class", (Class,), {})
    AnotherClass.__module__ += ".foo"
    registry.add_class(AnotherClass)

    prop1 = registry.get("Class.prop1", "tests.test_properties")
    prop2 = registry.get("Class.prop2", "tests.test_properties.bar")
    assert prop1 is Class.prop1
    assert prop2 is Class.prop2

    prop1 = registry.get("Class.prop1", "tests.test_properties.foo")
    prop2 = registry.get("Class.prop2", "tests.test_properties.foo.bar")
    assert prop1 is AnotherClass.prop1
    assert prop2 is AnotherClass.prop2


def test_registry_class_is_collectable(Class, registry):
    AnotherClass = type("Class", (Class,), {})
    AnotherClass.__module__ += ".foo"
    registry.add_class(AnotherClass)

    del AnotherClass
    gc.collect()

    # We should no longer have ambiguosity as AnotherClass is garbage collected
    prop1 = registry.get("prop1")
    prop2 = registry.get("prop2")
    assert prop1 is Class.prop1
    assert prop2 is Class.prop2


def test_registry_add_property(registry):
    class PropCls(object):
        prop = Int()

    registry.add_property(PropCls, PropCls.prop, "custom_name")
    prop = registry.get("PropCls.custom_name")
    assert prop == PropCls.prop
    with pytest.raises(PropertyPathError):
        registry.get("PropCls.prop")


class test_propert_publisher_meta_class():
    @add_metaclass(PropertyPublisherMeta)
    class Base(object):
        pass

    class Class(Base):
        __storm_table__ = "mytable"
        prop1 = Property("column1", primary=True)
        prop2 = Property()

    class SubClass(Class):
        __storm_table__ = "mysubtable"

    # Override the previous Class to ensure conflicts in resolution
    class Class(Class):
        __module__ += ".foo"
        prop3 = Property("column3")

    AnotherClass = Class

    registry = Base._storm_property_registry

    # Try a value that doesn't exist in the registry
    with pytest.raises(PropertyPathError):
        registry.get("unexistent")

    # Try ambiguos entries
    with pytest.raises(PropertyPathError):
        registry.get("Class.prop1")
    with pytest.raises(PropertyPathError):
        registry.get("Class.prop2")

    # Get from sub class
    prop1 = registry.get("SubClass.prop1")
    prop2 = registry.get("SubClass.prop2")
    assert prop1 is SubClass.prop1
    assert prop2 is SubClass.prop2
