from datetime import datetime, date, time

from storm.properties import *
from storm.variables import *
from storm.info import get_obj_info
from storm.expr import Undef, Column, Select, compile

from tests.helper import TestHelper


class CustomVariable(Variable):
    pass

class Custom(SimpleProperty):
    variable_class = CustomVariable


class PropertyTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __table__ = "table", "column1"
            prop1 = Custom("column1")
            prop2 = Custom()
            prop3 = Custom("column3", default=50, not_none=True)
        class SubClass(Class):
            __table__ = "subtable", "column1"
        self.Class = Class
        self.SubClass = SubClass

    def test_column(self):
        self.assertTrue(isinstance(self.Class.prop1, Column))

    def test_cls(self):
        self.assertEquals(self.Class.prop1.cls, self.Class)
        self.assertEquals(self.Class.prop2.cls, self.Class)
        self.assertEquals(self.SubClass.prop1.cls, self.SubClass)
        self.assertEquals(self.SubClass.prop2.cls, self.SubClass)
        self.assertEquals(self.Class.prop1.cls, self.Class)
        self.assertEquals(self.Class.prop2.cls, self.Class)

    def test_cls_reverse(self):
        self.assertEquals(self.SubClass.prop1.cls, self.SubClass)
        self.assertEquals(self.SubClass.prop2.cls, self.SubClass)
        self.assertEquals(self.Class.prop1.cls, self.Class)
        self.assertEquals(self.Class.prop2.cls, self.Class)
        self.assertEquals(self.SubClass.prop1.cls, self.SubClass)
        self.assertEquals(self.SubClass.prop2.cls, self.SubClass)

    def test_name(self):
        self.assertEquals(self.Class.prop1.name, "column1")

    def test_auto_name(self):
        self.assertEquals(self.Class.prop2.name, "prop2")

    def test_auto_table(self):
        self.assertEquals(self.Class.prop1.table, self.Class)
        self.assertEquals(self.Class.prop2.table, self.Class)

    def test_auto_table_subclass(self):
        self.assertEquals(self.Class.prop1.table, self.Class)
        self.assertEquals(self.Class.prop2.table, self.Class)
        self.assertEquals(self.SubClass.prop1.table, self.SubClass)
        self.assertEquals(self.SubClass.prop2.table, self.SubClass)

    def test_auto_table_subclass_reverse_initialization(self):
        self.assertEquals(self.SubClass.prop1.table, self.SubClass)
        self.assertEquals(self.SubClass.prop2.table, self.SubClass)
        self.assertEquals(self.Class.prop1.table, self.Class)
        self.assertEquals(self.Class.prop2.table, self.Class)

    def test_variable_factory(self):
        variable = self.Class.prop1.variable_factory()
        self.assertTrue(isinstance(variable, CustomVariable))
        self.assertFalse(variable.is_defined())

        variable = self.Class.prop3.variable_factory()
        self.assertTrue(isinstance(variable, CustomVariable))
        self.assertTrue(variable.is_defined())

    def test_default(self):
        obj = self.SubClass()
        self.assertEquals(obj.prop1, None)
        self.assertEquals(obj.prop2, None)
        self.assertEquals(obj.prop3, 50)

    def test_set_get(self):
        obj = self.Class()
        obj.prop1 = 10
        obj.prop2 = 20
        obj.prop3 = 30
        self.assertEquals(obj.prop1, 10)
        self.assertEquals(obj.prop2, 20)
        self.assertEquals(obj.prop3, 30)

    def test_set_get_none(self):
        obj = self.Class()
        obj.prop1 = None
        obj.prop2 = None
        self.assertEquals(obj.prop1, None)
        self.assertEquals(obj.prop2, None)
        self.assertRaises(NotNoneError, setattr, obj, "prop3", None)

    def test_set_get_subclass(self):
        obj = self.SubClass()
        obj.prop1 = 10
        obj.prop2 = 20
        obj.prop3 = 30
        self.assertEquals(obj.prop1, 10)
        self.assertEquals(obj.prop2, 20)
        self.assertEquals(obj.prop3, 30)

    def test_set_get_explicitly(self):
        obj = self.Class()
        prop1 = self.Class.prop1
        prop2 = self.Class.prop2
        prop3 = self.Class.prop3
        prop1.__set__(obj, 10)
        prop2.__set__(obj, 20)
        prop3.__set__(obj, 30)
        self.assertEquals(prop1.__get__(obj), 10)
        self.assertEquals(prop2.__get__(obj), 20)
        self.assertEquals(prop3.__get__(obj), 30)

    def test_set_get_subclass_explicitly(self):
        obj = self.SubClass()
        prop1 = self.Class.prop1
        prop2 = self.Class.prop2
        prop3 = self.Class.prop3
        prop1.__set__(obj, 10)
        prop2.__set__(obj, 20)
        prop3.__set__(obj, 30)
        self.assertEquals(prop1.__get__(obj), 10)
        self.assertEquals(prop2.__get__(obj), 20)
        self.assertEquals(prop3.__get__(obj), 30)

    def test_delete(self):
        obj = self.Class()
        obj.prop1 = 10
        obj.prop2 = 20
        obj.prop3 = 30
        del obj.prop1
        del obj.prop2
        del obj.prop3
        self.assertEquals(obj.prop1, None)
        self.assertEquals(obj.prop2, None)
        self.assertEquals(obj.prop3, None)

    def test_delete_subclass(self):
        obj = self.SubClass()
        obj.prop1 = 10
        obj.prop2 = 20
        obj.prop3 = 30
        del obj.prop1
        del obj.prop2
        del obj.prop3
        self.assertEquals(obj.prop1, None)
        self.assertEquals(obj.prop2, None)
        self.assertEquals(obj.prop3, None)

    def test_delete_explicitly(self):
        obj = self.Class()
        obj.prop1 = 10
        obj.prop2 = 20
        obj.prop3 = 30
        self.Class.prop1.__delete__(obj)
        self.Class.prop2.__delete__(obj)
        self.Class.prop3.__delete__(obj)
        self.assertEquals(obj.prop1, None)
        self.assertEquals(obj.prop2, None)
        self.assertEquals(obj.prop3, None)

    def test_delete_subclass_explicitly(self):
        obj = self.SubClass()
        obj.prop1 = 10
        obj.prop2 = 20
        obj.prop3 = 30
        self.Class.prop1.__delete__(obj)
        self.Class.prop2.__delete__(obj)
        self.Class.prop3.__delete__(obj)
        self.assertEquals(obj.prop1, None)
        self.assertEquals(obj.prop2, None)
        self.assertEquals(obj.prop3, None)

    def test_comparable_expr(self):
        prop1 = self.Class.prop1
        prop2 = self.Class.prop2
        prop3 = self.Class.prop3
        expr = Select("*", (prop1 == "value1") &
                           (prop2 == "value2") &
                           (prop3 == "value3"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "SELECT * FROM table WHERE "
                                     "table.column1 = ? AND "
                                     "table.prop2 = ? AND "
                                     "table.column3 = ?")
        self.assertEquals(parameters, [CustomVariable("value1"),
                                       CustomVariable("value2"),
                                       CustomVariable("value3")])

    def test_comparable_expr_subclass(self):
        prop1 = self.SubClass.prop1
        prop2 = self.SubClass.prop2
        prop3 = self.SubClass.prop3
        expr = Select("*", (prop1 == "value1") &
                           (prop2 == "value2") &
                           (prop3 == "value3"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "SELECT * FROM subtable WHERE "
                                     "subtable.column1 = ? AND "
                                     "subtable.prop2 = ? AND "
                                     "subtable.column3 = ?")
        self.assertEquals(parameters, [CustomVariable("value1"),
                                       CustomVariable("value2"),
                                       CustomVariable("value3")])


class PropertyKindsTest(TestHelper):

    def setup(self, property, *args, **kwargs):
        class Class(object):
            __table__ = "table", "column1"
            prop1 = property("column1", *args, **kwargs)
            prop2 = property()
        class SubClass(Class):
            pass
        self.Class = Class
        self.SubClass = SubClass
        self.obj = SubClass()
        self.obj_info = get_obj_info(self.obj)
        self.column1 = self.SubClass.prop1
        self.column2 = self.SubClass.prop2
        self.variable1 = self.obj_info.variables[self.column1]
        self.variable2 = self.obj_info.variables[self.column2]

    def test_bool(self):
        self.setup(Bool, default=True, not_none=True)

        self.assertTrue(isinstance(self.column1, Column))
        self.assertTrue(isinstance(self.column2, Column))
        self.assertEquals(self.column1.name, "column1")
        self.assertEquals(self.column1.table, self.SubClass)
        self.assertEquals(self.column2.name, "prop2")
        self.assertEquals(self.column2.table, self.SubClass)
        self.assertTrue(isinstance(self.variable1, BoolVariable))
        self.assertTrue(isinstance(self.variable2, BoolVariable))

        self.assertEquals(self.obj.prop1, True)
        self.assertRaises(NotNoneError, setattr, self.obj, "prop1", None)
        self.obj.prop2 = None
        self.assertEquals(self.obj.prop2, None)

        self.obj.prop1 = 1
        self.assertTrue(self.obj.prop1 is True)
        self.obj.prop1 = 0
        self.assertTrue(self.obj.prop1 is False)

    def test_int(self):
        self.setup(Int, default=50, not_none=True)

        self.assertTrue(isinstance(self.column1, Column))
        self.assertTrue(isinstance(self.column2, Column))
        self.assertEquals(self.column1.name, "column1")
        self.assertEquals(self.column1.table, self.SubClass)
        self.assertEquals(self.column2.name, "prop2")
        self.assertEquals(self.column2.table, self.SubClass)
        self.assertTrue(isinstance(self.variable1, IntVariable))
        self.assertTrue(isinstance(self.variable2, IntVariable))

        self.assertEquals(self.obj.prop1, 50)
        self.assertRaises(NotNoneError, setattr, self.obj, "prop1", None)
        self.obj.prop2 = None
        self.assertEquals(self.obj.prop2, None)

        self.obj.prop1 = False
        self.assertEquals(self.obj.prop1, 0)
        self.obj.prop1 = True
        self.assertEquals(self.obj.prop1, 1)

    def test_float(self):
        self.setup(Float, default=50.5, not_none=True)

        self.assertTrue(isinstance(self.column1, Column))
        self.assertTrue(isinstance(self.column2, Column))
        self.assertEquals(self.column1.name, "column1")
        self.assertEquals(self.column1.table, self.SubClass)
        self.assertEquals(self.column2.name, "prop2")
        self.assertEquals(self.column2.table, self.SubClass)
        self.assertTrue(isinstance(self.variable1, FloatVariable))
        self.assertTrue(isinstance(self.variable2, FloatVariable))

        self.assertEquals(self.obj.prop1, 50.5)
        self.assertRaises(NotNoneError, setattr, self.obj, "prop1", None)
        self.obj.prop2 = None
        self.assertEquals(self.obj.prop2, None)

        self.obj.prop1 = 1
        self.assertTrue(isinstance(self.obj.prop1, float))

    def test_str(self):
        self.setup(Str, default="def", not_none=True)

        self.assertTrue(isinstance(self.column1, Column))
        self.assertTrue(isinstance(self.column2, Column))
        self.assertEquals(self.column1.name, "column1")
        self.assertEquals(self.column1.table, self.SubClass)
        self.assertEquals(self.column2.name, "prop2")
        self.assertEquals(self.column2.table, self.SubClass)
        self.assertTrue(isinstance(self.variable1, StrVariable))
        self.assertTrue(isinstance(self.variable2, StrVariable))

        self.assertEquals(self.obj.prop1, "def")
        self.assertRaises(NotNoneError, setattr, self.obj, "prop1", None)
        self.obj.prop2 = None
        self.assertEquals(self.obj.prop2, None)

        self.obj.prop1 = u"str"
        self.assertTrue(isinstance(self.obj.prop1, str))

    def test_unicode(self):
        self.setup(Unicode, default=u"def", not_none=True)

        self.assertTrue(isinstance(self.column1, Column))
        self.assertTrue(isinstance(self.column2, Column))
        self.assertEquals(self.column1.name, "column1")
        self.assertEquals(self.column1.table, self.SubClass)
        self.assertEquals(self.column2.name, "prop2")
        self.assertEquals(self.column2.table, self.SubClass)
        self.assertTrue(isinstance(self.variable1, UnicodeVariable))
        self.assertTrue(isinstance(self.variable2, UnicodeVariable))

        self.assertEquals(self.obj.prop1, u"def")
        self.assertRaises(NotNoneError, setattr, self.obj, "prop1", None)
        self.obj.prop2 = None
        self.assertEquals(self.obj.prop2, None)

        self.obj.prop1 = "unicode"
        self.assertTrue(isinstance(self.obj.prop1, unicode))

    def test_datetime(self):
        self.setup(DateTime, default=0, not_none=True)

        self.assertTrue(isinstance(self.column1, Column))
        self.assertTrue(isinstance(self.column2, Column))
        self.assertEquals(self.column1.name, "column1")
        self.assertEquals(self.column1.table, self.SubClass)
        self.assertEquals(self.column2.name, "prop2")
        self.assertEquals(self.column2.table, self.SubClass)
        self.assertTrue(isinstance(self.variable1, DateTimeVariable))
        self.assertTrue(isinstance(self.variable2, DateTimeVariable))

        self.assertEquals(self.obj.prop1, datetime.utcfromtimestamp(0))
        self.assertRaises(NotNoneError, setattr, self.obj, "prop1", None)
        self.obj.prop2 = None
        self.assertEquals(self.obj.prop2, None)

        self.obj.prop1 = 0.0
        self.assertEquals(self.obj.prop1, datetime.utcfromtimestamp(0))
        self.obj.prop1 = datetime(2006, 1, 1, 12, 34)
        self.assertEquals(self.obj.prop1, datetime(2006, 1, 1, 12, 34))

        self.assertRaises(TypeError, setattr, self.obj, "prop1", object())

    def test_date(self):
        self.setup(Date, default=date(2006, 1, 1), not_none=1)

        self.assertTrue(isinstance(self.column1, Column))
        self.assertTrue(isinstance(self.column2, Column))
        self.assertEquals(self.column1.name, "column1")
        self.assertEquals(self.column1.table, self.SubClass)
        self.assertEquals(self.column2.name, "prop2")
        self.assertEquals(self.column2.table, self.SubClass)
        self.assertTrue(isinstance(self.variable1, DateVariable))
        self.assertTrue(isinstance(self.variable2, DateVariable))

        self.assertEquals(self.obj.prop1, date(2006, 1, 1))
        self.assertRaises(NotNoneError, setattr, self.obj, "prop1", None)
        self.obj.prop2 = None
        self.assertEquals(self.obj.prop2, None)

        self.obj.prop1 = datetime(2006, 1, 1, 12, 34, 56)
        self.assertEquals(self.obj.prop1, date(2006, 1, 1))
        self.obj.prop1 = date(2006, 1, 1)
        self.assertEquals(self.obj.prop1, date(2006, 1, 1))

        self.assertRaises(TypeError, setattr, self.obj, "prop1", object())

    def test_time(self):
        self.setup(Time, default=time(12, 34), not_none=True)

        self.assertTrue(isinstance(self.column1, Column))
        self.assertTrue(isinstance(self.column2, Column))
        self.assertEquals(self.column1.name, "column1")
        self.assertEquals(self.column1.table, self.SubClass)
        self.assertEquals(self.column2.name, "prop2")
        self.assertEquals(self.column2.table, self.SubClass)
        self.assertTrue(isinstance(self.variable1, TimeVariable))
        self.assertTrue(isinstance(self.variable2, TimeVariable))

        self.assertEquals(self.obj.prop1, time(12, 34))
        self.assertRaises(NotNoneError, setattr, self.obj, "prop1", None)
        self.obj.prop2 = None
        self.assertEquals(self.obj.prop2, None)

        self.obj.prop1 = datetime(2006, 1, 1, 12, 34, 56)
        self.assertEquals(self.obj.prop1, time(12, 34, 56))
        self.obj.prop1 = time(12, 34, 56)
        self.assertEquals(self.obj.prop1, time(12, 34, 56))

        self.assertRaises(TypeError, setattr, self.obj, "prop1", object())

    def test_pickle(self):
        self.setup(Pickle, default_factory=dict, not_none=True)

        self.assertTrue(isinstance(self.column1, Column))
        self.assertTrue(isinstance(self.column2, Column))
        self.assertEquals(self.column1.name, "column1")
        self.assertEquals(self.column1.table, self.SubClass)
        self.assertEquals(self.column2.name, "prop2")
        self.assertEquals(self.column2.table, self.SubClass)
        self.assertTrue(isinstance(self.variable1, PickleVariable))
        self.assertTrue(isinstance(self.variable2, PickleVariable))

        self.assertEquals(self.obj.prop1, {})
        self.assertRaises(NotNoneError, setattr, self.obj, "prop1", None)
        self.obj.prop2 = None
        self.assertEquals(self.obj.prop2, None)

        self.obj.prop1 = []
        self.assertEquals(self.obj.prop1, [])
        self.obj.prop1.append("a")
        self.assertEquals(self.obj.prop1, ["a"])

    def test_variable_factory_arguments(self):
        class Class(object):
            __table__ = "test", "name"

        for func, cls, value in [
                               (Bool, BoolVariable, True),
                               (Int, IntVariable, 1),
                               (Float, FloatVariable, 1.1),
                               (Str, StrVariable, "str"),
                               (Unicode, UnicodeVariable, "unicode"),
                               (DateTime, DateTimeVariable, datetime.now()),
                               (Date, DateVariable, date.today()),
                               (Time, TimeVariable, datetime.now().time()),
                               (Pickle, PickleVariable, {}),
                                     ]:

            # Test no default and not_none=False.
            prop = func(name="name")
            column = prop.__get__(None, Class)
            self.assertEquals(column.name, "name")
            self.assertEquals(column.table, Class)

            variable = column.variable_factory()
            self.assertTrue(isinstance(variable, cls))
            self.assertEquals(variable.get(), None)
            variable.set(None)
            self.assertEquals(variable.get(), None)

            # Test default and not_none=True.
            prop = func(name="name", default=value, not_none=True)
            column = prop.__get__(None, Class)
            self.assertEquals(column.name, "name")
            self.assertEquals(column.table, Class)

            variable = column.variable_factory()
            self.assertTrue(isinstance(variable, cls))
            self.assertRaises(NotNoneError, variable.set, None)
            self.assertEquals(variable.get(), value)

            # Test default_factory.
            prop = func(name="name", default_factory=lambda:value)
            column = prop.__get__(None, Class)
            self.assertEquals(column.name, "name")
            self.assertEquals(column.table, Class)

            variable = column.variable_factory()
            self.assertTrue(isinstance(variable, cls))
            self.assertEquals(variable.get(), value)
