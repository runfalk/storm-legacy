from datetime import datetime

from storm.properties import *
from storm.expr import compile

from tests.helpers import TestHelper


class PropertyTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __table__ = "table", "prop1"
            prop1 = Property()
            prop2 = Property()
        class SubClass(Class):
            __table__ = "subtable", "prop1"
        self.Class = Class
        self.SubClass = SubClass

    def test_name(self):
        class Class(object):
            __table__ = "table", "column1"
            prop = Property("column1")
        self.assertEquals(Class.prop.name, "column1")

    def test_auto_name(self):
        self.assertEquals(self.Class.prop1.name, "prop1")

    def test_auto_table(self):
        self.assertEquals(self.Class.prop1.table, "table")

    def test_auto_table_subclass(self):
        self.assertEquals(self.Class.prop1.table, "table")
        self.assertEquals(self.SubClass.prop1.table, "subtable")

    def test_auto_table_subclass_reverse_initialization(self):
        self.assertEquals(self.SubClass.prop1.table, "subtable")
        self.assertEquals(self.Class.prop1.table, "table")

    def test_set_get(self):
        obj = self.Class()
        obj.prop1 = 10
        obj.prop2 = 20
        self.assertEquals(obj.prop1, 10)
        self.assertEquals(obj.prop2, 20)

    def test_set_get_subclass(self):
        obj = self.SubClass()
        obj.prop1 = 10
        obj.prop2 = 20
        self.assertEquals(obj.prop1, 10)
        self.assertEquals(obj.prop2, 20)

    def test_del(self):
        obj = self.Class()
        obj.prop1 = 10
        del obj.prop1
        self.assertEquals(obj.prop1, None)

    def test_del_subclass(self):
        obj = self.SubClass()
        obj.prop1 = 10
        del obj.prop1
        self.assertEquals(obj.prop1, None)

    def test_comparable_expr(self):
        expr = self.Class.prop1 == "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "table.prop1 = ?")
        self.assertEquals(parameters, ["value"])

    def test_comparable_expr_subclass(self):
        expr = self.SubClass.prop1 == "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "subtable.prop1 = ?")
        self.assertEquals(parameters, ["value"])

    def test__get__(self):
        obj = self.SubClass()
        obj.prop1 = 10
        self.assertEquals(self.SubClass.prop1.__get__(obj), 10)

    def test__set__(self):
        obj = self.SubClass()
        self.SubClass.prop1.__set__(obj, 10)
        self.assertEquals(self.SubClass.prop1, 10)

    def test__del__(self):
        obj = self.SubClass()
        obj.prop1 = 10
        self.SubClass.prop1.__delete__(obj)
        self.assertEquals(self.SubClass.prop1, None)


class PropertyTypesTest(TestHelper):

    def setup(self, property):
        class Class(object):
            __table__ = "table", "prop"
            prop = property
        class SubClass(Class):
            pass
        self.Class = Class
        self.SubClass = SubClass
        self.obj = SubClass()

    def test_bool(self):
        self.setup(Bool())
        self.obj.prop = 1
        self.assertTrue(self.obj.prop is True)
        self.obj.prop = 0
        self.assertTrue(self.obj.prop is False)

    def test_int(self):
        self.setup(Int())
        self.obj.prop = False
        self.assertEquals(self.obj.prop, 0)
        self.obj.prop = True
        self.assertEquals(self.obj.prop, 1)

    def test_float(self):
        self.setup(Float())
        self.obj.prop = 1
        self.assertTrue(isinstance(self.obj.prop, float))

    def test_str(self):
        self.setup(Str())
        self.obj.prop = u"str"
        self.assertTrue(isinstance(self.obj.prop, str))

    def test_unicode(self):
        self.setup(Unicode())
        self.obj.prop = "unicode"
        self.assertTrue(isinstance(self.obj.prop, unicode))

    def test_datetime(self):
        self.setup(DateTime())
        self.obj.prop = 0.0
        self.assertEquals(self.obj.prop, datetime.utcfromtimestamp(0))
        self.obj.prop = datetime(2006, 1, 1)
        self.assertEquals(self.obj.prop, datetime(2006, 1, 1))
        self.assertRaises(TypeError, setattr, self.obj, "prop", object())
