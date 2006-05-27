from datetime import datetime, date, time

from storm.properties import *
from storm.kinds import *
from storm.expr import Column, compile

from tests.helper import TestHelper


class DecorateKind(object):

    def to_python(self, value):
        return "to", value

    def from_python(self, value):
        return "from", value


class PropertyTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        self.kind = DecorateKind()
        class Class(object):
            __table__ = "table", "prop1"
            prop1 = Property("column1", self.kind)
            prop2 = Property()
        class SubClass(Class):
            __table__ = "subtable", "prop1"
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
        self.assertEquals(self.Class.prop1.table, "table")
        self.assertEquals(self.Class.prop2.table, "table")

    def test_auto_table_subclass(self):
        self.assertEquals(self.Class.prop1.table, "table")
        self.assertEquals(self.Class.prop2.table, "table")
        self.assertEquals(self.SubClass.prop1.table, "subtable")
        self.assertEquals(self.SubClass.prop2.table, "subtable")

    def test_auto_table_subclass_reverse_initialization(self):
        self.assertEquals(self.SubClass.prop1.table, "subtable")
        self.assertEquals(self.SubClass.prop2.table, "subtable")
        self.assertEquals(self.Class.prop1.table, "table")
        self.assertEquals(self.Class.prop2.table, "table")

    def test_kind(self):
        self.assertEquals(self.Class.prop1.kind, self.kind)
        self.assertTrue(isinstance(self.Class.prop2.kind, AnyKind))
        self.assertEquals(self.SubClass.prop1.kind, self.kind)
        self.assertTrue(isinstance(self.SubClass.prop2.kind, AnyKind))

    def test_set_get(self):
        obj = self.Class()
        obj.prop1 = 10
        obj.prop2 = 20
        self.assertEquals(obj.prop1, ("to", ("from", 10)))
        self.assertEquals(obj.prop2, 20)

    def test_set_get_subclass(self):
        obj = self.SubClass()
        obj.prop1 = 10
        obj.prop2 = 20
        self.assertEquals(obj.prop1, ("to", ("from", 10)))
        self.assertEquals(obj.prop2, 20)

    def test_set_get_explicitly(self):
        obj = self.Class()
        prop1 = self.Class.prop1
        prop2 = self.Class.prop2
        prop1.__set__(obj, 10)
        prop2.__set__(obj, 20)
        self.assertEquals(prop1.__get__(obj), ("to", ("from", 10)))
        self.assertEquals(prop2.__get__(obj), 20)

    def test_set_get_explicitly_default(self):
        obj = self.Class()
        prop1 = self.SubClass.prop1
        prop2 = self.SubClass.prop2
        self.assertEquals(prop1.__get__(obj, default=30), 30)
        self.assertEquals(prop2.__get__(obj, default=30), 30)
        prop1.__set__(obj, 10)
        prop2.__set__(obj, 20)
        self.assertEquals(prop1.__get__(obj, default=30), ("to", ("from", 10)))
        self.assertEquals(prop2.__get__(obj, default=30), 20)

    def test_set_get_subclass_explicitly(self):
        obj = self.SubClass()
        prop1 = self.SubClass.prop1
        prop2 = self.SubClass.prop2
        prop1.__set__(obj, 10)
        prop2.__set__(obj, 20)
        self.assertEquals(prop1.__get__(obj), ("to", ("from", 10)))
        self.assertEquals(prop2.__get__(obj), 20)

    def test_set_get_subclass_explicitly_default(self):
        obj = self.SubClass()
        prop1 = self.SubClass.prop1
        prop2 = self.SubClass.prop2
        self.assertEquals(prop1.__get__(obj, default=30), 30)
        self.assertEquals(prop2.__get__(obj, default=30), 30)
        prop1.__set__(obj, 10)
        prop2.__set__(obj, 20)
        self.assertEquals(prop1.__get__(obj, default=30), ("to", ("from", 10)))
        self.assertEquals(prop2.__get__(obj, default=30), 20)

    def test_delete(self):
        obj = self.Class()
        obj.prop1 = 10
        obj.prop2 = 20
        del obj.prop1
        del obj.prop2
        self.assertEquals(obj.prop1, None)
        self.assertEquals(obj.prop2, None)

    def test_delete_subclass(self):
        obj = self.SubClass()
        obj.prop1 = 10
        obj.prop2 = 20
        del obj.prop1
        del obj.prop2
        self.assertEquals(obj.prop1, None)
        self.assertEquals(obj.prop2, None)

    def test_delete_explicitly(self):
        obj = self.Class()
        obj.prop1 = 10
        obj.prop2 = 20
        self.Class.prop1.__delete__(obj)
        self.Class.prop2.__delete__(obj)
        self.assertEquals(obj.prop1, None)
        self.assertEquals(obj.prop2, None)

    def test_delete_subclass_explicitly(self):
        obj = self.SubClass()
        obj.prop1 = 10
        obj.prop2 = 20
        self.SubClass.prop1.__delete__(obj)
        self.SubClass.prop2.__delete__(obj)
        self.assertEquals(obj.prop1, None)
        self.assertEquals(obj.prop2, None)

    def test_comparable_expr(self):
        prop1 = self.Class.prop1
        prop2 = self.Class.prop2
        expr = (prop1 == "value1") & (prop2 == "value2")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "table.column1 = ? AND "
                                     "table.prop2 = ?")
        self.assertEquals(parameters, ["value1", "value2"])

    def test_comparable_expr_subclass(self):
        prop1 = self.SubClass.prop1
        prop2 = self.SubClass.prop2
        expr = (prop1 == "value1") & (prop2 == "value2")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "subtable.column1 = ? AND "
                                     "subtable.prop2 = ?")
        self.assertEquals(parameters, ["value1", "value2"])


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
        return self.SubClass.prop1, self.SubClass.prop2

    def test_bool(self):
        prop1, prop2 = self.setup(Bool)

        self.assertTrue(isinstance(prop1, Column))
        self.assertTrue(isinstance(prop2, Column))
        self.assertEquals(prop1.name, "column1")
        self.assertEquals(prop1.table, "table")
        self.assertEquals(prop2.name, "prop2")
        self.assertEquals(prop2.table, "table")
        self.assertTrue(isinstance(prop1.kind, BoolKind))
        self.assertTrue(isinstance(prop2.kind, BoolKind))

        self.obj.prop1 = 1
        self.assertTrue(self.obj.prop1 is True)
        self.obj.prop1 = 0
        self.assertTrue(self.obj.prop1 is False)

    def test_int(self):
        prop1, prop2 = self.setup(Int)

        self.assertTrue(isinstance(prop1, Column))
        self.assertTrue(isinstance(prop2, Column))
        self.assertEquals(prop1.name, "column1")
        self.assertEquals(prop1.table, "table")
        self.assertEquals(prop2.name, "prop2")
        self.assertEquals(prop2.table, "table")
        self.assertTrue(isinstance(prop1.kind, IntKind))
        self.assertTrue(isinstance(prop2.kind, IntKind))

        self.obj.prop1 = False
        self.assertEquals(self.obj.prop1, 0)
        self.obj.prop1 = True
        self.assertEquals(self.obj.prop1, 1)

    def test_float(self):
        prop1, prop2 = self.setup(Float)

        self.assertTrue(isinstance(prop1, Column))
        self.assertTrue(isinstance(prop2, Column))
        self.assertEquals(prop1.name, "column1")
        self.assertEquals(prop1.table, "table")
        self.assertEquals(prop2.name, "prop2")
        self.assertEquals(prop2.table, "table")
        self.assertTrue(isinstance(prop1.kind, FloatKind))
        self.assertTrue(isinstance(prop2.kind, FloatKind))

        self.obj.prop1 = 1
        self.assertTrue(isinstance(self.obj.prop1, float))

    def test_str(self):
        prop1, prop2 = self.setup(Str)

        self.assertTrue(isinstance(prop1, Column))
        self.assertTrue(isinstance(prop2, Column))
        self.assertEquals(prop1.name, "column1")
        self.assertEquals(prop1.table, "table")
        self.assertEquals(prop2.name, "prop2")
        self.assertEquals(prop2.table, "table")
        self.assertTrue(isinstance(prop1.kind, StrKind))
        self.assertTrue(isinstance(prop2.kind, StrKind))

        self.obj.prop1 = u"str"
        self.assertTrue(isinstance(self.obj.prop1, str))

    def test_unicode(self):
        prop1, prop2 = self.setup(Unicode)

        self.assertTrue(isinstance(prop1, Column))
        self.assertTrue(isinstance(prop2, Column))
        self.assertEquals(prop1.name, "column1")
        self.assertEquals(prop1.table, "table")
        self.assertEquals(prop2.name, "prop2")
        self.assertEquals(prop2.table, "table")
        self.assertTrue(isinstance(prop1.kind, UnicodeKind))
        self.assertTrue(isinstance(prop2.kind, UnicodeKind))

        self.obj.prop1 = "unicode"
        self.assertTrue(isinstance(self.obj.prop1, unicode))

    def test_unicode_encoding(self):
        encoding = "iso-8859-1"

        prop1, prop2 = self.setup(Unicode, encoding)

        self.assertEquals(prop1.name, "column1")
        self.assertEquals(prop1.table, "table")
        self.assertEquals(prop1.kind.encoding, encoding)

        prop1, prop2 = self.setup(Unicode, encoding=encoding)

        self.assertEquals(prop1.name, "column1")
        self.assertEquals(prop1.table, "table")
        self.assertEquals(prop1.kind.encoding, encoding)

    def test_datetime(self):
        prop1, prop2 = self.setup(DateTime)

        self.assertTrue(isinstance(prop1, Column))
        self.assertTrue(isinstance(prop2, Column))
        self.assertEquals(prop1.name, "column1")
        self.assertEquals(prop1.table, "table")
        self.assertEquals(prop2.name, "prop2")
        self.assertEquals(prop2.table, "table")
        self.assertTrue(isinstance(prop1.kind, DateTimeKind))
        self.assertTrue(isinstance(prop2.kind, DateTimeKind))

        self.obj.prop1 = 0.0
        self.assertEquals(self.obj.prop1, datetime.utcfromtimestamp(0))
        self.obj.prop1 = datetime(2006, 1, 1, 12, 34)
        self.assertEquals(self.obj.prop1, datetime(2006, 1, 1, 12, 34))

        self.assertRaises(TypeError, setattr, self.obj, "prop1", object())

    def test_date(self):
        prop1, prop2 = self.setup(Date)

        self.assertTrue(isinstance(prop1, Column))
        self.assertTrue(isinstance(prop2, Column))
        self.assertEquals(prop1.name, "column1")
        self.assertEquals(prop1.table, "table")
        self.assertEquals(prop2.name, "prop2")
        self.assertEquals(prop2.table, "table")
        self.assertTrue(isinstance(prop1.kind, DateKind))
        self.assertTrue(isinstance(prop2.kind, DateKind))

        self.obj.prop1 = datetime(2006, 1, 1, 12, 34, 56)
        self.assertEquals(self.obj.prop1, date(2006, 1, 1))
        self.obj.prop1 = date(2006, 1, 1)
        self.assertEquals(self.obj.prop1, date(2006, 1, 1))

        self.assertRaises(TypeError, setattr, self.obj, "prop1", object())

    def test_time(self):
        prop1, prop2 = self.setup(Time)

        self.assertTrue(isinstance(prop1, Column))
        self.assertTrue(isinstance(prop2, Column))
        self.assertEquals(prop1.name, "column1")
        self.assertEquals(prop1.table, "table")
        self.assertEquals(prop2.name, "prop2")
        self.assertEquals(prop2.table, "table")
        self.assertTrue(isinstance(prop1.kind, TimeKind))
        self.assertTrue(isinstance(prop2.kind, TimeKind))

        self.obj.prop1 = datetime(2006, 1, 1, 12, 34, 56)
        self.assertEquals(self.obj.prop1, time(12, 34, 56))
        self.obj.prop1 = time(12, 34, 56)
        self.assertEquals(self.obj.prop1, time(12, 34, 56))

        self.assertRaises(TypeError, setattr, self.obj, "prop1", object())
