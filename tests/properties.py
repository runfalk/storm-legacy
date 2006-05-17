from datetime import datetime

from storm.properties import *
from storm.expr import compile

from tests.helpers import TestHelper


class ClassInfoTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __table__ = "table", "prop1"
            prop1 = Property("prop1")
            prop2 = Property("prop2")
        self.Class = Class
        self.info = ClassInfo(Class)

    def test_singleton(self):
        self.assertTrue(self.info is ClassInfo(self.Class))

    def test_table(self):
        self.assertTrue(self.info.table, "table")

    def test_primary_key(self):
        self.assertTrue(self.info.primary_key[0] is self.Class.prop1)

    def test_primary_key_composed(self):
        class Class(object):
            __table__ = "table", ("prop2", "prop1")
            prop1 = Property("prop1")
            prop2 = Property("prop2")
        info = ClassInfo(Class)
        self.assertTrue(info.primary_key[0] is Class.prop2)
        self.assertTrue(info.primary_key[1] is Class.prop1)

    def test_prop_insts(self):
        self.assertEquals(self.info.prop_insts,
                          (self.Class.prop1, self.Class.prop2))

    def test_prop_names(self):
        self.assertEquals(self.info.prop_names, ("prop1", "prop2"))


class ObjectInfoTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __table__ = "table", "prop1"
            prop1 = Property("prop1")
            prop2 = Property("prop2")
        self.Class = Class
        self.obj = Class()

    def test_singleton(self):
        self.assertTrue(ObjectInfo(self.obj) is ObjectInfo(self.obj))

    def test_push_pop_state(self):
        info = ObjectInfo(self.obj)
        self.obj.prop1 = 10
        self.obj.attr1 = 100
        self.assertEquals(self.obj.prop1, 10)
        self.assertEquals(self.obj.attr1, 100)
        info.push_state()
        self.assertEquals(self.obj.prop1, 10)
        self.assertEquals(self.obj.attr1, 100)
        self.obj.prop1 = 20
        self.obj.attr1 = 200
        self.assertEquals(self.obj.prop1, 20)
        self.assertEquals(self.obj.attr1, 200)
        info.pop_state()
        self.assertEquals(self.obj.prop1, 10)
        self.assertEquals(self.obj.attr1, 100)

    def test_check_changed(self):
        info = ObjectInfo(self.obj)
        self.assertEquals(info.check_changed(), None)
        self.obj.prop1 = 10
        self.obj.attr1 = 100
        self.assertEquals(info.check_changed(), None)
        info.push_state()
        self.assertEquals(info.check_changed(props=True), False)
        self.assertEquals(info.check_changed(attrs=True), False)
        self.obj.attr1 = 200
        self.assertEquals(info.check_changed(props=True), False)
        self.assertEquals(info.check_changed(attrs=True), True)
        self.obj.prop1 = 20
        self.assertEquals(info.check_changed(props=True), True)
        self.assertEquals(info.check_changed(attrs=True), True)
        info.pop_state()
        self.assertEquals(info.check_changed(props=True), None)
        self.assertEquals(info.check_changed(attrs=True), None)


class PropertyTest(TestHelper):

    def test_name(self):
        prop = Property("name")
        self.assertEquals(prop.name, "name")

    def test_name_auto(self):
        class Class(object):
            __table__ = "table", "foobar"
            foobar = Property()
        self.assertEquals(Class.foobar.name, "foobar")

    def test_set_get(self):
        class Class(object):
            __table__ = "table", "prop1"
            prop1 = Property("prop1")
            prop2 = Property("prop2")
        obj = Class()
        obj.prop1 = 10
        obj.prop2 = 20
        self.assertEquals(obj.prop1, 10)
        self.assertEquals(obj.prop2, 20)

    def test_comparable_expr(self):
        class Class(object):
            __table__ = "table", "prop"
            prop = Property()
        expr = Class.prop == "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(table.prop = ?)")
        self.assertEquals(parameters, ["value"])


class PropertyTypesTest(TestHelper):

    def test_bool(self):
        class Class(object):
            __table__ = "table", "prop"
            prop = Bool()
        obj = Class()
        obj.prop = 1
        self.assertEquals(obj.prop, True)
        obj.prop = 0
        self.assertEquals(obj.prop, False)

    def test_int(self):
        class Class(object):
            __table__ = "table", "prop"
            prop = Int()
        obj = Class()
        obj.prop = False
        self.assertEquals(obj.prop, 0)
        obj.prop = True
        self.assertEquals(obj.prop, 1)

    def test_float(self):
        class Class(object):
            __table__ = "table", "prop"
            prop = Float()
        obj = Class()
        obj.prop = 1
        self.assertTrue(isinstance(obj.prop, float))

    def test_str(self):
        class Class(object):
            __table__ = "table", "prop"
            prop = Str()
        obj = Class()
        obj.prop = u"str"
        self.assertTrue(isinstance(obj.prop, str))

    def test_unicode(self):
        class Class(object):
            __table__ = "table", "prop"
            prop = Unicode()
        obj = Class()
        obj.prop = "unicode"
        self.assertTrue(isinstance(obj.prop, unicode))

    def test_datetime(self):
        class Class(object):
            __table__ = "table", "prop"
            prop = DateTime()
        obj = Class()
        obj.prop = 0.0
        self.assertEquals(obj.prop, datetime.utcfromtimestamp(0))
        obj.prop = datetime(2006, 1, 1)
        self.assertEquals(obj.prop, datetime(2006, 1, 1))
        self.assertRaises(TypeError, setattr, obj, "prop", object())
