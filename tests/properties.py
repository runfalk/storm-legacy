from datetime import datetime

from storm.properties import *
from storm.expr import compile

from tests.helpers import TestHelper


class ClassInfoTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __table__ = "table", "column1"
            prop1 = Property("column1")
            prop2 = Property("column2")
        self.Class = Class
        self.info = ClassInfo(Class)

    def test_singleton(self):
        self.assertTrue(self.info is ClassInfo(self.Class))

    def test_cls(self):
        self.assertEquals(self.info.cls, self.Class)

    def test_properties(self):
        self.assertEquals(self.info.properties,
                          (self.Class.prop1, self.Class.prop2))

    def test_attributes(self):
        self.assertEquals(self.info.attributes, ("prop1", "prop2"))

    def test_columns(self):
        self.assertEquals([(column.name, column.table)
                           for column in self.info.columns],
                          [("column1", None), ("column2", None)])

    def test_tables(self):
        self.assertEquals(self.info.tables, ("table",))

    def test_table(self):
        self.assertEquals(self.info.table, "table")

    def test_primary_key(self):
        # Can't use == for props.
        self.assertTrue(self.info.primary_key[0] is self.Class.prop1)
        self.assertEquals(len(self.info.primary_key), 1)

    def test_primary_key_composed(self):
        class Class(object):
            __table__ = "table", ("column2", "column1")
            prop1 = Property("column1")
            prop2 = Property("column2")
        info = ClassInfo(Class)

        # Can't use == for props.
        self.assertTrue(info.primary_key[0] is Class.prop2)
        self.assertTrue(info.primary_key[1] is Class.prop1)
        self.assertEquals(len(info.primary_key), 2)

    def test_primary_key_pos(self):
        class Class(object):
            __table__ = "table", ("column3", "column1")
            prop1 = Property("column1")
            prop2 = Property("column2")
            prop3 = Property("column3")
        info = ClassInfo(Class)
        self.assertEquals(info.primary_key_pos, (2, 0))


class ObjectInfoTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __table__ = "table", "column1"
            prop1 = Property("column1")
            prop2 = Property("column2")
        self.Class = Class
        self.obj = Class()

    def test_singleton(self):
        self.assertTrue(ObjectInfo(self.obj) is ObjectInfo(self.obj))

    def test_save_restore(self):
        info = ObjectInfo(self.obj)
        self.obj.prop1 = 10
        self.obj.attr1 = 100
        info.save()
        self.assertEquals(self.obj.prop1, 10)
        self.assertEquals(self.obj.attr1, 100)
        self.obj.prop1 = 20
        self.obj.attr1 = 200
        self.assertEquals(self.obj.prop1, 20)
        self.assertEquals(self.obj.attr1, 200)
        info.restore()
        self.assertEquals(self.obj.prop1, 10)
        self.assertEquals(self.obj.attr1, 100)

    def test_check_changed(self):
        info = ObjectInfo(self.obj)
        self.obj.prop1 = 10
        self.obj.attr1 = 100
        info.save()
        self.assertEquals(info.check_changed(), False)
        self.obj.attr1 = 200
        self.assertEquals(info.check_changed(), False)
        self.obj.prop1 = 20
        self.assertEquals(info.check_changed(), True)
        info.restore()
        self.assertEquals(info.check_changed(), False)

    def test_get_changes(self):
        info = ObjectInfo(self.obj)
        info.save()
        self.assertEquals(info.get_changes(), {})
        self.obj.prop1 = 10
        self.obj.attr1 = 100
        self.assertEquals(info.get_changes(), {self.Class.prop1: 10})
        info.save()
        self.obj.prop1 = 20
        self.assertEquals(info.get_changes(), {self.Class.prop1: 20})
        self.obj.prop1 = None
        self.assertEquals(info.get_changes(), {self.Class.prop1: None})
        self.obj.prop1 = 10
        self.assertEquals(info.get_changes(), {})
        info.restore()
        self.assertEquals(info.get_changes(), {})
        self.obj.prop1 = None
        self.assertEquals(info.get_changes(), {})

    def test_notify_changes(self):
        info = ObjectInfo(self.obj)
        
        changes = []
        def object_changed(obj, prop, old_value, new_value):
            changes.append((obj, prop, old_value, new_value))

        info.set_change_notification(object_changed)
        info.save()

        self.obj.prop2 = 10
        self.obj.prop1 = 20

        self.assertEquals(changes, [(self.obj, self.Class.prop2, None, 10),
                                    (self.obj, self.Class.prop1, None, 20)])

        del changes[:]

        self.obj.prop1 = None
        self.obj.prop2 = None

        self.assertEquals(changes, [(self.obj, self.Class.prop1, 20, None),
                                    (self.obj, self.Class.prop2, 10, None)])

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
            prop1 = Property()
            prop2 = Property()
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
