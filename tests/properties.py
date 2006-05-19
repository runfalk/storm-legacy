from datetime import datetime

from storm.properties import *
from storm.expr import compile

from tests.helpers import TestHelper


class GetTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __table__ = "table", "column1"
            prop1 = Property("column1")
            prop2 = Property("column2")
        self.Class = Class
        self.obj = Class()

    def test_get_cls_info(self):
        cls_info = get_cls_info(self.Class)
        self.assertTrue(isinstance(cls_info, ClassInfo))
        self.assertTrue(cls_info is get_cls_info(self.Class))

    def test_get_obj_info(self):
        obj_info = get_obj_info(self.obj)
        self.assertTrue(isinstance(obj_info, ObjectInfo))
        self.assertTrue(obj_info is get_obj_info(self.obj))

    def test_get_info(self):
        obj_info1, cls_info1 = get_info(self.obj)
        obj_info2, cls_info2 = get_info(self.obj)
        self.assertTrue(isinstance(obj_info1, ObjectInfo))
        self.assertTrue(isinstance(cls_info1, ClassInfo))
        self.assertTrue(obj_info1 is obj_info2)
        self.assertTrue(cls_info1 is cls_info2)


class ClassInfoTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __table__ = "table", "column1"
            prop1 = Property("column1")
            prop2 = Property("column2")
        self.Class = Class
        self.cls_info = get_cls_info(Class)

    def test_cls(self):
        self.assertEquals(self.cls_info.cls, self.Class)

    def test_properties(self):
        self.assertEquals(self.cls_info.properties,
                          (self.Class.prop1, self.Class.prop2))

    def test_attributes(self):
        self.assertEquals(self.cls_info.attributes, ("prop1", "prop2"))

    def test_table(self):
        self.assertEquals(self.cls_info.table, "table")

    def test_primary_key(self):
        # Can't use == for props.
        self.assertTrue(self.cls_info.primary_key[0] is self.Class.prop1)
        self.assertEquals(len(self.cls_info.primary_key), 1)

    def test_primary_key_composed(self):
        class Class(object):
            __table__ = "table", ("column2", "column1")
            prop1 = Property("column1")
            prop2 = Property("column2")
        cls_info = ClassInfo(Class)

        # Can't use == for props.
        self.assertTrue(cls_info.primary_key[0] is Class.prop2)
        self.assertTrue(cls_info.primary_key[1] is Class.prop1)
        self.assertEquals(len(cls_info.primary_key), 2)

    def test_primary_key_pos(self):
        class Class(object):
            __table__ = "table", ("column3", "column1")
            prop1 = Property("column1")
            prop2 = Property("column2")
            prop3 = Property("column3")
        cls_info = ClassInfo(Class)
        self.assertEquals(cls_info.primary_key_pos, (2, 0))


class ObjectInfoTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __table__ = "table", "column1"
            prop1 = Property("column1")
            prop2 = Property("column2")
        self.Class = Class
        self.obj = Class()
        self.obj_info = get_obj_info(self.obj)

    def test_save_restore(self):
        self.obj.prop1 = 10
        self.obj.attr1 = 100
        self.obj_info["key"] = 1000
        self.obj_info.save()
        self.assertEquals(self.obj.prop1, 10)
        self.assertEquals(self.obj.attr1, 100)
        self.assertEquals(self.obj_info.get("key"), 1000)
        self.obj.prop1 = 20
        self.obj.attr1 = 200
        self.obj_info["key"] = 2000
        self.assertEquals(self.obj.prop1, 20)
        self.assertEquals(self.obj.attr1, 200)
        self.assertEquals(self.obj_info.get("key"), 2000)
        self.obj_info.restore()
        self.assertEquals(self.obj.prop1, 10)
        self.assertEquals(self.obj.attr1, 100)
        self.assertEquals(self.obj_info.get("key"), 1000)

    def test_check_changed(self):
        self.obj.prop1 = 10
        self.obj.attr1 = 100
        self.obj_info.save()
        self.assertEquals(self.obj_info.check_changed(), False)
        self.obj.attr1 = 200
        self.assertEquals(self.obj_info.check_changed(), False)
        self.obj.prop1 = 20
        self.assertEquals(self.obj_info.check_changed(), True)
        self.obj_info.restore()
        self.assertEquals(self.obj_info.check_changed(), False)

    def test_get_changes(self):
        self.obj_info.save()
        self.assertEquals(self.obj_info.get_changes(), {})
        self.obj.prop1 = 10
        self.obj.attr1 = 100
        self.assertEquals(self.obj_info.get_changes(), {self.Class.prop1: 10})
        self.obj_info.save()
        self.obj.prop1 = 20
        self.assertEquals(self.obj_info.get_changes(), {self.Class.prop1: 20})
        self.obj.prop1 = None
        self.assertEquals(self.obj_info.get_changes(),
                          {self.Class.prop1: None})
        self.obj.prop1 = 10
        self.assertEquals(self.obj_info.get_changes(), {})
        self.obj_info.restore()
        self.assertEquals(self.obj_info.get_changes(), {})
        self.obj.prop1 = 10
        self.assertEquals(self.obj_info.get_changes(), {})

    def test_notify_changes(self):
        changes = []
        def object_changed(obj, prop, old_value, new_value):
            changes.append((obj, prop, old_value, new_value))

        self.obj_info.set_change_notification(object_changed)
        self.obj_info.save()

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
        self.assertEquals(statement, "table.prop = ?")
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
