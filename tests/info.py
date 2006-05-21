from storm.properties import Property
from storm.expr import Undef
from storm.info import *

from tests.helpers import TestHelper


class GetTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __table__ = "table", "column1"
            prop1 = Property("column1")
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

    def test_columns(self):
        self.assertEquals(self.cls_info.columns,
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

        # Can't use == for props, since they're columns.
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

    def test_has_value(self):
        self.assertEquals(self.obj_info.has_value("column1"), False)
        self.obj.prop1 = None
        self.assertEquals(self.obj_info.has_value("column1"), True)
        del self.obj.prop1
        self.assertEquals(self.obj_info.has_value("column1"), False)

    def test_get_value(self):
        self.assertEquals(self.obj_info.get_value("column1"), None)
        self.assertEquals(self.obj_info.get_value("column1", Undef), Undef)
        self.obj.prop1 = None
        self.assertEquals(self.obj_info.get_value("column1"), None)
        self.assertEquals(self.obj_info.get_value("column1", Undef), None)

    def test_set_value(self):
        self.obj_info.set_value("column1", 10)
        self.assertEquals(self.obj.prop1, 10)

    def test_del_value(self):
        self.obj_info.set_value("column1", 10)
        self.assertEquals(self.obj.prop1, 10)
        self.obj_info.del_value("column1")
        self.assertEquals(self.obj.prop1, None)
        self.obj_info.del_value("column1")
        self.assertEquals(self.obj.prop1, None)

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

    def test_save_attributes(self):
        self.obj.prop1 = 10
        self.obj.attr1 = 100
        self.obj_info.save()
        self.obj.prop1 = 20
        self.obj.attr1 = 200
        self.obj_info.save_attributes()
        self.obj_info.restore()
        self.assertEquals(self.obj.prop1, 10)
        self.assertEquals(self.obj.attr1, 200)

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
        self.assertEquals(self.obj_info.get_changes(), {"column1": 10})
        self.obj_info.save()
        self.obj.prop1 = 20
        self.assertEquals(self.obj_info.get_changes(), {"column1": 20})
        self.obj.prop1 = None
        self.assertEquals(self.obj_info.get_changes(), {"column1": None})
        del self.obj.prop1
        self.assertEquals(self.obj_info.get_changes(), {"column1": Undef})
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

        self.assertEquals(changes, [(self.obj_info, "column2", Undef, 10),
                                    (self.obj_info, "column1", Undef, 20)])

        del changes[:]

        self.obj.prop1 = None
        self.obj.prop2 = None

        self.assertEquals(changes, [(self.obj_info, "column1", 20, None),
                                    (self.obj_info, "column2", 10, None)])

        del changes[:]

        del self.obj.prop1
        del self.obj.prop2

        self.assertEquals(changes, [(self.obj_info, "column1", None, Undef),
                                    (self.obj_info, "column2", None, Undef)])
