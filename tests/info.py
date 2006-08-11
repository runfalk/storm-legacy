from storm.properties import Property
from storm.variables import Variable
from storm.expr import Undef, Select, compile
from storm.info import *

from tests.helper import TestHelper


def Any(name=None, **kwargs):
    return Property(name, cls=Variable, **kwargs)


class GetTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __table__ = "table", "column1"
            prop1 = Any("column1")
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
            prop1 = Any("column1")
            prop2 = Any("column2")
        self.Class = Class
        self.cls_info = get_cls_info(Class)

    def test_cls(self):
        self.assertEquals(self.cls_info.cls, self.Class)

    def test_columns(self):
        self.assertEquals(self.cls_info.columns,
                          (self.Class.prop1, self.Class.prop2))

    def test_table(self):
        self.assertEquals(self.cls_info.table, "table")

    def test_primary_key(self):
        # Can't use == for props.
        self.assertTrue(self.cls_info.primary_key[0] is self.Class.prop1)
        self.assertEquals(len(self.cls_info.primary_key), 1)

    def test_primary_key_composed(self):
        class Class(object):
            __table__ = "table", ("column2", "column1")
            prop1 = Any("column1")
            prop2 = Any("column2")
        cls_info = ClassInfo(Class)

        # Can't use == for props, since they're columns.
        self.assertTrue(cls_info.primary_key[0] is Class.prop2)
        self.assertTrue(cls_info.primary_key[1] is Class.prop1)
        self.assertEquals(len(cls_info.primary_key), 2)

    def test_primary_key_pos(self):
        class Class(object):
            __table__ = "table", ("column3", "column1")
            prop1 = Any("column1")
            prop2 = Any("column2")
            prop3 = Any("column3")
        cls_info = ClassInfo(Class)
        self.assertEquals(cls_info.primary_key_pos, (2, 0))


class ObjectInfoTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __table__ = "table", "column1"
            prop1 = Any("column1")
            prop2 = Any("column2")
        self.Class = Class
        self.obj = Class()
        self.obj_info = get_obj_info(self.obj)
        self.cls_info = get_cls_info(Class)
        self.variable1 = self.obj_info.variables[Class.prop1]
        self.variable2 = self.obj_info.variables[Class.prop2]


    def test_dict_subclass(self):
        self.assertTrue(isinstance(self.obj_info, dict))

    def test_variables(self):
        self.assertTrue(isinstance(self.obj_info.variables, dict))
        
        for column in self.cls_info.columns:
            variable = self.obj_info.variables.get(column)
            self.assertTrue(isinstance(variable, Variable))

        self.assertEquals(len(self.obj_info.variables),
                          len(self.cls_info.columns))

    def test_primary_vars(self):
        self.assertTrue(isinstance(self.obj_info.primary_vars, tuple))
        
        for column, variable in zip(self.cls_info.primary_key,
                                    self.obj_info.primary_vars):
            self.assertEquals(self.obj_info.variables.get(column),
                              variable)

        self.assertEquals(len(self.obj_info.primary_vars),
                          len(self.cls_info.primary_key))

    def test_save_restore(self):
        self.obj.prop1 = 10
        self.obj.attr1 = 100
        self.obj_info["key"] = {"subkey": 1000}
        self.obj_info.save()
        self.assertEquals(self.obj.prop1, 10)
        self.assertEquals(self.obj.attr1, 100)
        self.assertEquals(self.obj_info["key"]["subkey"], 1000)
        self.obj.prop1 = 20
        self.obj.attr1 = 200
        self.obj_info["key"]["subkey"] = 2000
        self.assertEquals(self.obj.prop1, 20)
        self.assertEquals(self.obj.attr1, 200)
        self.assertEquals(self.obj_info["key"]["subkey"], 2000)
        self.obj_info.restore()
        self.assertEquals(self.obj.prop1, 10)
        self.assertEquals(self.obj.attr1, 100)
        self.assertEquals(self.obj_info["key"]["subkey"], 1000)

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

    def test_checkpoint(self):
        self.obj.prop1 = 10
        self.obj_info.save()
        self.assertEquals(self.obj.prop1, 10)
        self.assertEquals(self.variable1.has_changed(), False)
        self.obj.prop1 = 20
        self.assertEquals(self.obj.prop1, 20)
        self.assertEquals(self.variable1.has_changed(), True)
        self.obj_info.checkpoint()
        self.assertEquals(self.obj.prop1, 20)
        self.assertEquals(self.variable1.has_changed(), False)
        self.obj_info.restore()
        self.assertEquals(self.obj.prop1, 10)
        self.assertEquals(self.variable1.has_changed(), False)
        self.obj.prop1 = 20
        self.assertEquals(self.obj.prop1, 20)
        self.assertEquals(self.variable1.has_changed(), True)

    def test_add_change_notification(self):
        changes1 = []
        changes2 = []
        def object_changed1(obj_info, variable, old_value, new_value):
            changes1.append((1, obj_info, variable, old_value, new_value))
        def object_changed2(obj_info, variable, old_value, new_value):
            changes2.append((2, obj_info, variable, old_value, new_value))

        self.obj_info.save()
        self.obj_info.event.hook("changed", object_changed1)
        self.obj_info.event.hook("changed", object_changed2)

        self.obj.prop2 = 10
        self.obj.prop1 = 20

        self.assertEquals(changes1,
                          [(1, self.obj_info, self.variable2, Undef, 10),
                           (1, self.obj_info, self.variable1, Undef, 20)])
        self.assertEquals(changes2,
                          [(2, self.obj_info, self.variable2, Undef, 10),
                           (2, self.obj_info, self.variable1, Undef, 20)])

        del changes1[:]
        del changes2[:]

        self.obj.prop1 = None
        self.obj.prop2 = None

        self.assertEquals(changes1,
                          [(1, self.obj_info, self.variable1, 20, None),
                           (1, self.obj_info, self.variable2, 10, None)])
        self.assertEquals(changes2,
                          [(2, self.obj_info, self.variable1, 20, None),
                           (2, self.obj_info, self.variable2, 10, None)])

        del changes1[:]
        del changes2[:]

        del self.obj.prop1
        del self.obj.prop2

        self.assertEquals(changes1,
                          [(1, self.obj_info, self.variable1, None, Undef),
                           (1, self.obj_info, self.variable2, None, Undef)])
        self.assertEquals(changes2,
                          [(2, self.obj_info, self.variable1, None, Undef),
                           (2, self.obj_info, self.variable2, None, Undef)])

    def test_add_change_notification_with_arg(self):
        changes1 = []
        changes2 = []
        def object_changed1(obj_info, variable, old_value, new_value, arg):
            changes1.append((1, obj_info, variable, old_value, new_value, arg))
        def object_changed2(obj_info, variable, old_value, new_value, arg):
            changes2.append((2, obj_info, variable, old_value, new_value, arg))

        self.obj_info.save()

        obj = object()
        
        self.obj_info.event.hook("changed", object_changed1, obj)
        self.obj_info.event.hook("changed", object_changed2, obj)

        self.obj.prop2 = 10
        self.obj.prop1 = 20

        self.assertEquals(changes1,
                          [(1, self.obj_info, self.variable2, Undef, 10, obj),
                           (1, self.obj_info, self.variable1, Undef, 20, obj)])
        self.assertEquals(changes2,
                          [(2, self.obj_info, self.variable2, Undef, 10, obj),
                           (2, self.obj_info, self.variable1, Undef, 20, obj)])

        del changes1[:]
        del changes2[:]

        self.obj.prop1 = None
        self.obj.prop2 = None

        self.assertEquals(changes1,
                          [(1, self.obj_info, self.variable1, 20, None, obj),
                           (1, self.obj_info, self.variable2, 10, None, obj)])
        self.assertEquals(changes2,
                          [(2, self.obj_info, self.variable1, 20, None, obj),
                           (2, self.obj_info, self.variable2, 10, None, obj)])

        del changes1[:]
        del changes2[:]

        del self.obj.prop1
        del self.obj.prop2

        self.assertEquals(changes1,
                          [(1, self.obj_info, self.variable1, None, Undef, obj),
                           (1, self.obj_info, self.variable2, None, Undef, obj)])
        self.assertEquals(changes2,
                          [(2, self.obj_info, self.variable1, None, Undef, obj),
                           (2, self.obj_info, self.variable2, None, Undef, obj)])

    def test_remove_change_notification(self):
        changes1 = []
        changes2 = []
        def object_changed1(obj_info, variable, old_value, new_value):
            changes1.append((1, obj_info, variable, old_value, new_value))
        def object_changed2(obj_info, variable, old_value, new_value):
            changes2.append((2, obj_info, variable, old_value, new_value))

        self.obj_info.save()

        self.obj_info.event.hook("changed", object_changed1)
        self.obj_info.event.hook("changed", object_changed2)
        self.obj_info.event.unhook("changed", object_changed1)

        self.obj.prop2 = 20
        self.obj.prop1 = 10

        self.assertEquals(changes1, [])
        self.assertEquals(changes2,
                          [(2, self.obj_info, self.variable2, Undef, 20),
                           (2, self.obj_info, self.variable1, Undef, 10)])

    def test_remove_change_notification_with_arg(self):
        changes1 = []
        changes2 = []
        def object_changed1(obj_info, variable, old_value, new_value, arg):
            changes1.append((1, obj_info, variable, old_value, new_value, arg))
        def object_changed2(obj_info, variable, old_value, new_value, arg):
            changes2.append((2, obj_info, variable, old_value, new_value, arg))

        self.obj_info.save()

        obj = object()

        self.obj_info.event.hook("changed", object_changed1, obj)
        self.obj_info.event.hook("changed", object_changed2, obj)
        self.obj_info.event.unhook("changed", object_changed1, obj)

        self.obj.prop2 = 20
        self.obj.prop1 = 10

        self.assertEquals(changes1, [])
        self.assertEquals(changes2,
                          [(2, self.obj_info, self.variable2, Undef, 20, obj),
                           (2, self.obj_info, self.variable1, Undef, 10, obj)])

    def test_auto_remove_change_notification(self):
        changes1 = []
        changes2 = []
        def object_changed1(obj_info, variable, old_value, new_value):
            changes1.append((1, obj_info, variable, old_value, new_value))
            return False
        def object_changed2(obj_info, variable, old_value, new_value):
            changes2.append((2, obj_info, variable, old_value, new_value))
            return False

        self.obj_info.save()

        self.obj_info.event.hook("changed", object_changed1)
        self.obj_info.event.hook("changed", object_changed2)

        self.obj.prop2 = 20
        self.obj.prop1 = 10

        self.assertEquals(changes1,
                          [(1, self.obj_info, self.variable2, Undef, 20)])
        self.assertEquals(changes2,
                          [(2, self.obj_info, self.variable2, Undef, 20)])

    def test_auto_remove_change_notification_with_arg(self):
        changes1 = []
        changes2 = []
        def object_changed1(obj_info, variable, old_value, new_value, arg):
            changes1.append((1, obj_info, variable, old_value, new_value, arg))
            return False
        def object_changed2(obj_info, variable, old_value, new_value, arg):
            changes2.append((2, obj_info, variable, old_value, new_value, arg))
            return False

        self.obj_info.save()

        obj = object()

        self.obj_info.event.hook("changed", object_changed1, obj)
        self.obj_info.event.hook("changed", object_changed2, obj)

        self.obj.prop2 = 20
        self.obj.prop1 = 10

        self.assertEquals(changes1,
                          [(1, self.obj_info, self.variable2, Undef, 20, obj)])
        self.assertEquals(changes2,
                          [(2, self.obj_info, self.variable2, Undef, 20, obj)])

    def test_restore_hooks(self):
        changes = []
        def object_changed(obj_info, variable, old_value, new_value):
            changes.append((obj_info, variable, old_value, new_value))

        self.obj_info.event.hook("changed", object_changed)
        self.obj_info.save()
        self.obj_info.event.unhook("changed", object_changed)

        self.obj.prop1 = 10
        self.obj_info.restore()
        self.obj.prop1 = 20

        self.assertEquals(changes,
                          [(self.obj_info, self.variable1, Undef, 20)])


class ClassAliasTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __table__ = "table", "column1"
            prop1 = Any("column1")
        self.Class = Class
        self.obj = Class()
        self.ClassAlias = ClassAlias(self.Class, "alias")
        
    def test_cls_info_cls(self):
        cls_info = get_cls_info(self.ClassAlias)
        self.assertEquals(cls_info.cls, self.Class)
        self.assertEquals(cls_info.table, "alias")
        self.assertEquals(self.ClassAlias.prop1.name, "column1")
        self.assertEquals(self.ClassAlias.prop1.table, self.ClassAlias)

    def test_compile(self):
        statement, parameters = compile(self.ClassAlias)
        self.assertEquals(statement, "table AS alias")

    def test_compile_in_select(self):
        expr = Select(self.ClassAlias.prop1, self.ClassAlias.prop1 == 1,
                      self.ClassAlias)
        statement, parameters = compile(expr)
        self.assertEquals(statement,
                          "SELECT alias.column1 FROM table AS alias "
                          "WHERE alias.column1 = ?")


class TypeCompilerTest(TestHelper):

    def test_nested_classes(self):
        """Convoluted case checking that the model is right."""
        class Class1(object):
            __table__ = "class1", "id"
            id = Any()
        class Class2(object):
            __table__ = Class1, "id"
            id = Any()
        statement, parameters = compile(Class2)
        self.assertEquals(statement, "class1")
        statement, parameters = compile(ClassAlias(Class2, "alias"))
        self.assertEquals(statement, "class1 AS alias")
