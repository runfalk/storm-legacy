import gc

from storm.exceptions import ClassInfoError
from storm.properties import Property
from storm.variables import Variable
from storm.expr import Undef, Select, compile
from storm.info import *

from tests.helper import TestHelper


class Wrapper(object):

    def __init__(self, obj):
        self.obj = obj

Wrapper.__object_info = property(lambda self: self.obj.__object_info)


class GetTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __storm_table__ = "table"
            prop1 = Property("column1", primary=True)
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

    def test_get_obj_info_on_obj_info(self):
        obj_info = get_obj_info(self.obj)
        self.assertTrue(get_obj_info(obj_info) is obj_info)

    def test_set_obj_info(self):
        obj_info1 = get_obj_info(self.obj)
        obj_info2 = ObjectInfo(self.obj)
        self.assertEquals(get_obj_info(self.obj), obj_info1)
        set_obj_info(self.obj, obj_info2)
        self.assertEquals(get_obj_info(self.obj), obj_info2)

    def test_get_info(self):
        obj_info1, cls_info1 = get_info(self.obj)
        obj_info2, cls_info2 = get_info(self.obj)
        self.assertTrue(isinstance(obj_info1, ObjectInfo))
        self.assertTrue(isinstance(cls_info1, ClassInfo))
        self.assertTrue(obj_info1 is obj_info2)
        self.assertTrue(cls_info1 is cls_info2)

    def test_get_info_on_obj_info(self):
        obj_info1 = get_obj_info(self.obj)
        cls_info1 = get_cls_info(self.Class)
        obj_info2, cls_info2 = get_info(obj_info1)
        self.assertTrue(obj_info2 is obj_info1)
        self.assertTrue(cls_info2 is cls_info1)


class ClassInfoTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __storm_table__ = "table"
            prop1 = Property("column1", primary=True)
            prop2 = Property("column2")
        self.Class = Class
        self.cls_info = get_cls_info(Class)

    def test_invalid_class(self):
        class Class(object): pass
        self.assertRaises(ClassInfoError, ClassInfo, Class)

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

    def test_primary_key_with_attribute(self):
        class SubClass(self.Class):
            __storm_primary__ = "prop2"

        cls_info = get_cls_info(SubClass)

        self.assertTrue(cls_info.primary_key[0] is SubClass.prop2)
        self.assertEquals(len(self.cls_info.primary_key), 1)

    def test_primary_key_composed(self):
        class Class(object):
            __storm_table__ = "table"
            prop1 = Property("column1", primary=2)
            prop2 = Property("column2", primary=1)
        cls_info = ClassInfo(Class)

        # Can't use == for props, since they're columns.
        self.assertTrue(cls_info.primary_key[0] is Class.prop2)
        self.assertTrue(cls_info.primary_key[1] is Class.prop1)
        self.assertEquals(len(cls_info.primary_key), 2)

    def test_primary_key_composed_with_attribute(self):
        class Class(object):
            __storm_table__ = "table"
            __storm_primary__ = "prop2", "prop1"
            # Define primary=True to ensure that the attribute
            # has precedence.
            prop1 = Property("column1", primary=True)
            prop2 = Property("column2")
        cls_info = ClassInfo(Class)

        # Can't use == for props, since they're columns.
        self.assertTrue(cls_info.primary_key[0] is Class.prop2)
        self.assertTrue(cls_info.primary_key[1] is Class.prop1)
        self.assertEquals(len(cls_info.primary_key), 2)

    def test_primary_key_composed_duplicated(self):
        class Class(object):
            __storm_table__ = "table"
            prop1 = Property("column1", primary=True)
            prop2 = Property("column2", primary=True)
        self.assertRaises(ClassInfoError, ClassInfo, Class)

    def test_primary_key_missing(self):
        class Class(object):
            __storm_table__ = "table"
            prop1 = Property("column1")
            prop2 = Property("column2")
        self.assertRaises(ClassInfoError, ClassInfo, Class)

    def test_primary_key_attribute_missing(self):
        class Class(object):
            __storm_table__ = "table"
            __storm_primary__ = ()
            prop1 = Property("column1", primary=True)
            prop2 = Property("column2")
        self.assertRaises(ClassInfoError, ClassInfo, Class)

    def test_primary_key_pos(self):
        class Class(object):
            __storm_table__ = "table"
            prop1 = Property("column1", primary=2)
            prop2 = Property("column2")
            prop3 = Property("column3", primary=1)
        cls_info = ClassInfo(Class)
        self.assertEquals(cls_info.primary_key_pos, (2, 0))


class ObjectInfoTest(TestHelper):

    def setUp(self):
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

    def test_checkpoint(self):
        self.obj.prop1 = 10
        self.obj_info.checkpoint()
        self.assertEquals(self.obj.prop1, 10)
        self.assertEquals(self.variable1.has_changed(), False)
        self.obj.prop1 = 20
        self.assertEquals(self.obj.prop1, 20)
        self.assertEquals(self.variable1.has_changed(), True)
        self.obj_info.checkpoint()
        self.assertEquals(self.obj.prop1, 20)
        self.assertEquals(self.variable1.has_changed(), False)
        self.obj.prop1 = 20
        self.assertEquals(self.obj.prop1, 20)
        self.assertEquals(self.variable1.has_changed(), False)

    def test_add_change_notification(self):
        changes1 = []
        changes2 = []
        def object_changed1(obj_info, variable, old_value, new_value, fromdb):
            changes1.append((1, obj_info, variable,
                             old_value, new_value, fromdb))
        def object_changed2(obj_info, variable, old_value, new_value, fromdb):
            changes2.append((2, obj_info, variable,
                             old_value, new_value, fromdb))

        self.obj_info.checkpoint()
        self.obj_info.event.hook("changed", object_changed1)
        self.obj_info.event.hook("changed", object_changed2)

        self.obj.prop2 = 10
        self.obj.prop1 = 20

        self.assertEquals(changes1,
                      [(1, self.obj_info, self.variable2, Undef, 10, False),
                       (1, self.obj_info, self.variable1, Undef, 20, False)])
        self.assertEquals(changes2,
                      [(2, self.obj_info, self.variable2, Undef, 10, False),
                       (2, self.obj_info, self.variable1, Undef, 20, False)])

        del changes1[:]
        del changes2[:]

        self.obj.prop1 = None
        self.obj.prop2 = None

        self.assertEquals(changes1,
                      [(1, self.obj_info, self.variable1, 20, None, False),
                       (1, self.obj_info, self.variable2, 10, None, False)])
        self.assertEquals(changes2,
                      [(2, self.obj_info, self.variable1, 20, None, False),
                       (2, self.obj_info, self.variable2, 10, None, False)])

        del changes1[:]
        del changes2[:]

        del self.obj.prop1
        del self.obj.prop2

        self.assertEquals(changes1,
                      [(1, self.obj_info, self.variable1, None, Undef, False),
                       (1, self.obj_info, self.variable2, None, Undef, False)])
        self.assertEquals(changes2,
                      [(2, self.obj_info, self.variable1, None, Undef, False),
                       (2, self.obj_info, self.variable2, None, Undef, False)])

    def test_add_change_notification_with_arg(self):
        changes1 = []
        changes2 = []
        def object_changed1(obj_info, variable,
                            old_value, new_value, fromdb, arg):
            changes1.append((1, obj_info, variable,
                             old_value, new_value, fromdb, arg))
        def object_changed2(obj_info, variable,
                            old_value, new_value, fromdb, arg):
            changes2.append((2, obj_info, variable,
                             old_value, new_value, fromdb, arg))

        self.obj_info.checkpoint()

        obj = object()
        
        self.obj_info.event.hook("changed", object_changed1, obj)
        self.obj_info.event.hook("changed", object_changed2, obj)

        self.obj.prop2 = 10
        self.obj.prop1 = 20

        self.assertEquals(changes1,
                  [(1, self.obj_info, self.variable2, Undef, 10, False, obj),
                   (1, self.obj_info, self.variable1, Undef, 20, False, obj)])
        self.assertEquals(changes2,                                        
                  [(2, self.obj_info, self.variable2, Undef, 10, False, obj),
                   (2, self.obj_info, self.variable1, Undef, 20, False, obj)])

        del changes1[:]
        del changes2[:]

        self.obj.prop1 = None
        self.obj.prop2 = None

        self.assertEquals(changes1,
                  [(1, self.obj_info, self.variable1, 20, None, False, obj),
                   (1, self.obj_info, self.variable2, 10, None, False, obj)])
        self.assertEquals(changes2,                                       
                  [(2, self.obj_info, self.variable1, 20, None, False, obj),
                   (2, self.obj_info, self.variable2, 10, None, False, obj)])

        del changes1[:]
        del changes2[:]

        del self.obj.prop1
        del self.obj.prop2

        self.assertEquals(changes1,
              [(1, self.obj_info, self.variable1, None, Undef, False, obj),
               (1, self.obj_info, self.variable2, None, Undef, False, obj)])
        self.assertEquals(changes2,                                      
              [(2, self.obj_info, self.variable1, None, Undef, False, obj),
               (2, self.obj_info, self.variable2, None, Undef, False, obj)])

    def test_remove_change_notification(self):
        changes1 = []
        changes2 = []
        def object_changed1(obj_info, variable, old_value, new_value, fromdb):
            changes1.append((1, obj_info, variable,
                             old_value, new_value, fromdb))
        def object_changed2(obj_info, variable, old_value, new_value, fromdb):
            changes2.append((2, obj_info, variable,
                             old_value, new_value, fromdb))

        self.obj_info.checkpoint()

        self.obj_info.event.hook("changed", object_changed1)
        self.obj_info.event.hook("changed", object_changed2)
        self.obj_info.event.unhook("changed", object_changed1)

        self.obj.prop2 = 20
        self.obj.prop1 = 10

        self.assertEquals(changes1, [])
        self.assertEquals(changes2,
                      [(2, self.obj_info, self.variable2, Undef, 20, False),
                       (2, self.obj_info, self.variable1, Undef, 10, False)])

    def test_remove_change_notification_with_arg(self):
        changes1 = []
        changes2 = []
        def object_changed1(obj_info, variable,
                            old_value, new_value, fromdb, arg):
            changes1.append((1, obj_info, variable,
                             old_value, new_value, fromdb, arg))
        def object_changed2(obj_info, variable,
                            old_value, new_value, fromdb, arg):
            changes2.append((2, obj_info, variable,
                             old_value, new_value, fromdb, arg))

        self.obj_info.checkpoint()

        obj = object()

        self.obj_info.event.hook("changed", object_changed1, obj)
        self.obj_info.event.hook("changed", object_changed2, obj)
        self.obj_info.event.unhook("changed", object_changed1, obj)

        self.obj.prop2 = 20
        self.obj.prop1 = 10

        self.assertEquals(changes1, [])
        self.assertEquals(changes2,
                  [(2, self.obj_info, self.variable2, Undef, 20, False, obj),
                   (2, self.obj_info, self.variable1, Undef, 10, False, obj)])

    def test_auto_remove_change_notification(self):
        changes1 = []
        changes2 = []
        def object_changed1(obj_info, variable, old_value, new_value, fromdb):
            changes1.append((1, obj_info, variable,
                             old_value, new_value, fromdb))
            return False
        def object_changed2(obj_info, variable, old_value, new_value, fromdb):
            changes2.append((2, obj_info, variable,
                             old_value, new_value, fromdb))
            return False

        self.obj_info.checkpoint()

        self.obj_info.event.hook("changed", object_changed1)
        self.obj_info.event.hook("changed", object_changed2)

        self.obj.prop2 = 20
        self.obj.prop1 = 10

        self.assertEquals(changes1,
                      [(1, self.obj_info, self.variable2, Undef, 20, False)])
        self.assertEquals(changes2,
                      [(2, self.obj_info, self.variable2, Undef, 20, False)])

    def test_auto_remove_change_notification_with_arg(self):
        changes1 = []
        changes2 = []
        def object_changed1(obj_info, variable,
                            old_value, new_value, fromdb, arg):
            changes1.append((1, obj_info, variable,
                             old_value, new_value, fromdb, arg))
            return False
        def object_changed2(obj_info, variable,
                            old_value, new_value, fromdb, arg):
            changes2.append((2, obj_info, variable,
                             old_value, new_value, fromdb, arg))
            return False

        self.obj_info.checkpoint()

        obj = object()

        self.obj_info.event.hook("changed", object_changed1, obj)
        self.obj_info.event.hook("changed", object_changed2, obj)

        self.obj.prop2 = 20
        self.obj.prop1 = 10

        self.assertEquals(changes1,
                  [(1, self.obj_info, self.variable2, Undef, 20, False, obj)])
        self.assertEquals(changes2,
                  [(2, self.obj_info, self.variable2, Undef, 20, False, obj)])

    def test_get_obj(self):
        self.assertTrue(self.obj_info.get_obj() is self.obj)

    def test_set_obj(self):
        obj = self.Class()
        self.obj_info.set_obj(obj)
        self.assertTrue(self.obj_info.get_obj() is obj)

    def test_weak_reference(self):
        obj = self.Class()
        obj_info = get_obj_info(obj)
        del obj
        self.assertEquals(obj_info.get_obj(), None)

    def test_object_deleted_notification(self):
        obj = self.Class()
        obj_info = get_obj_info(obj)
        obj_info.tainted = True
        deleted = []
        def object_deleted(obj_info):
            deleted.append(obj_info)
        obj_info.event.hook("object-deleted", object_deleted)
        del obj_info
        del obj
        self.assertEquals(len(deleted), 1)
        self.assertEquals(getattr(deleted[0], "tainted", False), True)

    def test_object_deleted_notification_after_set_obj(self):
        obj = self.Class()
        obj_info = get_obj_info(obj)
        obj_info.tainted = True
        obj = self.Class()
        obj_info.set_obj(obj)
        deleted = []
        def object_deleted(obj_info):
            deleted.append(obj_info)
        obj_info.event.hook("object-deleted", object_deleted)
        del obj_info
        del obj
        self.assertEquals(len(deleted), 1)
        self.assertEquals(getattr(deleted[0], "tainted", False), True)


class ClassAliasTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        class Class(object):
            __storm_table__ = "table"
            prop1 = Property("column1", primary=True)
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
        self.assertEquals(statement, "alias")

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
            __storm_table__ = "class1"
            id = Property(primary=True)
        class Class2(object):
            __storm_table__ = Class1
            id = Property(primary=True)
        statement, parameters = compile(Class2)
        self.assertEquals(statement, "class1")
        alias = ClassAlias(Class2, "alias")
        statement, parameters = compile(Select(alias.id))
        self.assertEquals(statement, "SELECT alias.id FROM class1 AS alias")
