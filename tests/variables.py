from datetime import datetime, date, time
import cPickle as pickle

from storm.variables import *
from storm.event import EventSystem
from storm import Undef

from tests.helper import TestHelper


marker = object()


class CustomVariable(Variable):

    def __init__(self, *args, **kwargs):
        self.gets = []
        self.sets = []
        Variable.__init__(self, *args, **kwargs)

    def _parse_get(self, variable, to_db):
        self.gets.append((variable, to_db))
        return "g", variable

    def _parse_set(self, variable, from_db):
        self.sets.append((variable, from_db))
        return "s", variable


class VariableTest(TestHelper):

    def test_constructor_value(self):
        variable = CustomVariable(marker)
        self.assertEquals(variable.sets, [(marker, False)])

    def test_constructor_value_from_db(self):
        variable = CustomVariable(marker, from_db=True)
        self.assertEquals(variable.sets, [(marker, True)])

    def test_constructor_value_factory(self):
        variable = CustomVariable(value_factory=lambda:marker)
        self.assertEquals(variable.sets, [(marker, False)])

    def test_constructor_value_factory_from_db(self):
        variable = CustomVariable(value_factory=lambda:marker, from_db=True)
        self.assertEquals(variable.sets, [(marker, True)])

    def test_constructor_column(self):
        variable = CustomVariable(column=marker)
        self.assertEquals(variable.column, marker)

    def test_constructor_event(self):
        variable = CustomVariable(event=marker)
        self.assertEquals(variable.event, marker)

    def test_get_default(self):
        variable = CustomVariable()
        self.assertEquals(variable.get(default=marker), marker)

    def test_set(self):
        variable = CustomVariable()
        variable.set(marker)
        self.assertEquals(variable.sets, [(marker, False)])
        variable.set(marker, from_db=True)
        self.assertEquals(variable.sets, [(marker, False), (marker, True)])

    def test_get(self):
        variable = CustomVariable()
        variable.set(marker)
        self.assertEquals(variable.get(), ("g", ("s", marker)))
        self.assertEquals(variable.gets, [(("s", marker), False)])

        variable = CustomVariable()
        variable.set(marker)
        self.assertEquals(variable.get(to_db=True), ("g", ("s", marker)))
        self.assertEquals(variable.gets, [(("s", marker), True)])

    def test_eq(self):
        self.assertEquals(CustomVariable(marker), CustomVariable(marker))
        self.assertNotEquals(CustomVariable(marker), CustomVariable(object()))

    def test_is_defined(self):
        variable = CustomVariable()
        self.assertFalse(variable.is_defined())
        variable.set(marker)
        self.assertTrue(variable.is_defined())

    def test_set_get_none(self):
        variable = CustomVariable()
        variable.set(None)
        self.assertEquals(variable.get(marker), None)
        self.assertEquals(variable.sets, [])
        self.assertEquals(variable.gets, [])

    def test_set_none_with_not_none(self):
        variable = CustomVariable(not_none=True)
        self.assertRaises(NotNoneError, variable.set, None)

    def test_event_changed(self):
        event = EventSystem(marker)

        changed_values = []
        def changed(owner, variable, old_value, new_value):
            changed_values.append((owner, variable, old_value, new_value))
        
        event.hook("changed", changed)

        variable = CustomVariable(event=event)
        variable.set("value1")
        variable.set("value1")
        variable.set("value1", from_db=True)
        variable.set("value2")
        variable.set("value2", from_db=True)
        variable.set("value3", from_db=True)
        variable.set(None, from_db=True)
        variable.set("value4")
        variable.delete()
        variable.delete()

        self.assertEquals(changed_values[0],
          (marker, variable, Undef, "value1"))
        self.assertEquals(changed_values[1],
          (marker, variable, ("g", ("s", "value1")), "value2"))
        self.assertEquals(changed_values[2],
          (marker, variable, ("g", ("s", "value2")), ("g", ("s", "value3"))))
        self.assertEquals(changed_values[3],
          (marker, variable, ("g", ("s", "value3")), None))
        self.assertEquals(changed_values[4],
          (marker, variable, None, "value4"))
        self.assertEquals(changed_values[5],
          (marker, variable, ("g", ("s", "value4")), Undef))
        self.assertEquals(len(changed_values), 6)

    def test_get_state(self):
        variable = CustomVariable(marker)
        self.assertEquals(variable.get_state(), ("s", marker))

    def test_set_state(self):
        variable = CustomVariable()
        variable.set_state(marker)
        self.assertEquals(variable.get(), ("g", marker))

    def test_checkpoint_and_has_changed(self):
        variable = CustomVariable()
        self.assertFalse(variable.has_changed())
        variable.set(marker)
        self.assertTrue(variable.has_changed())
        variable.save()
        self.assertFalse(variable.has_changed())
        variable.set(marker)
        self.assertFalse(variable.has_changed())
        variable.set((marker, marker))
        self.assertTrue(variable.has_changed())
        variable.checkpoint()
        self.assertFalse(variable.has_changed())
        variable.set((marker, marker))
        self.assertFalse(variable.has_changed())
        variable.restore()
        self.assertFalse(variable.has_changed())
        variable.set((marker, marker))
        self.assertTrue(variable.has_changed())
        variable.set(marker)
        self.assertFalse(variable.has_changed())

    def test_save_restore(self):
        variable = CustomVariable()
        variable.set(marker)
        variable.save()
        self.assertTrue(variable.get(), ("g", ("s", marker)))
        variable.set((marker, marker))
        self.assertTrue(variable.get(), ("g", ("s", (marker, marker))))
        variable.restore()
        self.assertTrue(variable.get(), ("g", ("s", marker)))

    def test_copy(self):
        variable = CustomVariable()
        variable.set(marker)
        variable_copy = variable.copy()
        self.assertTrue(variable is not variable_copy)
        self.assertTrue(variable == variable_copy)

    def test_hash(self):
        obj1 = CustomVariable(marker)
        obj2 = CustomVariable(marker)
        self.assertEquals(hash(obj1), hash(obj2))


class BoolVariableTest(TestHelper):

    def test_set_get(self):
        variable = BoolVariable()
        variable.set(1)
        self.assertTrue(variable.get() is True)
        variable.set(0)
        self.assertTrue(variable.get() is False)


class IntVariableTest(TestHelper):

    def test_set_get(self):
        variable = IntVariable()
        variable.set("1")
        self.assertEquals(variable.get(), 1)
        variable.set(1.1)
        self.assertEquals(variable.get(), 1)


class FloatVariableTest(TestHelper):

    def test_set_get(self):
        variable = FloatVariable()
        variable.set("1.1")
        self.assertEquals(variable.get(), 1.1)
        variable.set(1.1)
        self.assertEquals(variable.get(), 1.1)


class StrVariableTest(TestHelper):

    def test_set_get(self):
        variable = StrVariable()
        variable.set(1)
        self.assertEquals(variable.get(), "1")
        variable.set(u"")
        self.assertTrue(isinstance(variable.get(), str))


class UnicodeVariableTest(TestHelper):

    def test_set_get(self):
        variable = UnicodeVariable()
        variable.set(1)
        self.assertEquals(variable.get(), u"1")
        variable.set("")
        self.assertTrue(isinstance(variable.get(), unicode))


class DateTimeVariableTest(TestHelper):

    def test_get_set(self):
        epoch = datetime.utcfromtimestamp(0)
        variable = DateTimeVariable()
        variable.set(0)
        self.assertEquals(variable.get(), epoch)
        variable.set(0.0)
        self.assertEquals(variable.get(), epoch)
        variable.set(0L)
        self.assertEquals(variable.get(), epoch)
        variable.set(epoch)
        self.assertEquals(variable.get(), epoch)
        self.assertRaises(TypeError, variable.set, marker)

    def test_get_set_from_database(self):
        datetime_str = "1977-05-04 12:34:56.78"
        datetime_uni = unicode(datetime_str)
        datetime_obj = datetime(1977, 5, 4, 12, 34, 56, 780000)

        variable = DateTimeVariable()

        variable.set(datetime_str, from_db=True)
        self.assertEquals(variable.get(), datetime_obj)
        variable.set(datetime_uni, from_db=True)
        self.assertEquals(variable.get(), datetime_obj)
        variable.set(datetime_obj, from_db=True)
        self.assertEquals(variable.get(), datetime_obj)

        datetime_str = "1977-05-04 12:34:56"
        datetime_uni = unicode(datetime_str)
        datetime_obj = datetime(1977, 5, 4, 12, 34, 56)

        variable.set(datetime_str, from_db=True)
        self.assertEquals(variable.get(), datetime_obj)
        variable.set(datetime_uni, from_db=True)
        self.assertEquals(variable.get(), datetime_obj)
        variable.set(datetime_obj, from_db=True)
        self.assertEquals(variable.get(), datetime_obj)

        self.assertRaises(TypeError, variable.set, 0, from_db=True)
        self.assertRaises(TypeError, variable.set, marker, from_db=True)
        self.assertRaises(ValueError, variable.set, "foobar", from_db=True)
        self.assertRaises(ValueError, variable.set, "foo bar", from_db=True)


class DateVariableTest(TestHelper):

    def test_get_set(self):
        epoch = datetime.utcfromtimestamp(0)
        epoch_date = epoch.date()

        variable = DateVariable()

        variable.set(epoch)
        self.assertEquals(variable.get(), epoch_date)
        variable.set(epoch_date)
        self.assertEquals(variable.get(), epoch_date)

        self.assertRaises(TypeError, variable.set, marker)

    def test_get_set_from_database(self):
        date_str = "1977-05-04"
        date_uni = unicode(date_str)
        date_obj = date(1977, 5, 4)

        variable = DateVariable()

        variable.set(date_str, from_db=True)
        self.assertEquals(variable.get(), date_obj)
        variable.set(date_uni, from_db=True)
        self.assertEquals(variable.get(), date_obj)
        variable.set(date_obj, from_db=True)
        self.assertEquals(variable.get(), date_obj)

        self.assertRaises(TypeError, variable.set, 0, from_db=True)
        self.assertRaises(TypeError, variable.set, marker, from_db=True)
        self.assertRaises(ValueError, variable.set, "foobar", from_db=True)


class TimeVariableTest(TestHelper):

    def test_get_set(self):
        epoch = datetime.utcfromtimestamp(0)
        epoch_time = epoch.time()

        variable = TimeVariable()

        variable.set(epoch)
        self.assertEquals(variable.get(), epoch_time)
        variable.set(epoch_time)
        self.assertEquals(variable.get(), epoch_time)

        self.assertRaises(TypeError, variable.set, marker)

    def test_get_set_from_database(self):
        time_str = "12:34:56.78"
        time_uni = unicode(time_str)
        time_obj = time(12, 34, 56, 780000)

        variable = TimeVariable()

        variable.set(time_str, from_db=True)
        self.assertEquals(variable.get(), time_obj)
        variable.set(time_uni, from_db=True)
        self.assertEquals(variable.get(), time_obj)
        variable.set(time_obj, from_db=True)
        self.assertEquals(variable.get(), time_obj)

        time_str = "12:34:56"
        time_uni = unicode(time_str)
        time_obj = time(12, 34, 56)

        variable.set(time_str, from_db=True)
        self.assertEquals(variable.get(), time_obj)
        variable.set(time_uni, from_db=True)
        self.assertEquals(variable.get(), time_obj)
        variable.set(time_obj, from_db=True)
        self.assertEquals(variable.get(), time_obj)

        self.assertRaises(TypeError, variable.set, 0, from_db=True)
        self.assertRaises(TypeError, variable.set, marker, from_db=True)
        self.assertRaises(ValueError, variable.set, "foobar", from_db=True)


class PickleVariableTest(TestHelper):

    def test_get_set(self):
        d = {"a": 1}
        d_dump = pickle.dumps(d, -1)

        variable = PickleVariable()

        variable.set(d)
        self.assertEquals(variable.get(), d)
        self.assertEquals(variable.get(to_db=True), d_dump)

        variable.set(d_dump, from_db=True)
        self.assertEquals(variable.get(), d)
        self.assertEquals(variable.get(to_db=True), d_dump)

        self.assertEquals(variable.get_state(), d_dump)
        
        variable.set(marker)
        variable.set_state(d_dump)
        self.assertEquals(variable.get(), d)

        variable.get()["b"] = 2
        self.assertEquals(variable.get(), {"a": 1, "b": 2})

