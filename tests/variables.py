from datetime import datetime, date, time, timedelta
import cPickle as pickle

from storm.exceptions import NoneError
from storm.variables import *
from storm.event import EventSystem
from storm.expr import Column, SQLToken
from storm.tz import tzutc, tzoffset
from storm import Undef

from tests.helper import TestHelper


class Marker(object):
    pass

marker = Marker()


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

    def test_set_none_with_allow_none(self):
        variable = CustomVariable(allow_none=False)
        self.assertRaises(NoneError, variable.set, None)

    def test_set_none_with_allow_none_and_column(self):
        column = Column("column_name")
        variable = CustomVariable(allow_none=False, column=column)
        try:
            variable.set(None)
        except NoneError, e:
            pass
        self.assertTrue("column_name" in str(e))

    def test_set_none_with_allow_none_and_column_with_table(self):
        column = Column("column_name", SQLToken("table_name"))
        variable = CustomVariable(allow_none=False, column=column)
        try:
            variable.set(None)
        except NoneError, e:
            pass
        self.assertTrue("table_name.column_name" in str(e))

    def test_event_changed(self):
        event = EventSystem(marker)

        changed_values = []
        def changed(owner, variable, old_value, new_value, fromdb):
            changed_values.append((owner, variable,
                                   old_value, new_value, fromdb))
        
        event.hook("changed", changed)

        variable = CustomVariable(event=event)
        variable.set("value1")
        variable.set("value2")
        variable.set("value3", from_db=True)
        variable.set(None, from_db=True)
        variable.set("value4")
        variable.delete()
        variable.delete()

        self.assertEquals(changed_values[0],
          (marker, variable, Undef, "value1", False))
        self.assertEquals(changed_values[1],
          (marker, variable, ("g", ("s", "value1")), "value2", False))
        self.assertEquals(changed_values[2],
          (marker, variable, ("g", ("s", "value2")), ("g", ("s", "value3")),
           True))
        self.assertEquals(changed_values[3],
          (marker, variable, ("g", ("s", "value3")), None, True))
        self.assertEquals(changed_values[4],
          (marker, variable, None, "value4", False))
        self.assertEquals(changed_values[5],
          (marker, variable, ("g", ("s", "value4")), Undef, False))
        self.assertEquals(len(changed_values), 6)

    def test_get_state(self):
        variable = CustomVariable(marker)
        self.assertEquals(variable.get_state(), (Undef, ("s", marker)))

    def test_set_state(self):
        lazy_value = object()
        variable = CustomVariable()
        variable.set_state((lazy_value, marker))
        self.assertEquals(variable.get(), ("g", marker))
        self.assertEquals(variable.get_lazy(), lazy_value)

    def test_checkpoint_and_has_changed(self):
        variable = CustomVariable()
        self.assertTrue(variable.has_changed())
        variable.set(marker)
        self.assertTrue(variable.has_changed())
        variable.checkpoint()
        self.assertFalse(variable.has_changed())
        variable.set(marker)
        self.assertFalse(variable.has_changed())
        variable.set((marker, marker))
        self.assertTrue(variable.has_changed())
        variable.checkpoint()
        self.assertFalse(variable.has_changed())
        variable.set((marker, marker))
        self.assertFalse(variable.has_changed())
        variable.set(marker)
        self.assertTrue(variable.has_changed())
        variable.set((marker, marker))
        self.assertFalse(variable.has_changed())

    def test_copy(self):
        variable = CustomVariable()
        variable.set(marker)
        variable_copy = variable.copy()
        self.assertTrue(variable is not variable_copy)
        self.assertTrue(variable == variable_copy)

    def test_hash(self):
        # They must hash the same to be used as cache keys.
        obj1 = CustomVariable(marker)
        obj2 = CustomVariable(marker)
        self.assertEquals(hash(obj1), hash(obj2))

    def test_lazy_value_setting(self):
        variable = CustomVariable()
        variable.set(LazyValue())
        self.assertEquals(variable.sets, [])
        self.assertTrue(variable.has_changed())

    def test_lazy_value_getting(self):
        variable = CustomVariable()
        variable.set(LazyValue())
        self.assertEquals(variable.get(marker), marker)
        variable.set(1)
        variable.set(LazyValue())
        self.assertEquals(variable.get(marker), marker)
        self.assertFalse(variable.is_defined())

    def test_lazy_value_resolving(self):
        event = EventSystem(marker)

        resolve_values = []
        def resolve(owner, variable, value):
            resolve_values.append((owner, variable, value))



        lazy_value = LazyValue()
        variable = CustomVariable(lazy_value, event=event)

        event.hook("resolve-lazy-value", resolve)

        variable.get()

        self.assertEquals(resolve_values,
                          [(marker, variable, lazy_value)])

    def test_lazy_value_changed_event(self):
        event = EventSystem(marker)

        changed_values = []
        def changed(owner, variable, old_value, new_value, fromdb):
            changed_values.append((owner, variable,
                                   old_value, new_value, fromdb))
        
        event.hook("changed", changed)

        variable = CustomVariable(event=event)

        lazy_value = LazyValue()

        variable.set(lazy_value)

        self.assertEquals(changed_values,
                          [(marker, variable, Undef, lazy_value, False)])

    def test_lazy_value_setting_on_resolving(self):
        event = EventSystem(marker)

        def resolve(owner, variable, value):
            variable.set(marker)

        event.hook("resolve-lazy-value", resolve)

        lazy_value = LazyValue()
        variable = CustomVariable(lazy_value, event=event)

        self.assertEquals(variable.get(), ("g", ("s", marker)))

    def test_lazy_value_reset_after_changed(self):
        event = EventSystem(marker)

        resolve_called = []
        def resolve(owner, variable, value):
            resolve_called.append(True)

        event.hook("resolve-lazy-value", resolve)

        variable = CustomVariable(event=event)

        variable.set(LazyValue())
        variable.set(1)
        self.assertEquals(variable.get(), ("g", ("s", 1)))
        self.assertEquals(resolve_called, [])

    def test_get_lazy_value(self):
        lazy_value = LazyValue()
        variable = CustomVariable()
        self.assertEquals(variable.get_lazy(), None)
        self.assertEquals(variable.get_lazy(marker), marker)
        variable.set(lazy_value)
        self.assertEquals(variable.get_lazy(marker), lazy_value)


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

    def test_get_set_with_tzinfo(self):
        datetime_str = "1977-05-04 12:34:56.78"
        datetime_obj = datetime(1977, 5, 4, 12, 34, 56, 780000, tzinfo=tzutc())

        variable = DateTimeVariable(tzinfo=tzutc())

        # Naive timezone, from_db=True.
        variable.set(datetime_str, from_db=True)
        self.assertEquals(variable.get(), datetime_obj)
        variable.set(datetime_obj, from_db=True)
        self.assertEquals(variable.get(), datetime_obj)

        # Naive timezone, from_db=False (doesn't work).
        datetime_obj = datetime(1977, 5, 4, 12, 34, 56, 780000)
        self.assertRaises(ValueError, variable.set, datetime_obj)

        # Different timezone, from_db=False.
        datetime_obj = datetime(1977, 5, 4, 12, 34, 56, 780000,
                                tzinfo=tzoffset("1h", 3600))
        variable.set(datetime_obj, from_db=False)
        converted_obj = variable.get()
        self.assertEquals(converted_obj, datetime_obj)
        self.assertEquals(type(converted_obj.tzinfo), tzutc)

        # Different timezone, from_db=True.
        datetime_obj = datetime(1977, 5, 4, 12, 34, 56, 780000,
                                tzinfo=tzoffset("1h", 3600))
        variable.set(datetime_obj, from_db=True)
        converted_obj = variable.get()
        self.assertEquals(converted_obj, datetime_obj)
        self.assertEquals(type(converted_obj.tzinfo), tzutc)


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

    def test_set_with_datetime(self):
        datetime_str = "1977-05-04 12:34:56.78"
        date_obj = date(1977, 5, 4)
        variable = DateVariable()
        variable.set(datetime_str, from_db=True)
        self.assertEquals(variable.get(), date_obj)


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

    def test_set_with_datetime(self):
        datetime_str = "1977-05-04 12:34:56.78"
        time_obj = time(12, 34, 56, 780000)
        variable = TimeVariable()
        variable.set(datetime_str, from_db=True)
        self.assertEquals(variable.get(), time_obj)


class TimeDeltaVariableTest(TestHelper):

    def test_get_set(self):
        delta = timedelta(days=42)

        variable = TimeDeltaVariable()

        variable.set(delta)
        self.assertEquals(variable.get(), delta)

        self.assertRaises(TypeError, variable.set, marker)
    
    def test_get_set_from_database(self):
        delta_str = "42 days 12:34:56.78"
        delta_uni = unicode(delta_str)
        delta_obj = timedelta(days=42, hours=12, minutes=34,
                              seconds=56, microseconds=780000)

        variable = TimeDeltaVariable()

        variable.set(delta_str, from_db=True)
        self.assertEquals(variable.get(), delta_obj)
        variable.set(delta_uni, from_db=True)
        self.assertEquals(variable.get(), delta_obj)
        variable.set(delta_obj, from_db=True)
        self.assertEquals(variable.get(), delta_obj)

        delta_str = "1 day, 12:34:56"
        delta_uni = unicode(delta_str)
        delta_obj = timedelta(days=1, hours=12, minutes=34, seconds=56)

        variable.set(delta_str, from_db=True)
        self.assertEquals(variable.get(), delta_obj)
        variable.set(delta_uni, from_db=True)
        self.assertEquals(variable.get(), delta_obj)
        variable.set(delta_obj, from_db=True)
        self.assertEquals(variable.get(), delta_obj)

        self.assertRaises(TypeError, variable.set, 0, from_db=True)
        self.assertRaises(TypeError, variable.set, marker, from_db=True)
        self.assertRaises(ValueError, variable.set, "foobar", from_db=True)

        # Intervals of months or years can not be converted to a
        # Python timedelta, so a ValueError exception is raised:
        self.assertRaises(ValueError, variable.set, "42 months", from_db=True)
        self.assertRaises(ValueError, variable.set, "42 years", from_db=True)


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

        self.assertEquals(variable.get_state(), (Undef, d_dump))
        
        variable.set(marker)
        variable.set_state((Undef, d_dump))
        self.assertEquals(variable.get(), d)

        variable.get()["b"] = 2
        self.assertEquals(variable.get(), {"a": 1, "b": 2})

    def test_pickle_events(self):
        event = EventSystem(marker)

        variable = PickleVariable(event=event, value_factory=list)

        changes = []
        def changed(owner, variable, old_value, new_value, fromdb):
            changes.append((variable, old_value, new_value, fromdb))

        event.hook("changed", changed)

        variable.checkpoint()

        event.emit("flush")

        self.assertEquals(changes, [])

        lst = variable.get()

        self.assertEquals(lst, [])
        self.assertEquals(changes, [])

        lst.append("a")

        self.assertEquals(changes, [])

        event.emit("flush")

        self.assertEquals(changes, [(variable, None, ["a"], False)])

        del changes[:]

        event.emit("object-deleted")
        self.assertEquals(changes, [(variable, None, ["a"], False)])


class ListVariableTest(TestHelper):

    def test_get_set(self):
        l = [1, 2]
        l_dump = pickle.dumps(l, -1)

        variable = ListVariable(IntVariable)

        variable.set(l)
        self.assertEquals(variable.get(), l)
        self.assertEquals(variable.get(to_db=True),
                          [IntVariable(1), IntVariable(2)])

        variable.set([1.1, 2.2], from_db=True)
        self.assertEquals(variable.get(), l)
        self.assertEquals(variable.get(to_db=True),
                          [IntVariable(1), IntVariable(2)])

        self.assertEquals(variable.get_state(), (Undef, l_dump))

        variable.set([])
        variable.set_state((Undef, l_dump))
        self.assertEquals(variable.get(), l)

        variable.get().append(3)
        self.assertEquals(variable.get(), [1, 2, 3])

    def test_list_events(self):
        event = EventSystem(marker)

        variable = ListVariable(StrVariable, event=event, value_factory=list)

        changes = []
        def changed(owner, variable, old_value, new_value, fromdb):
            changes.append((variable, old_value, new_value, fromdb))

        event.hook("changed", changed)

        variable.checkpoint()

        event.emit("flush")

        self.assertEquals(changes, [])

        lst = variable.get()

        self.assertEquals(lst, [])
        self.assertEquals(changes, [])

        lst.append("a")

        self.assertEquals(changes, [])

        event.emit("flush")

        self.assertEquals(changes, [(variable, None, ["a"], False)])

        del changes[:]

        event.emit("object-deleted")
        self.assertEquals(changes, [(variable, None, ["a"], False)])


class EnumVariableTest(TestHelper):

    def test_set_get(self):
        variable = EnumVariable({1: "foo", 2: "bar"}, {"foo": 1, "bar": 2})
        variable.set("foo")
        self.assertEquals(variable.get(), "foo")
        self.assertEquals(variable.get(to_db=True), 1)
        variable.set(2, from_db=True)
        self.assertEquals(variable.get(), "bar")
        self.assertEquals(variable.get(to_db=True), 2)
        self.assertRaises(ValueError, variable.set, "foobar")
        self.assertRaises(ValueError, variable.set, 2)

    def test_in_map(self):
        variable = EnumVariable({1: "foo", 2: "bar"}, {"one": 1, "two": 2})
        variable.set("one")
        self.assertEquals(variable.get(), "foo")
        self.assertEquals(variable.get(to_db=True), 1)
        variable.set(2, from_db=True)
        self.assertEquals(variable.get(), "bar")
        self.assertEquals(variable.get(to_db=True), 2)
        self.assertRaises(ValueError, variable.set, "foo")
        self.assertRaises(ValueError, variable.set, 2)
