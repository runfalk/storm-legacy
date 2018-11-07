#
# Copyright (c) 2006, 2007 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# Storm is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 2.1 of
# the License, or (at your option) any later version.
#
# Storm is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
import gc
import json
import pytest
import uuid
import weakref

from datetime import datetime, date, time, timedelta
from decimal import Decimal
from mock import sentinel

from storm.compat import buffer, bstr, pickle, ustr
from storm.exceptions import NoneError
from storm.variables import *
from storm.event import EventSystem
from storm.expr import Column, SQLToken
from storm.tz import tzutc, tzoffset
from storm import Undef

from tests.helper import assert_variables_equal, TestHelper


class Marker(object):
    pass


marker = Marker()


class CustomVariable(Variable):
    def __init__(self, *args, **kwargs):
        self.gets = []
        self.sets = []
        Variable.__init__(self, *args, **kwargs)

    def parse_get(self, variable, to_db):
        self.gets.append((variable, to_db))
        return "g", variable

    def parse_set(self, variable, from_db):
        self.sets.append((variable, from_db))
        return "s", variable


def test_constructor_value():
    variable = CustomVariable(marker)
    assert variable.sets == [(marker, False)]


def test_constructor_value_from_db():
    variable = CustomVariable(marker, from_db=True)
    assert variable.sets == [(marker, True)]


def test_constructor_value_factory():
    variable = CustomVariable(value_factory=lambda:marker)
    assert variable.sets == [(marker, False)]


def test_constructor_value_factory_from_db():
    variable = CustomVariable(value_factory=lambda:marker, from_db=True)
    assert variable.sets == [(marker, True)]


def test_constructor_column():
    variable = CustomVariable(column=marker)
    assert variable.column == marker


def test_constructor_event():
    variable = CustomVariable(event=marker)
    assert variable.event == marker


def test_get_default():
    variable = CustomVariable()
    assert variable.get(default=marker) == marker


def test_set():
    variable = CustomVariable()
    variable.set(marker)
    assert variable.sets == [(marker, False)]
    variable.set(marker, from_db=True)
    assert variable.sets == [(marker, False), (marker, True)]


def test_set_leak():
    """When a variable is checkpointed, the value must not leak."""
    variable = Variable()
    m = Marker()
    m_ref = weakref.ref(m)
    variable.set(m)
    variable.checkpoint()
    variable.set(LazyValue())
    del m
    gc.collect()
    assert m_ref() is None


def test_get():
    variable = CustomVariable()
    variable.set(marker)
    assert variable.get() == ("g", ("s", marker))
    assert variable.gets == [(("s", marker), False)]

    variable = CustomVariable()
    variable.set(marker)
    assert variable.get(to_db=True) == ("g", ("s", marker))
    assert variable.gets == [(("s", marker), True)]


def test_is_defined():
    variable = CustomVariable()
    assert not variable.is_defined()
    variable.set(marker)
    assert variable.is_defined()


def test_set_get_none():
    variable = CustomVariable()
    variable.set(None)
    assert variable.get(marker) == None
    assert variable.sets == []
    assert variable.gets == []


def test_set_none_with_allow_none():
    variable = CustomVariable(allow_none=False)
    with pytest.raises(NoneError):
        variable.set(None)


def test_set_none_with_allow_none_and_column():
    column = Column("column_name")
    variable = CustomVariable(allow_none=False, column=column)
    try:
        variable.set(None)
        assert False
    except NoneError as e:
        assert "column_name" in ustr(e)


def test_set_none_with_allow_none_and_column_with_table():
    column = Column("column_name", SQLToken("table_name"))
    variable = CustomVariable(allow_none=False, column=column)
    with pytest.raises(NoneError):
        variable.set(None)


def test_set_with_validator():
    args = []
    def validator(obj, attr, value):
        args.append((obj, attr, value))
        return value
    variable = CustomVariable(validator=validator)
    variable.set(3)
    assert args == [(None, None, 3)]


def test_set_with_validator_and_validator_arguments():
    args = []
    def validator(obj, attr, value):
        args.append((obj, attr, value))
        return value
    variable = CustomVariable(validator=validator,
                              validator_object_factory=lambda: 1,
                              validator_attribute=2)
    variable.set(3)
    assert args == [(1, 2, 3)]


def test_set_with_validator_raising_error():
    args = []
    def validator(obj, attr, value):
        args.append((obj, attr, value))
        raise ZeroDivisionError()
    variable = CustomVariable(validator=validator)
    with pytest.raises(ZeroDivisionError):
        variable.set(marker)
    assert args == [(None, None, marker)]
    assert variable.get() == None


def test_set_with_validator_changing_value():
    args = []
    def validator(obj, attr, value):
        args.append((obj, attr, value))
        return 42
    variable = CustomVariable(validator=validator)
    variable.set(marker)
    assert args == [(None, None, marker)]
    assert variable.get() == ('g', ('s', 42))


def test_set_from_db_wont_call_validator():
    args = []
    def validator(obj, attr, value):
        args.append((obj, attr, value))
        return 42
    variable = CustomVariable(validator=validator)
    variable.set(marker, from_db=True)
    assert args == []
    assert variable.get() == ('g', ('s', marker))


def test_event_changed():
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

    assert changed_values[0] == (marker, variable, Undef, "value1", False)
    assert changed_values[1] == (
        marker,
        variable,
        ("g", ("s", "value1")),
        "value2",
        False
    )
    assert changed_values[2] == (
        marker,
        variable,
        ("g", ("s", "value2")),
        ("g", ("s", "value3")),
        True,
    )
    assert changed_values[3] == (
        marker,
        variable,
        ("g", ("s", "value3")),
        None,
        True,
    )
    assert changed_values[4] == (marker, variable, None, "value4", False)
    assert changed_values[5] == (
        marker,
        variable,
        ("g", ("s", "value4")),
        Undef,
        False,
    )
    assert len(changed_values) == 6


def test_get_state():
    variable = CustomVariable(marker)
    assert variable.get_state() == (Undef, ("s", marker))


def test_set_state():
    lazy_value = object()
    variable = CustomVariable()
    variable.set_state((lazy_value, marker))
    assert variable.get() == ("g", marker)
    assert variable.get_lazy() == lazy_value


def test_checkpoint_and_has_changed():
    variable = CustomVariable()
    assert variable.has_changed()
    variable.set(marker)
    assert variable.has_changed()
    variable.checkpoint()
    assert not variable.has_changed()
    variable.set(marker)
    assert not variable.has_changed()
    variable.set((marker, marker))
    assert variable.has_changed()
    variable.checkpoint()
    assert not variable.has_changed()
    variable.set((marker, marker))
    assert not variable.has_changed()
    variable.set(marker)
    assert variable.has_changed()
    variable.set((marker, marker))
    assert not variable.has_changed()


def test_copy():
    variable = CustomVariable()
    variable.set(marker)
    variable_copy = variable.copy()
    variable_copy.gets = []
    assert variable is not variable_copy
    assert_variables_equal([variable], [variable_copy])


def test_lazy_value_setting():
    variable = CustomVariable()
    variable.set(LazyValue())
    assert variable.sets == []
    assert variable.has_changed()


def test_lazy_value_getting():
    variable = CustomVariable()
    variable.set(LazyValue())
    assert variable.get(marker) == marker
    variable.set(1)
    variable.set(LazyValue())
    assert variable.get(marker) == marker
    assert not variable.is_defined()


def test_lazy_value_resolving():
    event = EventSystem(marker)

    resolve_values = []
    def resolve(owner, variable, value):
        resolve_values.append((owner, variable, value))



    lazy_value = LazyValue()
    variable = CustomVariable(lazy_value, event=event)

    event.hook("resolve-lazy-value", resolve)

    variable.get()

    assert resolve_values == [(marker, variable, lazy_value)]


def test_lazy_value_changed_event():
    event = EventSystem(marker)

    changed_values = []
    def changed(owner, variable, old_value, new_value, fromdb):
        changed_values.append((owner, variable,
                               old_value, new_value, fromdb))

    event.hook("changed", changed)

    variable = CustomVariable(event=event)

    lazy_value = LazyValue()

    variable.set(lazy_value)

    assert changed_values == [(marker, variable, Undef, lazy_value, False)]


def test_lazy_value_setting_on_resolving():
    event = EventSystem(marker)

    def resolve(owner, variable, value):
        variable.set(marker)

    event.hook("resolve-lazy-value", resolve)

    lazy_value = LazyValue()
    variable = CustomVariable(lazy_value, event=event)

    assert variable.get() == ("g", ("s", marker))


def test_lazy_value_reset_after_changed():
    event = EventSystem(marker)

    resolve_called = []
    def resolve(owner, variable, value):
        resolve_called.append(True)

    event.hook("resolve-lazy-value", resolve)

    variable = CustomVariable(event=event)

    variable.set(LazyValue())
    variable.set(1)
    assert variable.get() == ("g", ("s", 1))
    assert resolve_called == []


def test_get_lazy_value():
    lazy_value = LazyValue()
    variable = CustomVariable()
    assert variable.get_lazy() == None
    assert variable.get_lazy(marker) == marker
    variable.set(lazy_value)
    assert variable.get_lazy(marker) == lazy_value


def test_bool_set_get():
    variable = BoolVariable()
    variable.set(1)
    assert variable.get() is True
    variable.set(0)
    assert variable.get() is False
    variable.set(1.1)
    assert variable.get() is True
    variable.set(0.0)
    assert variable.get() is False
    variable.set(Decimal(1))
    assert variable.get() is True
    variable.set(Decimal(0))
    assert variable.get() is False
    with pytest.raises(TypeError):
        variable.set("string")


def test_int_set_get():
    variable = IntVariable()
    variable.set(1)
    assert variable.get() == 1
    variable.set(1.1)
    assert variable.get() == 1
    variable.set(Decimal(2))
    assert variable.get() == 2
    with pytest.raises(TypeError):
        variable.set("1")


def test_float_set_get():
    variable = FloatVariable()
    variable.set(1.1)
    assert variable.get() == 1.1
    variable.set(1)
    assert variable.get() == 1
    assert type(variable.get()) == float
    variable.set(Decimal("1.1"))
    assert variable.get() == 1.1
    with pytest.raises(TypeError):
        variable.set("1")


def test_decimal_set_get():
    variable = DecimalVariable()
    variable.set(Decimal("1.1"))
    assert variable.get() == Decimal("1.1")
    variable.set(1)
    assert variable.get() == 1
    assert type(variable.get()) == Decimal
    variable.set(Decimal("1.1"))
    assert variable.get() == Decimal("1.1")
    with pytest.raises(TypeError):
        variable.set("1")
    with pytest.raises(TypeError):
        variable.set(1.1)


def test_decimal_get_set_from_database():
    """Strings used to/from the database."""
    variable = DecimalVariable()
    variable.set("1.1", from_db=True)
    assert variable.get() == Decimal("1.1")
    assert variable.get(to_db=True) == "1.1"


def test_raw_str_set_get():
    variable = RawStrVariable()
    variable.set(b"str")
    assert variable.get() == b"str"
    variable.set(buffer(b"buffer"))
    assert variable.get() == b"buffer"
    with pytest.raises(TypeError):
        variable.set(u"unicode")


def test_unicode_set_get():
    variable = UnicodeVariable()
    variable.set(u"unicode")
    assert variable.get() == u"unicode"
    with pytest.raises(TypeError):
        variable.set(b"str")


def test_datetime_get_set():
    epoch = datetime.utcfromtimestamp(0)
    variable = DateTimeVariable()
    variable.set(0)
    assert variable.get() == epoch
    variable.set(0.0)
    assert variable.get() == epoch
    variable.set(0)
    assert variable.get() == epoch
    variable.set(epoch)
    assert variable.get() == epoch
    with pytest.raises(TypeError):
        variable.set(marker)


def test_datetime_get_set_from_database():
    datetime_str = "1977-05-04 12:34:56.78"
    datetime_uni = ustr(datetime_str)
    datetime_obj = datetime(1977, 5, 4, 12, 34, 56, 780000)

    variable = DateTimeVariable()

    variable.set(datetime_str, from_db=True)
    assert variable.get() == datetime_obj
    variable.set(datetime_uni, from_db=True)
    assert variable.get() == datetime_obj
    variable.set(datetime_obj, from_db=True)
    assert variable.get() == datetime_obj

    datetime_str = "1977-05-04 12:34:56"
    datetime_uni = ustr(datetime_str)
    datetime_obj = datetime(1977, 5, 4, 12, 34, 56)

    variable.set(datetime_str, from_db=True)
    assert variable.get() == datetime_obj
    variable.set(datetime_uni, from_db=True)
    assert variable.get() == datetime_obj
    variable.set(datetime_obj, from_db=True)
    assert variable.get() == datetime_obj

    with pytest.raises(TypeError):
        variable.set(0, from_db=True)
    with pytest.raises(TypeError):
        variable.set(marker, from_db=True)
    with pytest.raises(ValueError):
        variable.set("foobar", from_db=True)
    with pytest.raises(ValueError):
        variable.set("foo bar", from_db=True)


def test_datetime_get_set_with_tzinfo():
    datetime_str = "1977-05-04 12:34:56.78"
    datetime_obj = datetime(1977, 5, 4, 12, 34, 56, 780000, tzinfo=tzutc())

    variable = DateTimeVariable(tzinfo=tzutc())

    # Naive timezone, from_db=True.
    variable.set(datetime_str, from_db=True)
    assert variable.get() == datetime_obj
    variable.set(datetime_obj, from_db=True)
    assert variable.get() == datetime_obj

    # Naive timezone, from_db=False (doesn't work).
    datetime_obj = datetime(1977, 5, 4, 12, 34, 56, 780000)
    with pytest.raises(ValueError):
        variable.set(datetime_obj)

    # Different timezone, from_db=False.
    datetime_obj = datetime(1977, 5, 4, 12, 34, 56, 780000,
                            tzinfo=tzoffset("1h", 3600))
    variable.set(datetime_obj, from_db=False)
    converted_obj = variable.get()
    assert converted_obj == datetime_obj
    assert type(converted_obj.tzinfo) == tzutc

    # Different timezone, from_db=True.
    datetime_obj = datetime(1977, 5, 4, 12, 34, 56, 780000,
                            tzinfo=tzoffset("1h", 3600))
    variable.set(datetime_obj, from_db=True)
    converted_obj = variable.get()
    assert converted_obj == datetime_obj
    assert type(converted_obj.tzinfo) == tzutc


def test_date_get_set():
    epoch = datetime.utcfromtimestamp(0)
    epoch_date = epoch.date()

    variable = DateVariable()

    variable.set(epoch)
    assert variable.get() == epoch_date
    variable.set(epoch_date)
    assert variable.get() == epoch_date

    with pytest.raises(TypeError):
        variable.set(marker)


def test_date_get_set_from_database():
    date_str = "1977-05-04"
    date_uni = ustr(date_str)
    date_obj = date(1977, 5, 4)
    datetime_obj = datetime(1977, 5, 4, 0, 0, 0)

    variable = DateVariable()

    variable.set(date_str, from_db=True)
    assert variable.get() == date_obj
    variable.set(date_uni, from_db=True)
    assert variable.get() == date_obj
    variable.set(date_obj, from_db=True)
    assert variable.get() == date_obj
    variable.set(datetime_obj, from_db=True)
    assert variable.get() == date_obj

    with pytest.raises(TypeError):
        variable.set(0, from_db=True)
    with pytest.raises(TypeError):
        variable.set(marker, from_db=True)
    with pytest.raises(ValueError):
        variable.set("foobar", from_db=True)


def test_date_set_with_datetime():
    datetime_str = "1977-05-04 12:34:56.78"
    date_obj = date(1977, 5, 4)
    variable = DateVariable()
    variable.set(datetime_str, from_db=True)
    assert variable.get() == date_obj


def test_time_get_set():
    epoch = datetime.utcfromtimestamp(0)
    epoch_time = epoch.time()

    variable = TimeVariable()

    variable.set(epoch)
    assert variable.get() == epoch_time
    variable.set(epoch_time)
    assert variable.get() == epoch_time

    with pytest.raises(TypeError):
        variable.set(marker)


def test_time_get_set_from_database():
    time_str = "12:34:56.78"
    time_uni = ustr(time_str)
    time_obj = time(12, 34, 56, 780000)

    variable = TimeVariable()

    variable.set(time_str, from_db=True)
    assert variable.get() == time_obj
    variable.set(time_uni, from_db=True)
    assert variable.get() == time_obj
    variable.set(time_obj, from_db=True)
    assert variable.get() == time_obj

    time_str = "12:34:56"
    time_uni = ustr(time_str)
    time_obj = time(12, 34, 56)

    variable.set(time_str, from_db=True)
    assert variable.get() == time_obj
    variable.set(time_uni, from_db=True)
    assert variable.get() == time_obj
    variable.set(time_obj, from_db=True)
    assert variable.get() == time_obj

    with pytest.raises(TypeError):
        variable.set(0, from_db=True)
    with pytest.raises(TypeError):
        variable.set(marker, from_db=True)
    with pytest.raises(ValueError):
        variable.set("foobar", from_db=True)


def test_time_set_with_datetime():
    datetime_str = "1977-05-04 12:34:56.78"
    time_obj = time(12, 34, 56, 780000)
    variable = TimeVariable()
    variable.set(datetime_str, from_db=True)
    assert variable.get() == time_obj


def test_time_microsecond_error():
    time_str = "15:14:18.598678"
    time_obj = time(15, 14, 18, 598678)
    variable = TimeVariable()
    variable.set(time_str, from_db=True)
    assert variable.get() == time_obj


def test_time_microsecond_error_less_digits():
    time_str = "15:14:18.5986"
    time_obj = time(15, 14, 18, 598600)
    variable = TimeVariable()
    variable.set(time_str, from_db=True)
    assert variable.get() == time_obj


def test_time_microsecond_error_more_digits():
    time_str = "15:14:18.5986789"
    time_obj = time(15, 14, 18, 598678)
    variable = TimeVariable()
    variable.set(time_str, from_db=True)
    assert variable.get() == time_obj


def test_timedelta_get_set():
    delta = timedelta(days=42)

    variable = TimeDeltaVariable()

    variable.set(delta)
    assert variable.get() == delta

    with pytest.raises(TypeError):
        variable.set(marker)


def test_timedelta_get_set_from_database():
    delta_str = "42 days 12:34:56.78"
    delta_uni = ustr(delta_str)
    delta_obj = timedelta(days=42, hours=12, minutes=34,
                          seconds=56, microseconds=780000)

    variable = TimeDeltaVariable()

    variable.set(delta_str, from_db=True)
    assert variable.get() == delta_obj
    variable.set(delta_uni, from_db=True)
    assert variable.get() == delta_obj
    variable.set(delta_obj, from_db=True)
    assert variable.get() == delta_obj

    delta_str = "1 day, 12:34:56"
    delta_uni = ustr(delta_str)
    delta_obj = timedelta(days=1, hours=12, minutes=34, seconds=56)

    variable.set(delta_str, from_db=True)
    assert variable.get() == delta_obj
    variable.set(delta_uni, from_db=True)
    assert variable.get() == delta_obj
    variable.set(delta_obj, from_db=True)
    assert variable.get() == delta_obj

    with pytest.raises(TypeError):
        variable.set(0, from_db=True)
    with pytest.raises(TypeError):
        variable.set(marker, from_db=True)
    with pytest.raises(ValueError):
        variable.set("foobar", from_db=True)

    # Intervals of months or years can not be converted to a
    # Python timedelta, so a ValueError exception is raised:
    with pytest.raises(ValueError):
        variable.set("42 months", from_db=True)
    with pytest.raises(ValueError):
        variable.set("42 years", from_db=True)


@pytest.mark.parametrize("interval, td", [
    ("0:00:00", timedelta(0)),
    ("0:00:00.000001", timedelta(0, 0, 1)),
    ("0:00:00.120000", timedelta(0, 0, 120000)),
    ("0:00:01", timedelta(0, 1)),
    ("0:00:12", timedelta(0, 12)),
    ("0:01:00", timedelta(0, 60)),
    ("0:12:00", timedelta(0, 12*60)),
    ("1:00:00", timedelta(0, 60*60)),
    ("12:00:00", timedelta(0, 12*60*60)),
    ("1 day, 0:00:00", timedelta(1)),
    ("12 days, 0:00:00", timedelta(12)),
    ("12 days, 12:12:12.120000", timedelta(12, 12*60*60 + 12*60 + 12, 120000)),
    ("-1 day, 23:59:59.880000", timedelta(0, 0, -120000)),
    ("-12 days, 0:00:00", timedelta(-12)),
    ("-12:00:00", timedelta(hours=-12)),
    ("1.5 days", timedelta(days=1, hours=12)),
    ("1h123", timedelta(hours=1, seconds=123)),
    ("1d1h1m1s1ms", timedelta(days=1, hours=1, minutes=1, seconds=1, microseconds=1000)),
    ("-12 1:02 3s", timedelta(days=-12, hours=1, minutes=2, seconds=3)),
])
def test_timedelta_parse(interval, td):
    assert TimeDeltaVariable(interval, from_db=True).get() == td


@pytest.mark.parametrize("interval", [
    "1 month",
    "day",
])
def test_timedelta_invalid(interval):
    with pytest.raises(ValueError):
        TimeDeltaVariable(interval, from_db=True).get()


def test_uuid_get_set():
    value = uuid.UUID("{0609f76b-878f-4546-baf5-c1b135e8de72}")

    variable = UUIDVariable()

    variable.set(value)
    assert variable.get() == value
    assert variable.get(to_db=True) == "0609f76b-878f-4546-baf5-c1b135e8de72"

    with pytest.raises(TypeError):
        variable.set(marker)
    with pytest.raises(TypeError):
        variable.set("0609f76b-878f-4546-baf5-c1b135e8de72")
    with pytest.raises(TypeError):
        variable.set(u"0609f76b-878f-4546-baf5-c1b135e8de72")


def test_uuid_get_set_from_database():
    value = uuid.UUID("{0609f76b-878f-4546-baf5-c1b135e8de72}")

    variable = UUIDVariable()

    # Strings and UUID objects are accepted from the database.
    variable.set(value, from_db=True)
    assert variable.get() == value
    variable.set("0609f76b-878f-4546-baf5-c1b135e8de72", from_db=True)
    assert variable.get() == value
    variable.set(u"0609f76b-878f-4546-baf5-c1b135e8de72", from_db=True)
    assert variable.get() == value

    # Some other representations for UUID values.
    variable.set("{0609f76b-878f-4546-baf5-c1b135e8de72}", from_db=True)
    assert variable.get() == value
    variable.set("0609f76b878f4546baf5c1b135e8de72", from_db=True)
    assert variable.get() == value


def test_json_get_set():
    d = {"a": 1}
    d_dump = json.dumps(d)

    # On Python 2 json_data will be a byte string. We want unicode
    if isinstance(d_dump, bstr):
        d_dump = d_dump.decode("utf-8")

    variable = JSONVariable()

    variable.set(d)
    assert variable.get() == d
    assert variable.get(to_db=True) == d_dump

    variable.set(d_dump, from_db=True)
    assert variable.get() == d
    assert variable.get(to_db=True) == d_dump

    assert variable.get_state() == (Undef, d_dump)

    variable.set(marker)
    variable.set_state((Undef, d_dump))
    assert variable.get() == d

    variable.get()["b"] = 2
    assert variable.get() == {"a": 1, "b": 2}


def test_json_events():
    event = EventSystem(marker)

    variable = JSONVariable(event=event, value_factory=list)

    changes = []
    def changed(owner, variable, old_value, new_value, fromdb):
        changes.append((variable, old_value, new_value, fromdb))

    event.emit("start-tracking-changes", event)
    event.hook("changed", changed)

    variable.checkpoint()

    event.emit("flush")
    assert changes == []

    lst = variable.get()
    assert lst == []
    assert changes == []

    lst.append("a")
    assert changes == []

    event.emit("flush")
    assert changes == [(variable, None, ["a"], False)]

    del changes[:]
    event.emit("object-deleted")
    assert changes == [(variable, None, ["a"], False)]


def test_json_unicode_from_db_required():
    # JSONVariable._loads() complains loudly if it does not receive a
    # unicode string because it has no way of knowing its encoding.
    variable = JSONVariable()
    with pytest.raises(TypeError):
        variable.set(b'"abc"', from_db=True)


def test_json_unicode_to_db():
    # JSONVariable._dumps() works around unicode/str handling issues in
    # simplejson/json.
    variable = JSONVariable()
    variable.set({u"a": 1})
    assert isinstance(variable.get(to_db=True), ustr)


def test_list_get_set():
    # Enumeration variables are used as items so that database
    # side and python side values can be distinguished.
    get_map = {1: "a", 2: "b", 3: "c"}
    set_map = {"a": 1, "b": 2, "c": 3}
    item_factory = VariableFactory(
        EnumVariable, get_map=get_map, set_map=set_map)

    l = ["a", "b"]
    l_dump = pickle.dumps(l, -1)
    l_vars = [item_factory(value=x) for x in l]

    variable = ListVariable(item_factory)

    variable.set(l)
    assert variable.get() == l
    assert_variables_equal(variable.get(to_db=True), l_vars)

    variable.set([1, 2], from_db=True)
    assert variable.get() == l
    assert_variables_equal(variable.get(to_db=True), l_vars)

    assert variable.get_state() == (Undef, l_dump)

    variable.set([])
    variable.set_state((Undef, l_dump))
    assert variable.get() == l

    variable.get().append("c")
    assert variable.get() == ["a", "b", "c"]


def test_list_events():
    event = EventSystem(marker)

    variable = ListVariable(RawStrVariable, event=event,
                            value_factory=list)

    changes = []
    def changed(owner, variable, old_value, new_value, fromdb):
        changes.append((variable, old_value, new_value, fromdb))

    event.emit("start-tracking-changes", event)
    event.hook("changed", changed)

    variable.checkpoint()

    event.emit("flush")
    assert changes == []

    lst = variable.get()
    assert lst == []
    assert changes == []

    lst.append("a")
    assert changes == []

    event.emit("flush")
    assert changes == [(variable, None, ["a"], False)]

    del changes[:]
    event.emit("object-deleted")
    assert changes == [(variable, None, ["a"], False)]


def test_enum_set_get():
    variable = EnumVariable({1: "foo", 2: "bar"}, {"foo": 1, "bar": 2})
    variable.set("foo")
    assert variable.get() == "foo"
    assert variable.get(to_db=True) == 1
    variable.set(2, from_db=True)
    assert variable.get() == "bar"
    assert variable.get(to_db=True) == 2
    with pytest.raises(ValueError):
        variable.set("foobar")
    with pytest.raises(ValueError):
        variable.set(2)


def test_enum_in_map():
    variable = EnumVariable({1: "foo", 2: "bar"}, {"one": 1, "two": 2})
    variable.set("one")
    assert variable.get() == "foo"
    assert variable.get(to_db=True) == 1
    variable.set(2, from_db=True)
    assert variable.get() == "bar"
    assert variable.get(to_db=True) == 2
    with pytest.raises(ValueError):
        variable.set("foo")
    with pytest.raises(ValueError):
        variable.set(2)
