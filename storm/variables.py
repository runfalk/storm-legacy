#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
from datetime import datetime, date, time
import cPickle as pickle

from storm.exceptions import NoneError
from storm import Undef


__all__ = [
    "VariableFactory",
    "Variable",
    "LazyValue",
    "BoolVariable",
    "IntVariable",
    "FloatVariable",
    "StrVariable",
    "UnicodeVariable",
    "DateTimeVariable",
    "DateVariable",
    "TimeVariable",
    "EnumVariable",
    "PickleVariable",
    "ListVariable",
]


def VariableFactory(cls, **old_kwargs):
    """Build cls with kwargs of constructor updated by kwargs of call.

    This is really an implementation of partial/curry functions, and
    should be replaced by 'partial' once 2.5 is in use.
    """
    def variable_factory(**new_kwargs):
        kwargs = old_kwargs.copy()
        kwargs.update(new_kwargs)
        return cls(**kwargs)
    return variable_factory


class Variable(object):

    _value = Undef
    _lazy_value = Undef
    _saved_state = Undef
    _checkpoint_state = Undef
    _allow_none = True

    column = None
    event = None

    def __init__(self, value=Undef, value_factory=Undef, from_db=False,
                 allow_none=True, column=None, event=None):
        if not allow_none:
            self._allow_none = False
        if value is not Undef:
            self.set(value, from_db)
        elif value_factory is not Undef:
            self.set(value_factory(), from_db)
        self.column = column
        self.event = event

    @staticmethod
    def _parse_get(value, to_db):
        return value

    @staticmethod
    def _parse_set(value, from_db):
        return value

    def get_lazy(self, default=None):
        if self._lazy_value is Undef:
            return default
        return self._lazy_value

    def get(self, default=None, to_db=False):
        if self._lazy_value is not Undef and self.event is not None:
            self.event.emit("resolve-lazy-value", self, self._lazy_value)
        value = self._value
        if value is Undef:
            return default
        if value is None:
            return value
        return self._parse_get(value, to_db)

    def set(self, value, from_db=False):
        if isinstance(value, LazyValue):
            self._lazy_value = value
            new_value = Undef
        else:
            if self._lazy_value is not Undef:
                del self._lazy_value
            if value is None:
                if self._allow_none is False:
                    raise self._get_none_error()
                new_value = None
            else:
                new_value = self._parse_set(value, from_db)
                if from_db:
                    # Prepare it for being used by the hook below.
                    value = self._parse_get(new_value, False)
        old_value = self._value
        self._value = new_value
        if (self.event is not None and
            (self._lazy_value is not Undef or new_value is not old_value)):
            if old_value is not None and old_value is not Undef:
                old_value = self._parse_get(old_value, False)
            self.event.emit("changed", self, old_value, value, from_db)

    def delete(self):
        old_value = self._value
        if old_value != Undef:
            self._value = Undef
            if self.event:
                if old_value is not None and old_value is not Undef:
                    old_value = self._parse_get(old_value, False)
                self.event.emit("changed", self, old_value, Undef, False)

    def is_defined(self):
        return self._value is not Undef

    def has_changed(self):
        return (self._lazy_value is not Undef or
                self.get_state() != self._checkpoint_state)

    def get_state(self):
        return (self._lazy_value, self._value)

    def set_state(self, state):
        self._lazy_value, self._value = state

    def save(self):
        self._saved_state = self._checkpoint_state = self.get_state()

    def restore(self):
        self.set_state(self._saved_state)
        self._checkpoint_state = self._saved_state

    def checkpoint(self):
        self._checkpoint_state = self.get_state()

    def copy(self):
        variable = object.__new__(self.__class__)
        variable.set_state(self.get_state())
        return variable

    def __eq__(self, other):
        return (self.__class__ is other.__class__ and
                self._value == other._value)

    def __hash__(self):
        return hash(self._value)

    def _get_none_error(self):
        if not self.column:
            return NoneError("None isn't acceptable as a value")
        else:
            from storm.expr import compile, CompileError
            column = self.column.name
            if self.column.table is not Undef:
                try:
                    table, parameters = compile(self.column.table)
                    column = "%s.%s" % (table, column)
                except CompileError:
                    pass
            return NoneError("None isn't acceptable as a value for %s"
                             % column)


class LazyValue(object):
    """Marker to be used as a base class on lazily evaluated values."""


class BoolVariable(Variable):

    @staticmethod
    def _parse_set(value, from_db):
        return bool(value)


class IntVariable(Variable):

    @staticmethod
    def _parse_set(value, from_db):
        return int(value)


class FloatVariable(Variable):

    @staticmethod
    def _parse_set(value, from_db):
        return float(value)


class StrVariable(Variable):

    @staticmethod
    def _parse_set(value, from_db):
        return str(value)


class UnicodeVariable(Variable):

    @staticmethod
    def _parse_set(value, from_db):
        return unicode(value)


class DateTimeVariable(Variable):

    def __init__(self, *args, **kwargs):
        self._tzinfo = kwargs.pop("tzinfo", None)
        super(DateTimeVariable, self).__init__(*args, **kwargs)

    def _parse_set(self, value, db):
        if db:
            if isinstance(value, datetime):
                pass
            elif isinstance(value, (str, unicode)):
                if " " not in value:
                    raise ValueError("Unknown date/time format: %r" % value)
                date_str, time_str = value.split(" ")
                value = datetime(*(_parse_date(date_str) +
                                   _parse_time(time_str)))
            else:
                raise TypeError("Expected datetime, found %s" % repr(value))
            if self._tzinfo is not None:
                if value.tzinfo is None:
                    value = value.replace(tzinfo=self._tzinfo)
                else:
                    value = value.astimezone(self._tzinfo)
        else:
            if type(value) in (int, long, float):
                value = datetime.utcfromtimestamp(value)
            elif not isinstance(value, datetime):
                raise TypeError("Expected datetime, found %s" % repr(value))
            if self._tzinfo is not None:
                value = value.astimezone(self._tzinfo)
        return value


class DateVariable(Variable):

    @staticmethod
    def _parse_set(value, db):
        if db:
            if value is None:
                return None
            if isinstance(value, date):
                return value
            if not isinstance(value, (str, unicode)):
                raise TypeError("Expected date, found %s" % repr(value))
            if " " in value:
                value, time_str = value.split(" ")
            return date(*_parse_date(value))
        else:
            if isinstance(value, datetime):
                return value.date()
            if not isinstance(value, date):
                raise TypeError("Expected date, found %s" % repr(value))
            return value


class TimeVariable(Variable):

    @staticmethod
    def _parse_set(value, db):
        if db:
            # XXX Can None ever get here, considering that set() checks for it?
            if value is None:
                return None
            if isinstance(value, time):
                return value
            if not isinstance(value, (str, unicode)):
                raise TypeError("Expected time, found %s" % repr(value))
            if " " in value:
                date_str, value = value.split(" ")
            return time(*_parse_time(value))
        else:
            if isinstance(value, datetime):
                return value.time()
            if not isinstance(value, time):
                raise TypeError("Expected time, found %s" % repr(value))
            return value


class EnumVariable(Variable):

    def __init__(self, map, *args, **kwargs):
        # XXX These maps should be cached in the property.
        self._py_to_db = dict(map)
        self._db_to_py = dict((value, key)
                              for key, value in self._py_to_db.items())
        Variable.__init__(self, *args, **kwargs)

    def _parse_set(self, value, db):
        if db:
            return value
        try:
            return self._py_to_db[value]
        except KeyError:
            raise ValueError("Invalid enum value: %s" % repr(value))

    def _parse_get(self, value, db):
        if db:
            return value
        try:
            return self._db_to_py[value]
        except KeyError:
            raise ValueError("Invalid enum value: %s" % repr(value))


class PickleVariable(Variable):

    def __init__(self, *args, **kwargs):
        Variable.__init__(self, *args, **kwargs)
        if self.event:
            self.event.hook("flush", self._detect_changes)
            self.event.hook("object-deleted", self._detect_changes)

    def _detect_changes(self, obj_info):
        if self.get_state() != self._checkpoint_state:
            self.event.emit("changed", self, None, self._value, False)

    @staticmethod
    def _parse_set(value, db):
        if db:
            return pickle.loads(value)
        else:
            return value

    @staticmethod
    def _parse_get(value, db):
        if db:
            return pickle.dumps(value, -1)
        else:
            return value

    def get_state(self):
        return (self._lazy_value, pickle.dumps(self._value, -1))

    def set_state(self, state):
        self._lazy_value = state[0]
        self._value = pickle.loads(state[1])

    def __hash__(self):
        try:
            return hash(self._value)
        except TypeError:
            return hash(pickle.dumps(self._value, -1))


class ListVariable(Variable):

    def __init__(self, item_factory, *args, **kwargs):
        self._item_factory = item_factory
        value_factory = kwargs.get("value_factory", Undef)
        if value_factory is Undef:
            kwargs["value_factory"] = list
        Variable.__init__(self, *args, **kwargs)
        if self.event:
            self.event.hook("flush", self._detect_changes)
            self.event.hook("object-deleted", self._detect_changes)

    def _detect_changes(self, obj_info):
        if self.get_state() != self._checkpoint_state:
            self.event.emit("changed", self, None, self._value, False)

    def _parse_set(self, value, db):
        if db:
            item_factory = self._item_factory
            return [item_factory(value=val, from_db=db).get() for val in value]
        else:
            return value

    def _parse_get(self, value, db):
        if db:
            item_factory = self._item_factory
            return [item_factory(value=val, from_db=db) for val in value]
        else:
            return value

    def get_state(self):
        return (self._lazy_value, pickle.dumps(self._value, -1))

    def set_state(self, state):
        self._lazy_value = state[0]
        self._value = pickle.loads(state[1])

    def __hash__(self):
        return hash(pickle.dumps(self._value, -1))


def _parse_time(time_str):
    # TODO Add support for timezones.
    if ":" not in time_str:
        raise ValueError("Unknown time format: %r" % time_str)
    hour, minute, second = time_str.split(":")
    if "." in second:
        fsecond = float(second)
        second = int(fsecond)
        return int(hour), int(minute), second, int((fsecond-second)*1000000)
    return int(hour), int(minute), int(second), 0

def _parse_date(date_str):
    if "-" not in date_str:
        raise ValueError("Unknown date format: %r" % date_str)
    year, month, day = date_str.split("-")
    return int(year), int(month), int(day)
