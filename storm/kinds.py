from datetime import datetime, date, time


class Kind(object):

    @staticmethod
    def to_python(value):
        return value

    @staticmethod
    def to_database(value):
        return value

    @staticmethod
    def from_python(value):
        return value

    @staticmethod
    def from_database(value):
        return value


class AnyKind(Kind):
    pass


class BoolKind(Kind):

    from_python = from_database = staticmethod(bool)


class IntKind(Kind):

    from_python = from_database = staticmethod(int)


class FloatKind(Kind):

    from_python = from_database = staticmethod(float)


class StrKind(Kind):

    from_python = from_database = staticmethod(str)


class UnicodeKind(Kind):

    def __init__(self, encoding=None):
        self.encoding = encoding

    def to_database(self, value):
        if self.encoding is None:
            return value
        return value.encode(self.encoding)

    def from_python(self, value):
        if isinstance(value, unicode):
            return value
        elif self.encoding is None:
            return unicode(value)
        return unicode(value, self.encoding)

    from_database = from_python


class DateTimeKind(Kind):

    @staticmethod
    def from_python(value):
        if type(value) in (int, long, float):
            value = datetime.utcfromtimestamp(value)
        elif not isinstance(value, datetime):
            raise TypeError("Expected datetime, found %s" % repr(value))
        return value

    @staticmethod
    def from_database(value):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if not isinstance(value, (str, unicode)):
            raise TypeError("Expected datetime, found %s" % repr(value))
        if " " not in value:
            raise ValueError("Unknown date/time format: %r" % value)
        date_str, time_str = value.split(" ")
        return datetime(*(_parse_date(date_str)+_parse_time(time_str)))


class DateKind(Kind):

    @staticmethod
    def from_python(value):
        if isinstance(value, datetime):
            return value.date()
        if not isinstance(value, date):
            raise TypeError("Expected date, found %s" % repr(value))
        return value

    @staticmethod
    def from_database(value):
        if value is None:
            return None
        if isinstance(value, date):
            return value
        if not isinstance(value, (str, unicode)):
            raise TypeError("Expected date, found %s" % repr(value))
        return date(*_parse_date(value))


class TimeKind(Kind):

    @staticmethod
    def from_python(value):
        if isinstance(value, datetime):
            return value.time()
        if not isinstance(value, time):
            raise TypeError("Expected time, found %s" % repr(value))
        return value

    @staticmethod
    def from_database(value):
        if value is None:
            return None
        if isinstance(value, time):
            return value
        if not isinstance(value, (str, unicode)):
            raise TypeError("Expected time, found %s" % repr(value))
        return time(*_parse_time(value))


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
