from datetime import datetime


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

    from_python = from_database = staticmethod(unicode)


class DateTimeKind(Kind):

    @staticmethod
    def from_python(value):
        if type(value) in (int, long, float):
            value = datetime.utcfromtimestamp(value)
        elif not isinstance(value, datetime):
            raise TypeError("Expected datetime, found %s" % repr(value))
        return value

    from_database = from_python
