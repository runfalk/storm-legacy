from datetime import datetime

from storm.kinds import *

from tests.helper import TestHelper


marker = object()


class AnyKindTest(TestHelper):

    kind_class = AnyKind

    def setUp(self):
        TestHelper.setUp(self)
        self.kind = self.kind_class()

    def test_base(self):
        self.assertTrue(isinstance(self.kind, Kind))

    def test_to_python(self):
        self.assertEquals(self.kind.to_python(marker), marker)

    def test_to_database(self):
        self.assertEquals(self.kind.to_database(marker), marker)

    def test_from_python(self):
        self.assertEquals(self.kind.from_python(marker), marker)

    def test_from_database(self):
        self.assertEquals(self.kind.from_database(marker), marker)


class BoolKindTest(AnyKindTest):

    kind_class = BoolKind

    def test_from_python(self):
        self.assertTrue(self.kind.from_python(1) is True)
        self.assertTrue(self.kind.from_python(0) is False)

    def test_from_database(self):
        self.assertTrue(self.kind.from_database(1) is True)
        self.assertTrue(self.kind.from_database(0) is False)


class IntKindTest(AnyKindTest):

    kind_class = IntKind

    def test_from_python(self):
        self.assertEquals(self.kind.from_python("1"), 1)
        self.assertEquals(self.kind.from_python(1.1), 1)

    def test_from_database(self):
        self.assertEquals(self.kind.from_database("1"), 1)
        self.assertEquals(self.kind.from_database(1.1), 1)


class FloatKindTest(AnyKindTest):

    kind_class = FloatKind

    def test_from_python(self):
        self.assertEquals(self.kind.from_python("1.1"), 1.1)
        self.assertEquals(self.kind.from_python(1.1), 1.1)

    def test_from_database(self):
        self.assertEquals(self.kind.from_database("1.1"), 1.1)
        self.assertEquals(self.kind.from_database(1.1), 1.1)


class StrKindTest(AnyKindTest):

    kind_class = StrKind

    def test_from_python(self):
        self.assertEquals(self.kind.from_python(1), "1")
        self.assertFalse(isinstance(self.kind.from_python(u""), unicode))

    def test_from_database(self):
        self.assertEquals(self.kind.from_database(1), "1")
        self.assertFalse(isinstance(self.kind.from_database(u""), unicode))


class UnicodeKindTest(AnyKindTest):

    kind_class = UnicodeKind

    def test_from_python(self):
        self.assertEquals(self.kind.from_python(1), u"1")
        self.assertTrue(isinstance(self.kind.from_python(""), unicode))

    def test_from_database(self):
        self.assertEquals(self.kind.from_database(1), u"1")
        self.assertTrue(isinstance(self.kind.from_database(""), unicode))


class DateTimeKindTest(AnyKindTest):

    kind_class = DateTimeKind

    def test_from_python(self):
        epoch = datetime.utcfromtimestamp(0)
        self.assertEquals(self.kind.from_python(0), epoch)
        self.assertEquals(self.kind.from_python(0.0), epoch)
        self.assertEquals(self.kind.from_python(0L), epoch)
        self.assertEquals(self.kind.from_python(epoch), epoch)
        self.assertRaises(TypeError, self.kind.from_python, marker)

    def test_from_database(self):
        datetime_str = "1977-05-04 12:34:56.78"
        datetime_uni = unicode(datetime_str)
        datetime_obj = datetime(1977, 5, 4, 12, 34, 56, 780000)
        self.assertEquals(self.kind.from_database(datetime_str), datetime_obj)
        self.assertEquals(self.kind.from_database(datetime_uni), datetime_obj)
        self.assertEquals(self.kind.from_database(datetime_obj), datetime_obj)

        datetime_str = "1977-05-04 12:34:56"
        datetime_uni = unicode(datetime_str)
        datetime_obj = datetime(1977, 5, 4, 12, 34, 56)
        self.assertEquals(self.kind.from_database(datetime_str), datetime_obj)
        self.assertEquals(self.kind.from_database(datetime_uni), datetime_obj)
        self.assertEquals(self.kind.from_database(datetime_obj), datetime_obj)

        self.assertEquals(self.kind.from_database(None), None)

        self.assertRaises(TypeError, self.kind.from_database, 0)
        self.assertRaises(TypeError, self.kind.from_database, marker)
        self.assertRaises(ValueError, self.kind.from_database, "foobar")
        self.assertRaises(ValueError, self.kind.from_database, "foo bar")


class DateKindTest(AnyKindTest):

    kind_class = DateKind

    def test_from_python(self):
        epoch = datetime.utcfromtimestamp(0)
        epoch_date = epoch.date()
        self.assertEquals(self.kind.from_python(epoch), epoch_date)
        self.assertEquals(self.kind.from_python(epoch_date), epoch_date)
        self.assertRaises(TypeError, self.kind.from_python, marker)

    def test_from_database(self):
        date_str = "1977-05-04"
        date_uni = unicode(date_str)
        date_obj = date(1977, 5, 4)
        self.assertEquals(self.kind.from_database(date_str), date_obj)
        self.assertEquals(self.kind.from_database(date_uni), date_obj)
        self.assertEquals(self.kind.from_database(date_obj), date_obj)

        self.assertEquals(self.kind.from_database(None), None)

        self.assertRaises(TypeError, self.kind.from_database, 0)
        self.assertRaises(TypeError, self.kind.from_database, marker)
        self.assertRaises(ValueError, self.kind.from_database, "foobar")


class TimeKindTest(AnyKindTest):

    kind_class = TimeKind

    def test_from_python(self):
        epoch = datetime.utcfromtimestamp(0)
        epoch_time = epoch.time()
        self.assertEquals(self.kind.from_python(epoch), epoch_time)
        self.assertEquals(self.kind.from_python(epoch_time), epoch_time)
        self.assertRaises(TypeError, self.kind.from_python, marker)

    def test_from_database(self):
        time_str = "12:34:56.78"
        time_uni = unicode(time_str)
        time_obj = time(12, 34, 56, 780000)
        self.assertEquals(self.kind.from_database(time_str), time_obj)
        self.assertEquals(self.kind.from_database(time_uni), time_obj)
        self.assertEquals(self.kind.from_database(time_obj), time_obj)

        time_str = "12:34:56"
        time_uni = unicode(time_str)
        time_obj = time(12, 34, 56)
        self.assertEquals(self.kind.from_database(time_str), time_obj)
        self.assertEquals(self.kind.from_database(time_uni), time_obj)
        self.assertEquals(self.kind.from_database(time_obj), time_obj)


        self.assertEquals(self.kind.from_database(None), None)

        self.assertRaises(TypeError, self.kind.from_database, 0)
        self.assertRaises(TypeError, self.kind.from_database, marker)
        self.assertRaises(ValueError, self.kind.from_database, "foobar")
