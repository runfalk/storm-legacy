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


class DataTimeKindTest(AnyKindTest):

    kind_class = DateTimeKind

    def test_from_python(self):
        epoch = datetime.utcfromtimestamp(0)
        self.assertEquals(self.kind.from_python(0), epoch)
        self.assertEquals(self.kind.from_python(0.0), epoch)
        self.assertEquals(self.kind.from_python(0L), epoch)
        self.assertEquals(self.kind.from_python(epoch), epoch)
        self.assertRaises(TypeError, self.kind.from_python, marker)

    def test_from_database(self):
        epoch = datetime.utcfromtimestamp(0)
        self.assertEquals(self.kind.from_database(0), epoch)
        self.assertEquals(self.kind.from_database(0.0), epoch)
        self.assertEquals(self.kind.from_database(0L), epoch)
        self.assertEquals(self.kind.from_database(epoch), epoch)
        self.assertRaises(TypeError, self.kind.from_database, marker)
