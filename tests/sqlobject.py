import datetime

from storm.database import create_database
from storm.exceptions import NoneError
from storm.sqlobject import *
from storm.store import Store
from storm.expr import Asc
from storm.tz import tzutc

from tests.helper import TestHelper


class SQLObjectTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)

        self.store = Store(create_database("sqlite:"))
        class SQLObject(SQLObjectBase):
            @staticmethod
            def _get_store():
                return self.store

        self.SQLObject = SQLObject

        self.store.execute("CREATE TABLE person "
                           "(id INTEGER PRIMARY KEY, name TEXT, age INTEGER,"
                           " ts TIMESTAMP)")
        self.store.execute("INSERT INTO person VALUES "
                           "(1, 'John Joe', 20, '2007-02-05 19:53:15')")
        self.store.execute("INSERT INTO person VALUES "
                           "(2, 'John Doe', 20, '2007-02-05 20:53:15')")

        class Person(self.SQLObject):
            _defaultOrder = "-name"
            name = StringCol()
            age = IntCol()
            ts = UtcDateTimeCol()

        self.Person = Person

    def test_get(self):
        person = self.Person.get(2)
        self.assertTrue(person)
        self.assertEquals(person.name, "John Doe")

    def test_custom_table_name(self):
        class MyPerson(self.Person):
            _table = "person"

        person = MyPerson.get(2)

        self.assertTrue(person)
        self.assertEquals(person.name, "John Doe")

    def test_custom_id_name(self):
        class MyPerson(self.Person):
            _table = "person"
            _idName = "name"
            _idType = unicode

        person = MyPerson.get("John Doe")

        self.assertTrue(person)
        self.assertEquals(person.name, "John Doe")

    def test_create(self):
        person = self.Person(name="John Joe")

        self.store.flush()

        self.assertTrue(Store.of(person) is self.store)
        self.assertEquals(type(person.id), int)
        self.assertEquals(person.name, "John Joe")

    def test_select(self):
        result = self.Person.select("name = 'John Joe'")
        self.assertEquals(result[0].name, "John Joe")

    def test_select_orderBy(self):
        result = self.Person.select("name LIKE 'John%'", orderBy=("name","id"))
        self.assertEquals(result[0].name, "John Doe")

    def test_select_orderBy_expr(self):
        result = self.Person.select("name LIKE 'John%'",
                                    orderBy=self.Person.name)
        self.assertEquals(result[0].name, "John Doe")

    def test_select_all(self):
        result = self.Person.select()
        self.assertEquals(result[0].name, "John Joe")

    def test_selectBy(self):
        result = self.Person.selectBy(name="John Joe")
        self.assertEquals(result[0].name, "John Joe")

    def test_selectOne(self):
        person = self.Person.selectOne("name = 'John Joe'")

        self.assertTrue(person)
        self.assertEquals(person.name, "John Joe")

        nobody = self.Person.selectOne("name = 'John None'")

        self.assertEquals(nobody, None)

    def test_selectOneBy(self):
        person = self.Person.selectOneBy(name="John Joe")

        self.assertTrue(person)
        self.assertEquals(person.name, "John Joe")

        nobody = self.Person.selectOneBy(name="John None")

        self.assertEquals(nobody, None)

    def test_selectFirst(self):
        person = self.Person.selectFirst("name LIKE 'John%'", orderBy="name")

        self.assertTrue(person)
        self.assertEquals(person.name, "John Doe")

        person = self.Person.selectFirst("name LIKE 'John%'", orderBy="-name")

        self.assertTrue(person)
        self.assertEquals(person.name, "John Joe")

        nobody = self.Person.selectFirst("name = 'John None'", orderBy="name")

        self.assertEquals(nobody, None)

    def test_selectFirst_default_order(self):
        person = self.Person.selectFirst("name LIKE 'John%'")

        self.assertTrue(person)
        self.assertEquals(person.name, "John Joe")

    def test_selectFirstBy(self):
        person = self.Person.selectFirstBy(age=20, orderBy="name")

        self.assertTrue(person)
        self.assertEquals(person.name, "John Doe")

        person = self.Person.selectFirstBy(age=20, orderBy="-name")

        self.assertTrue(person)
        self.assertEquals(person.name, "John Joe")

        nobody = self.Person.selectFirstBy(age=1000, orderBy="name")

        self.assertEquals(nobody, None)

    def test_selectFirstBy_default_order(self):
        person = self.Person.selectFirstBy(age=20)

        self.assertTrue(person)
        self.assertEquals(person.name, "John Joe")

    def test_dummy_methods(self):
        person = self.Person.get(id=1)
        person.sync()
        person.syncUpdate()

    def test_string_col(self):
        class Person(self.SQLObject):
            foo = StringCol("name", notNull=True)
        person = Person.get(2)
        self.assertEquals(person.foo, "John Doe")
        self.assertRaises(NoneError, setattr, person, "foo", None)

    def test_int_col(self):
        class Person(self.SQLObject):
            foo = IntCol("age", notNull=True)
        person = Person.get(2)
        self.assertEquals(person.foo, 20)
        self.assertRaises(NoneError, setattr, person, "foo", None)

    def test_bool_col(self):
        class Person(self.SQLObject):
            foo = BoolCol("age", notNull=True)
        person = Person.get(2)
        self.assertEquals(person.foo, True)
        self.assertRaises(NoneError, setattr, person, "foo", None)

    def test_utcdatetime_col(self):
        class Person(self.SQLObject):
            foo = UtcDateTimeCol("ts", notNull=True)
        person = Person.get(2)
        self.assertEquals(person.foo,
                          datetime.datetime(2007, 2, 5, 20, 53, 15,
                                            tzinfo=tzutc()))
        self.assertRaises(NoneError, setattr, person, "foo", None)
