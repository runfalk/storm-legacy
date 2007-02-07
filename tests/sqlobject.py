import datetime

from storm.database import create_database
from storm.exceptions import NoneError
from storm.sqlobject import *
from storm.store import Store
from storm.expr import Asc, Like
from storm.tz import tzutc

from tests.helper import TestHelper


class SQLObjectTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)

        # Allow classes with the same name in different tests to resolve
        # property path strings properly.
        SQLObjectBase._storm_property_registry.clear()

        self.store = Store(create_database("sqlite:"))
        class SQLObject(SQLObjectBase):
            @staticmethod
            def _get_store():
                return self.store

        self.SQLObject = SQLObject

        self.store.execute("CREATE TABLE person "
                           "(id INTEGER PRIMARY KEY, name TEXT, age INTEGER,"
                           " ts TIMESTAMP, address_id INTEGER)")
        self.store.execute("INSERT INTO person VALUES "
                           "(1, 'John Joe', 20, '2007-02-05 19:53:15', 1)")
        self.store.execute("INSERT INTO person VALUES "
                           "(2, 'John Doe', 20, '2007-02-05 20:53:15', 2)")

        self.store.execute("CREATE TABLE address "
                           "(id INTEGER PRIMARY KEY, city TEXT)")
        self.store.execute("INSERT INTO address VALUES (1, 'Curitiba')")
        self.store.execute("INSERT INTO address VALUES (2, 'Sao Carlos')")

        self.store.execute("CREATE TABLE phone "
                           "(id INTEGER PRIMARY KEY, person_id INTEGER,"
                           "number TEXT)")
        self.store.execute("INSERT INTO phone VALUES (1, 2, '1234-5678')")
        self.store.execute("INSERT INTO phone VALUES (2, 1, '8765-4321')")
        self.store.execute("INSERT INTO phone VALUES (3, 2, '8765-5678')")

        self.store.execute("CREATE TABLE person_phone "
                           "(person_id INTEGER , phone_id INTEGER)")
        self.store.execute("INSERT INTO person_phone VALUES (2, 1)")
        self.store.execute("INSERT INTO person_phone VALUES (2, 2)")
        self.store.execute("INSERT INTO person_phone VALUES (1, 1)")

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

    def test_select_sqlbuilder(self):
        result = self.Person.select(self.Person.q.name == 'John Joe')
        self.assertEqual(result[0].name, "John Joe")

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

        # SQLBuilder style expression:
        person = self.Person.selectOne(self.Person.q.name == 'John Joe')

        self.assertNotEqual(person, None)
        self.assertEqual(person.name, 'John Joe')

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

        # SQLBuilder style expression:
        # XXX: 20070206 jamesh
        # This should use an sqlbuilder-style LIKE() function instead.
        person = self.Person.selectFirst(self.Person.q.name.like('John%'),
                                         orderBy="name")
        self.assertNotEqual(person, None)
        self.assertEqual(person.name, 'John Doe')

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

    def test_col_name(self):
        class Person(self.SQLObject):
            foo = StringCol(dbName="name")
        person = Person.get(2)
        self.assertEquals(person.foo, "John Doe")

        class Person(self.SQLObject):
            foo = StringCol("name")
        person = Person.get(2)
        self.assertEquals(person.foo, "John Doe")

    def test_col_default(self):
        class Person(self.SQLObject):
            name = StringCol(default="Johny")
        person = Person()
        self.assertEquals(person.name, "Johny")

    def test_col_default_factory(self):
        class Person(self.SQLObject):
            name = StringCol(default=lambda: "Johny")
        person = Person()
        self.assertEquals(person.name, "Johny")

    def test_col_not_null(self):
        class Person(self.SQLObject):
            name = StringCol(notNull=True)
        person = Person.get(2)
        self.assertRaises(NoneError, setattr, person, "name", None)

    def test_string_col(self):
        class Person(self.SQLObject):
            name = StringCol()
        person = Person.get(2)
        self.assertEquals(person.name, "John Doe")

    def test_int_col(self):
        class Person(self.SQLObject):
            age = IntCol()
        person = Person.get(2)
        self.assertEquals(person.age, 20)

    def test_bool_col(self):
        class Person(self.SQLObject):
            age = BoolCol()
        person = Person.get(2)
        self.assertEquals(person.age, True)

    def test_utcdatetime_col(self):
        class Person(self.SQLObject):
            ts = UtcDateTimeCol()
        person = Person.get(2)
        self.assertEquals(person.ts,
                          datetime.datetime(2007, 2, 5, 20, 53, 15,
                                            tzinfo=tzutc()))
    def test_date_col(self):
        class Person(self.SQLObject):
            ts = DateCol()
        person = Person.get(2)
        self.assertEquals(person.ts, datetime.date(2007, 2, 5))

    def test_foreign_key(self):
        class Person(self.Person):
            address = ForeignKey(foreignKey="Address", dbName="address_id",
                                 notNull=True)

        class Address(self.SQLObject):
            city = StringCol()

        person = Person.get(2)

        self.assertEquals(person.addressID, 2)
        self.assertEquals(person.address.city, "Sao Carlos")

    def test_foreign_key_no_dbname(self):
        self.store.execute("CREATE TABLE another_person "
                           "(id INTEGER PRIMARY KEY, name TEXT, age INTEGER,"
                           " ts TIMESTAMP, address INTEGER)")
        self.store.execute("INSERT INTO another_person VALUES "
                           "(2, 'John Doe', 20, '2007-02-05 20:53:15', 2)")

        class AnotherPerson(self.Person):
            address = ForeignKey(foreignKey="Address", notNull=True)

        class Address(self.SQLObject):
            city = StringCol()

        person = AnotherPerson.get(2)

        self.assertEquals(person.addressID, 2)
        self.assertEquals(person.address.city, "Sao Carlos")

    def test_multiple_join(self):
        class AnotherPerson(self.Person):
            _table = "person"
            phones = SQLMultipleJoin("Phone", joinColumn="person_id")

        class Phone(self.SQLObject):
            person_id = IntCol()
            number = StringCol()

        person = AnotherPerson.get(2)

        # Make sure that the result is wrapped.
        result = person.phones.orderBy("-number")

        self.assertEquals([phone.number for phone in result],
                          ["8765-5678", "1234-5678"])

    def test_related_join(self):
        class AnotherPerson(self.Person):
            _table = "person"
            phones = SQLRelatedJoin("Phone", otherColumn="phone_id",
                                    intermediateTable="PersonPhone",
                                    joinColumn="person_id", orderBy="id")

        class PersonPhone(self.Person):
            person_id = IntCol()
            phone_id = IntCol()

        class Phone(self.SQLObject):
            number = StringCol()

        person = AnotherPerson.get(2)

        self.assertEquals([phone.number for phone in person.phones],
                          ["1234-5678", "8765-4321"])

        # Make sure that the result is wrapped.
        result = person.phones.orderBy("-number")

        self.assertEquals([phone.number for phone in result],
                          ["8765-4321", "1234-5678"])

    def test_result_set_orderBy(self):
        result = self.Person.select()

        result = result.orderBy("-name")
        self.assertEquals([person.name for person in result],
                          ["John Joe", "John Doe"])

        result = result.orderBy("name")
        self.assertEquals([person.name for person in result],
                          ["John Doe", "John Joe"])

    def test_result_set_count(self):
        result = self.Person.select()
        self.assertEquals(result.count(), 2)

    def test_result_set__getitem__(self):
        result = self.Person.select()
        self.assertEquals(result[0].name, "John Joe")

    def test_table_dot_q(self):
        # Table.q.fieldname is a syntax used in SQLObject for
        # sqlbuilder expressions.  Storm can use the main properties
        # for this, so the Table.q syntax just returns those
        # properties:
        class Person(self.SQLObject):
            _idName = "name"
            _idType = unicode
            address = ForeignKey(foreignKey="Phone", dbName='address_id',
                                 notNull=True)

        # *.q.id points to the primary key, no matter its name.
        self.assertEquals(id(Person.q.id), id(Person.name))

        self.assertEquals(id(Person.q.name), id(Person.name))
        self.assertEquals(id(Person.q.address), id(Person.address))
        self.assertEquals(id(Person.q.addressID), id(Person.addressID))

        person = Person.get("John Joe")

        self.assertEquals(id(person.q.id), id(Person.name))
        self.assertEquals(id(person.q.name), id(Person.name))
        self.assertEquals(id(person.q.address), id(Person.address))
        self.assertEquals(id(person.q.addressID), id(Person.addressID))
