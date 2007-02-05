from storm.database import create_database
from storm.sqlobject import *
from storm.store import Store
from storm.expr import Asc

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
                           "(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        self.store.execute("INSERT INTO person VALUES (1, 'John Joe', 20)")
        self.store.execute("INSERT INTO person VALUES (2, 'John Doe', 20)")

        class Person(self.SQLObject):
            _defaultOrder = "-name"
            name = StringCol()
            age = IntCol()

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
        john = self.Person.selectOne("name = 'John Joe'")

        self.assertTrue(john)
        self.assertEquals(john.name, "John Joe")

        nobody = self.Person.selectOne("name = 'John None'")

        self.assertEquals(nobody, None)

    def test_selectOneBy(self):
        john = self.Person.selectOneBy(name="John Joe")

        self.assertTrue(john)
        self.assertEquals(john.name, "John Joe")

        nobody = self.Person.selectOneBy(name="John None")

        self.assertEquals(nobody, None)

    def test_selectFirst(self):
        john = self.Person.selectFirst("name LIKE 'John%'", orderBy="name")

        self.assertTrue(john)
        self.assertEquals(john.name, "John Doe")

        john = self.Person.selectFirst("name LIKE 'John%'", orderBy="-name")

        self.assertTrue(john)
        self.assertEquals(john.name, "John Joe")

        nobody = self.Person.selectFirst("name = 'John None'", orderBy="name")

        self.assertEquals(nobody, None)

    def test_selectFirst_default_order(self):
        john = self.Person.selectFirst("name LIKE 'John%'")

        self.assertTrue(john)
        self.assertEquals(john.name, "John Joe")

    def test_selectFirstBy(self):
        john = self.Person.selectFirstBy(age=20, orderBy="name")

        self.assertTrue(john)
        self.assertEquals(john.name, "John Doe")

        john = self.Person.selectFirstBy(age=20, orderBy="-name")

        self.assertTrue(john)
        self.assertEquals(john.name, "John Joe")

        nobody = self.Person.selectFirstBy(age=1000, orderBy="name")

        self.assertEquals(nobody, None)

    def test_selectFirstBy_default_order(self):
        john = self.Person.selectFirstBy(age=20)

        self.assertTrue(john)
        self.assertEquals(john.name, "John Joe")
