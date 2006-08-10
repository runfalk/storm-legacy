from datetime import datetime, date, time
import cPickle as pickle
import shutil
import sys
import os

from storm.uri import URI
from storm.expr import Select, Column, Undef
from storm.variables import Variable, PickleVariable
from storm.variables import DateTimeVariable, DateVariable, TimeVariable
from storm.database import *
from storm.exceptions import DatabaseModuleError

from tests.helper import MakePath


class DatabaseTest(object):

    supports_microseconds = True

    def setUp(self):
        self.create_database()
        self.create_connection()
        self.drop_tables()
        self.create_tables()
        self.create_sample_data()

    def tearDown(self):
        self.drop_sample_data()
        self.drop_tables()
        self.drop_database()

    def create_database(self):
        raise NotImplementedError

    def create_connection(self):
        self.connection = self.database.connect()

    def create_tables(self):
        raise NotImplementedError

    def create_sample_data(self):
        self.connection.execute("INSERT INTO test VALUES (10, 'Title 10')")
        self.connection.execute("INSERT INTO test VALUES (20, 'Title 20')")
        self.connection.commit()

    def drop_sample_data(self):
        pass

    def drop_tables(self):
        try:
            self.connection.execute("DROP TABLE test")
            self.connection.commit()
        except:
            self.connection.rollback()
        try:
            self.connection.execute("DROP TABLE datetime_test")
            self.connection.commit()
        except:
            self.connection.rollback()
        try:
            self.connection.execute("DROP TABLE bin_test")
            self.connection.commit()
        except:
            self.connection.rollback()

    def drop_database(self):
        pass

    def test_create(self):
        self.assertTrue(isinstance(self.database, Database))
        
    def test_connection(self):
        self.assertTrue(isinstance(self.connection, Connection))

    def test_execute_result(self):
        result = self.connection.execute("SELECT 1")
        self.assertTrue(isinstance(result, Result))

    def test_execute_params(self):
        result = self.connection.execute("SELECT 1 FROM (SELECT 1) AS ALIAS "
                                         "WHERE 1=?", (1,))
        self.assertTrue(result.get_one())
        result = self.connection.execute("SELECT 1 FROM (SELECT 1) AS ALIAS "
                                         "WHERE 1=?", (2,))
        self.assertFalse(result.get_one())

    def test_get_one(self):
        result = self.connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEquals(result.get_one(), (10, "Title 10"))

    def test_get_all(self):
        result = self.connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEquals(result.get_all(),
                          [(10, "Title 10"), (20, "Title 20")])

    def test_iter(self):
        result = self.connection.execute("SELECT * FROM test ORDER BY id")
        self.assertEquals([item for item in result],
                          [(10, "Title 10"), (20, "Title 20")])

    def test_simultaneous_iter(self):
        result1 = self.connection.execute("SELECT * FROM test "
                                          "ORDER BY id ASC")
        result2 = self.connection.execute("SELECT * FROM test "
                                          "ORDER BY id DESC")
        iter1 = iter(result1)
        iter2 = iter(result2)
        self.assertEquals(iter1.next(), (10, "Title 10"))
        self.assertEquals(iter2.next(), (20, "Title 20"))
        self.assertEquals(iter1.next(), (20, "Title 20"))
        self.assertEquals(iter2.next(), (10, "Title 10"))
        self.assertRaises(StopIteration, iter1.next)
        self.assertRaises(StopIteration, iter2.next)

    def test_get_insert_identity(self):
        result = self.connection.execute("INSERT INTO test (title) "
                                         "VALUES ('Title 30')")
        primary_key = (Column("id", "test"),)
        primary_variables = (Variable(),)
        expr = result.get_insert_identity(primary_key, primary_variables)
        result = self.connection.execute(Select(Column("title", "test"), expr))
        self.assertEquals(result.get_one(), ("Title 30",))

    def test_get_insert_identity_composed(self):
        result = self.connection.execute("INSERT INTO test (title) "
                                         "VALUES ('Title 30')")
        primary_key = (Column("id", "test"), Column("title", "test"))
        primary_variables = (Variable(), Variable("Title 30"))
        expr = result.get_insert_identity(primary_key, primary_variables)
        result = self.connection.execute(Select(Column("title", "test"), expr))
        self.assertEquals(result.get_one(), ("Title 30",))

    def test_datetime(self):
        value = datetime(1977, 4, 5, 12, 34, 56, 78)
        self.connection.execute("INSERT INTO datetime_test (dt) VALUES (?)",
                                (value,))
        result = self.connection.execute("SELECT dt FROM datetime_test")
        variable = DateTimeVariable()
        result.set_variable(variable, result.get_one()[0])
        if not self.supports_microseconds:
            value = value.replace(microsecond=0)
        self.assertEquals(variable.get(), value)

    def test_date(self):
        value = date(1977, 4, 5)
        self.connection.execute("INSERT INTO datetime_test (d) VALUES (?)",
                                (value,))
        result = self.connection.execute("SELECT d FROM datetime_test")
        variable = DateVariable()
        result.set_variable(variable, result.get_one()[0])
        self.assertEquals(variable.get(), value)

    def test_time(self):
        value = time(12, 34, 56, 78)
        self.connection.execute("INSERT INTO datetime_test (t) VALUES (?)",
                                (value,))
        result = self.connection.execute("SELECT t FROM datetime_test")
        variable = TimeVariable()
        result.set_variable(variable, result.get_one()[0])
        if not self.supports_microseconds:
            value = value.replace(microsecond=0)
        self.assertEquals(variable.get(), value)

    def test_pickle(self):
        value = {"a": 1, "b": 2}
        value_dump = pickle.dumps(value, -1)
        self.connection.execute("INSERT INTO bin_test (b) VALUES (?)",
                                (value_dump,))
        result = self.connection.execute("SELECT b FROM bin_test")
        variable = PickleVariable()
        result.set_variable(variable, result.get_one()[0])
        self.assertEquals(variable.get(), value)


class UnsupportedDatabaseTest(object):
    
    helpers = [MakePath]

    dbapi_module_name = None
    db_module_name = None

    def test_exception_when_unsupported(self):

        # Install a directory in front of the search path.
        module_dir = self.make_path()
        os.mkdir(module_dir)
        sys.path.insert(0, module_dir)

        # If the real module is available, remove it from the sys.modules.
        dbapi_module = sys.modules.get(self.dbapi_module_name)
        if dbapi_module is not None:
            del sys.modules[self.dbapi_module_name]

        # Create a module which raises ImportError when imported, to fake
        # a missing module.
        self.make_path("raise ImportError",
                       os.path.join(module_dir, self.dbapi_module_name+".py"))

        # Copy the real module over to a new place, since the old one is
        # already using the real module, if it's available.
        db_module = __import__("storm.databases."+self.db_module_name,
                               None, None, [""])
        db_module_filename = db_module.__file__
        if db_module_filename.endswith(".pyc"):
            db_module_filename = db_module_filename[:-1]
        shutil.copyfile(db_module_filename,
                        os.path.join(module_dir, "_fake_.py"))

        # Finally, test it.
        import _fake_
        uri = URI.parse("_fake_://db")

        try:
            self.assertRaises(DatabaseModuleError,
                              _fake_.create_from_uri, uri)
        finally:
            # Unhack the environment.
            del sys.path[0]
            del sys.modules["_fake_"]

            if dbapi_module is not None:
                sys.modules[self.dbapi_module_name] = dbapi_module
