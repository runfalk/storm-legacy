import sys
import new
import gc

from storm.exceptions import ClosedError
from storm.variables import Variable
import storm.database
from storm.database import *
from storm.uri import URI
from storm.expr import *

from tests.helper import TestHelper


marker = object()


class RawConnection(object):

    closed = False

    def __init__(self, executed):
        self.executed = executed

    def cursor(self):
        return RawCursor(executed=self.executed)

    def commit(self):
        self.executed.append("COMMIT")

    def rollback(self):
        self.executed.append("ROLLBACK")

    def close(self):
        self.executed.append("CCLOSE")


class RawCursor(object):

    def __init__(self, arraysize=1, executed=None):
        self.arraysize = arraysize
        if executed is None:
            self.executed = []
        else:
            self.executed = executed

        self._fetchone_data = [("fetchone%d" % i,) for i in range(3)]
        self._fetchall_data = [("fetchall%d" % i,) for i in range(2)]
        self._fetchmany_data = [("fetchmany%d" % i,) for i in range(5)]

    def close(self):
        self.executed.append("RCLOSE")

    def execute(self, statement, params=marker):
        self.executed.append((statement, params))

    def fetchone(self):
        if self._fetchone_data:
            return self._fetchone_data.pop(0)
        return None

    def fetchall(self):
        result = self._fetchall_data
        self._fetchall_data = []
        return result

    def fetchmany(self):
        result = self._fetchmany_data[:self.arraysize]
        del self._fetchmany_data[:self.arraysize]
        return result


class DatabaseTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        self.database = Database()

    def test_connect(self):
        self.assertRaises(NotImplementedError, self.database.connect)


class ConnectionTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        self.executed = []
        self.database = Database()
        self.raw_connection = RawConnection(self.executed)
        self.connection = Connection(self.database, self.raw_connection)

    def test_execute(self):
        result = self.connection.execute("something")
        self.assertTrue(isinstance(result, Result))
        self.assertEquals(self.executed, [("something", marker)])

    def test_execute_params(self):
        result = self.connection.execute("something", (1,2,3))
        self.assertTrue(isinstance(result, Result))
        self.assertEquals(self.executed, [("something", (1,2,3))])

    def test_execute_noresult(self):
        result = self.connection.execute("something", noresult=True)
        self.assertEquals(result, None)
        self.assertEquals(self.executed, [("something", marker), "RCLOSE"])

    def test_execute_convert_param_style(self):
        self.connection.execute("'?' ? '?' ? '?'")
        self.assertEquals(self.executed, [("'?' ? '?' ? '?'", marker)])

    def test_execute_convert_param_style(self):
        class MyConnection(Connection):
            _param_mark = "%s"
        connection = MyConnection(self.database, RawConnection(self.executed))
        result = connection.execute("'?' ? '?' ? '?'")
        self.assertEquals(self.executed, [("'?' %s '?' %s '?'", marker)])

        # TODO: Unsupported for now.
        #result = connection.execute("$$?$$ ? $asd$'?$asd$ ? '?'")
        #self.assertEquals(self.executed,
        #                  [("'?' %s '?' %s '?'", marker),
        #                   ("$$?$$ %s $asd'?$asd$ %s '?'", marker)])

    def test_execute_select(self):
        select = Select([SQLToken("column1"), SQLToken("column2")],
                        tables=[SQLToken("table1"), SQLToken("table2")])
        result = self.connection.execute(select)
        self.assertTrue(isinstance(result, Result))
        self.assertEquals(self.executed,
                          [("SELECT column1, column2 FROM table1, table2",
                            ())])

    def test_execute_select_and_params(self):
        select = Select(["column1", "column2"], tables=["table1", "table2"])
        self.assertRaises(ValueError, self.connection.execute,
                          select, ("something",))

    def test_execute_closed(self):
        self.connection.close()
        self.assertRaises(ClosedError, self.connection.execute, "SELECT 1")

    def test_commit(self):
        self.connection.commit()
        self.assertEquals(self.executed, ["COMMIT"])

    def test_rollback(self):
        self.connection.rollback()
        self.assertEquals(self.executed, ["ROLLBACK"])

    def test_close(self):
        self.connection.close()
        self.assertEquals(self.executed, ["CCLOSE"])

    def test_close_twice(self):
        self.connection.close()
        self.connection.close()
        self.assertEquals(self.executed, ["CCLOSE"])

    def test_close_deallocates_raw_connection(self):
        refs_before = len(gc.get_referrers(self.raw_connection))
        self.connection.close()
        refs_after = len(gc.get_referrers(self.raw_connection))
        self.assertEquals(refs_after, refs_before-1)

    def test_del_deallocates_raw_connection(self):
        refs_before = len(gc.get_referrers(self.raw_connection))
        self.connection.__del__()
        refs_after = len(gc.get_referrers(self.raw_connection))
        self.assertEquals(refs_after, refs_before-1)

    def test_wb_del_with_previously_deallocated_connection(self):
        self.connection._raw_connection = None
        self.connection.__del__()

    def test_get_insert_identity(self):
        result = self.connection.execute("INSERT")
        self.assertRaises(NotImplementedError,
                          result.get_insert_identity, None, None)

class ResultTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        self.executed = []
        self.raw_cursor = RawCursor(executed=self.executed)
        self.result = Result(None, self.raw_cursor)

    def test_get_one(self):
        self.assertEquals(self.result.get_one(), ("fetchone0",))
        self.assertEquals(self.result.get_one(), ("fetchone1",))
        self.assertEquals(self.result.get_one(), ("fetchone2",))
        self.assertEquals(self.result.get_one(), None)

    def test_get_all(self):
        self.assertEquals(self.result.get_all(),
                          [("fetchall0",), ("fetchall1",)])
        self.assertEquals(self.result.get_all(), [])

    def test_iter_arraysize_1(self):
        self.assertEquals([item for item in self.result],
                          [("fetchone0",), ("fetchone1",), ("fetchone2",),])

    def test_iter_arraysize_2(self):
        result = Result(None, RawCursor(2))
        self.assertEquals([item for item in result],
                          [("fetchmany0",), ("fetchmany1",), ("fetchmany2",),
                           ("fetchmany3",), ("fetchmany4",)])

    def test_set_variable(self):
        variable = Variable()
        self.result.set_variable(variable, marker)
        self.assertEquals(variable.get(), marker)

    def test_close(self):
        self.result.close()
        self.assertEquals(self.executed, ["RCLOSE"])

    def test_close_twice(self):
        self.result.close()
        self.result.close()
        self.assertEquals(self.executed, ["RCLOSE"])

    def test_close_deallocates_raw_cursor(self):
        refs_before = len(gc.get_referrers(self.raw_cursor))
        self.result.close()
        refs_after = len(gc.get_referrers(self.raw_cursor))
        self.assertEquals(refs_after, refs_before-1)

    def test_del_deallocates_raw_cursor(self):
        refs_before = len(gc.get_referrers(self.raw_cursor))
        self.result.__del__()
        refs_after = len(gc.get_referrers(self.raw_cursor))
        self.assertEquals(refs_after, refs_before-1)

    def test_wb_del_with_previously_deallocated_cursor(self):
        self.result._raw_cursor = None
        self.result.__del__()


class CreateDatabaseTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        self.db_module = new.module("db_module")
        self.uri = None
        def create_from_uri(uri):
            self.uri = uri
            return "RESULT"
        self.db_module.create_from_uri = create_from_uri
        sys.modules["storm.databases.db_module"] = self.db_module

    def tearDown(self):
        del sys.modules["storm.databases.db_module"]
        TestHelper.tearDown(self)

    def test_create_database_with_str(self):
        create_database("db_module:db")
        self.assertTrue(self.uri)
        self.assertEquals(self.uri.scheme, "db_module")
        self.assertEquals(self.uri.database, "db")

    def test_create_database_with_unicode(self):
        create_database(u"db_module:db")
        self.assertTrue(self.uri)
        self.assertEquals(self.uri.scheme, "db_module")
        self.assertEquals(self.uri.database, "db")

    def test_create_database_with_uri(self):
        uri = URI.parse("db_module:db")
        create_database(uri)
        self.assertTrue(self.uri is uri)


class RegisterSchemeTest(TestHelper):

    uri = None

    def tearDown(self):
        if 'factory' in storm.database._database_schemes:
            del storm.database._database_schemes['factory']
        TestHelper.tearDown(self)
    
    def test_register_scheme(self):
        def factory(uri):
            self.uri = uri
            return "FACTORY RESULT"
        register_scheme('factory', factory)
        self.assertEqual(storm.database._database_schemes['factory'], factory)
        # Check that we can create databases that use this scheme ...
        result = create_database('factory:foobar')
        self.assertEqual(result, "FACTORY RESULT")
        self.assertTrue(self.uri)
        self.assertEqual(self.uri.scheme, 'factory')
        self.assertEqual(self.uri.database, 'foobar')
