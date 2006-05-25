from storm.database import *
from storm.expr import *

from tests.helper import TestHelper


marker = object()


class RawConnection(object):

    def __init__(self, executed):
        self.executed = executed

    def cursor(self):
        return RawCursor(executed=self.executed)

    def commit(self):
        self.executed.append("COMMIT")

    def rollback(self):
        self.executed.append("ROLLBACK")

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
        self.executed.append("CLOSE")

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
        self.connection = Connection(self.database,
                                     RawConnection(self.executed))

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
        self.assertEquals(self.executed, [("something", marker), "CLOSE"])

    def test_execute_convert_param_style(self):
        self.connection.execute("'?' ? '?' ? '?'")
        self.assertEquals(self.executed, [("'?' ? '?' ? '?'", marker)])

    def test_execute_convert_param_style(self):
        class MyConnection(Connection):
            _param_mark = "%s"
        connection = MyConnection(self.database, RawConnection(self.executed))
        connection.execute("'?' ? '?' ? '?'")
        self.assertEquals(self.executed, [("'?' %s '?' %s '?'", marker)])

        # TODO: Unsupported for now.
        #connection.execute("$$?$$ ? $asd$'?$asd$ ? '?'")
        #self.assertEquals(self.executed,
        #                  [("'?' %s '?' %s '?'", marker),
        #                   ("$$?$$ %s $asd'?$asd$ %s '?'", marker)])

    def test_execute_select(self):
        select = Select(["column1", "column2"], tables=["table1", "table2"])
        result = self.connection.execute(select)
        self.assertTrue(isinstance(result, Result))
        self.assertEquals(self.executed,
                          [("SELECT column1, column2 FROM table1, table2",
                            ())])

    def test_execute_select_and_params(self):
        select = Select(["column1", "column2"], tables=["table1", "table2"])
        self.assertRaises(ValueError, self.connection.execute,
                          select, ("something",))

    def test_commit(self):
        self.connection.commit()
        self.assertEquals(self.executed, ["COMMIT"])

    def test_rollback(self):
        self.connection.rollback()
        self.assertEquals(self.executed, ["ROLLBACK"])

    def test_get_insert_identity(self):
        result = self.connection.execute("INSERT")
        self.assertRaises(NotImplementedError,
                          result.get_insert_identity, None, None)

class ResultTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        self.result = Result(None, RawCursor())

    def test_fetch_one(self):
        self.assertEquals(self.result.fetch_one(), ("fetchone0",))
        self.assertEquals(self.result.fetch_one(), ("fetchone1",))
        self.assertEquals(self.result.fetch_one(), ("fetchone2",))
        self.assertEquals(self.result.fetch_one(), None)

    def test_fetch_all(self):
        self.assertEquals(self.result.fetch_all(),
                          [("fetchall0",), ("fetchall1",)])
        self.assertEquals(self.result.fetch_all(), [])

    def test_iter_arraysize_1(self):
        self.assertEquals([item for item in self.result],
                          [("fetchone0",), ("fetchone1",), ("fetchone2",),])

    def test_iter_arraysize_2(self):
        result = Result(None, RawCursor(2))
        self.assertEquals([item for item in result],
                          [("fetchmany0",), ("fetchmany1",), ("fetchmany2",),
                           ("fetchmany3",), ("fetchmany4",)])

    def test_to_kind(self):
        obj1, obj2 = object(), object()
        self.assertEquals(self.result.to_kind(obj1, obj2), obj1)
