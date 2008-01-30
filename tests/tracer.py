from storm.tracer import (trace, install_tracer, remove_tracer_type,
                          remove_all_tracers, debug, DebugTracer,
                          TimeoutTracer, TimeoutError, _tracers)

from tests.mocker import ARGS
from tests.helper import TestHelper


class TracerTest(TestHelper):

    def tearDown(self):
        super(TracerTest, self).tearDown()
        del _tracers[:]

    def test_wb_install_tracer(self):
        c = object()
        d = object()
        install_tracer(c)
        install_tracer(d)
        self.assertEquals(_tracers, [c, d])

    def test_wb_remove_all_tracers(self):
        install_tracer(object())
        remove_all_tracers()
        self.assertEquals(_tracers, [])

    def test_wb_remove_tracer_type(self):
        class C(object): pass
        class D(C): pass
        c = C()
        d1 = D()
        d2 = D()
        install_tracer(d1)
        install_tracer(c)
        install_tracer(d2)
        remove_tracer_type(C)
        self.assertEquals(_tracers, [d1, d2])
        remove_tracer_type(D)
        self.assertEquals(_tracers, [])

    def test_wb_install_debug(self):
        debug(True)
        debug(True)
        self.assertEquals([type(x) for x in _tracers], [DebugTracer])

    def test_wb_remove_debug(self):
        debug(True)
        debug(True)
        debug(False)
        self.assertEquals(_tracers, [])

    def test_trace(self):
        stash = []
        class Tracer(object):
            def m1(_, *args, **kwargs):
                stash.extend(["m1", args, kwargs])
            def m2(_, *args, **kwargs):
                stash.extend(["m2", args, kwargs])

        install_tracer(Tracer())
        trace("m1", 1, 2, c=3)
        trace("m2")
        trace("m3")
        self.assertEquals(stash, ["m1", (1, 2), {"c": 3}, "m2", (), {}])


class DebugTracerTest(TestHelper):

    def setUp(self):
        super(DebugTracerTest, self).setUp()
        self.tracer = DebugTracer()

    def tearDown(self):
        super(DebugTracerTest, self).tearDown()
        del _tracers[:]

    def test_connection_raw_execute(self):
        stderr = self.mocker.replace("sys.stderr")
        stderr.write("EXECUTE: 'STATEMENT', 'PARAMS'\n")
        stderr.flush()
        self.mocker.replay()

        connection = "CONNECTION"
        raw_cursor = "RAW_CURSOR"
        statement = "STATEMENT"
        params = "PARAMS"

        self.tracer.connection_raw_execute(connection, raw_cursor,
                                           statement, params)

    def test_connection_raw_execute_error(self):
        stderr = self.mocker.replace("sys.stderr")
        stderr.write("ERROR: 'ERROR'\n")
        stderr.flush()
        self.mocker.replay()

        connection = "CONNECTION"
        raw_cursor = "RAW_CURSOR"
        statement = "STATEMENT"
        params = "PARAMS"
        error = "ERROR"

        self.tracer.connection_raw_execute_error(connection, raw_cursor,
                                                 statement, params, error)


class TimeoutTracerTestBase(TestHelper):

    def setUp(self):
        super(TimeoutTracerTestBase, self).setUp()
        self.tracer = TimeoutTracer()
        self.raw_cursor = self.mocker.mock()
        self.statement = self.mocker.mock()
        self.params = self.mocker.mock()

        # Some data is kept in the connection, so we use a proxy to
        # allow things we don't care about here to happen.
        class Connection(object): pass
        self.connection = self.mocker.proxy(Connection())

    def tearDown(self):
        super(TimeoutTracerTestBase, self).tearDown()
        del _tracers[:]

    def execute(self):
        self.tracer.connection_raw_execute(self.connection, self.raw_cursor,
                                           self.statement, self.params)

    def execute_raising(self):
        self.assertRaises(TimeoutError, self.tracer.connection_raw_execute,
                          self.connection, self.raw_cursor,
                          self.statement, self.params)


class TimeoutTracerTest(TimeoutTracerTestBase):

    def test_raise_not_implemented(self):
        self.assertRaises(NotImplementedError,
                          self.tracer.connection_raw_execute_error,
                          None, None, None, None, None)
        self.assertRaises(NotImplementedError,
                          self.tracer.set_statement_timeout, None, None)
        self.assertRaises(NotImplementedError,
                          self.tracer.get_remaining_time)

    def test_raise_timeout_error_when_no_remaining_time(self):
        tracer_mock = self.mocker.patch(self.tracer)
        tracer_mock.get_remaining_time()
        self.mocker.result(0)
        self.mocker.replay()

        self.execute_raising()

    def test_raise_timeout_on_granularity(self):
        tracer_mock = self.mocker.patch(self.tracer)

        self.mocker.order()

        tracer_mock.get_remaining_time()
        self.mocker.result(self.tracer.granularity)
        tracer_mock.set_statement_timeout(self.raw_cursor,
                                          self.tracer.granularity)
        tracer_mock.get_remaining_time()
        self.mocker.result(0)
        self.mocker.replay()

        self.execute()
        self.execute_raising()

    def test_wont_raise_timeout_before_granularity(self):
        tracer_mock = self.mocker.patch(self.tracer)

        self.mocker.order()

        tracer_mock.get_remaining_time()
        self.mocker.result(self.tracer.granularity)
        tracer_mock.set_statement_timeout(self.raw_cursor,
                                          self.tracer.granularity)
        tracer_mock.get_remaining_time()
        self.mocker.result(1)
        self.mocker.replay()

        self.execute()
        self.execute()

    def test_always_set_when_remaining_time_increased(self):
        tracer_mock = self.mocker.patch(self.tracer)

        self.mocker.order()

        tracer_mock.get_remaining_time()
        self.mocker.result(1)
        tracer_mock.set_statement_timeout(self.raw_cursor, 1)
        tracer_mock.get_remaining_time()
        self.mocker.result(2)
        tracer_mock.set_statement_timeout(self.raw_cursor, 2)
        self.mocker.replay()

        self.execute()
        self.execute()

    def test_set_again_on_granularity(self):
        tracer_mock = self.mocker.patch(self.tracer)

        self.mocker.order()

        tracer_mock.get_remaining_time()
        self.mocker.result(self.tracer.granularity * 2)
        tracer_mock.set_statement_timeout(self.raw_cursor,
                                          self.tracer.granularity * 2)
        tracer_mock.get_remaining_time()
        self.mocker.result(self.tracer.granularity)
        tracer_mock.set_statement_timeout(self.raw_cursor,
                                          self.tracer.granularity)
        self.mocker.replay()

        self.execute()
        self.execute()

    def test_set_again_after_granularity(self):
        tracer_mock = self.mocker.patch(self.tracer)

        self.mocker.order()

        tracer_mock.get_remaining_time()
        self.mocker.result(self.tracer.granularity * 2)
        tracer_mock.set_statement_timeout(self.raw_cursor,
                                          self.tracer.granularity * 2)
        tracer_mock.get_remaining_time()
        self.mocker.result(self.tracer.granularity - 1)
        tracer_mock.set_statement_timeout(self.raw_cursor,
                                          self.tracer.granularity - 1)
        self.mocker.replay()

        self.execute()
        self.execute()
