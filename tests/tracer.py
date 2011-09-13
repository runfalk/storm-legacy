import datetime
import sys
from unittest import TestCase

try:
    # Optional dependency, if missing TimelineTracer tests are skipped.
    import timeline
except ImportError:
    timeline = None

from storm.tracer import (trace, install_tracer, get_tracers,
                          remove_tracer_type, remove_all_tracers, debug,
                          BaseStatementTracer, DebugTracer, TimeoutTracer,
                          TimelineTracer, TimeoutError, _tracers)
from storm.database import Connection
from storm.expr import Variable

from tests.helper import TestHelper


class TracerTest(TestHelper):

    def tearDown(self):
        super(TracerTest, self).tearDown()
        del _tracers[:]

    def test_install_tracer(self):
        c = object()
        d = object()
        install_tracer(c)
        install_tracer(d)
        self.assertEquals(get_tracers(), [c, d])

    def test_remove_all_tracers(self):
        install_tracer(object())
        remove_all_tracers()
        self.assertEquals(get_tracers(), [])

    def test_remove_tracer_type(self):
        class C(object): pass
        class D(C): pass
        c = C()
        d1 = D()
        d2 = D()
        install_tracer(d1)
        install_tracer(c)
        install_tracer(d2)
        remove_tracer_type(C)
        self.assertEquals(get_tracers(), [d1, d2])
        remove_tracer_type(D)
        self.assertEquals(get_tracers(), [])

    def test_install_debug(self):
        debug(True)
        debug(True)
        self.assertEquals([type(x) for x in get_tracers()], [DebugTracer])

    def test_wb_install_debug_with_custom_stream(self):
        marker = object()
        debug(True, marker)
        [tracer] = get_tracers()
        self.assertEquals(tracer._stream, marker)

    def test_remove_debug(self):
        debug(True)
        debug(True)
        debug(False)
        self.assertEquals(get_tracers(), [])

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



class MockVariable(Variable):

    def __init__(self, value):
        self._value = value

    def get(self, to_db=False):
        return self._value


class DebugTracerTest(TestHelper):

    def setUp(self):
        super(DebugTracerTest, self).setUp()
        self.stream = self.mocker.mock(file)
        self.tracer = DebugTracer(self.stream)

        datetime_mock = self.mocker.replace("datetime.datetime")
        datetime_mock.now()
        self.mocker.result(datetime.datetime(1,2,3,4,5,6,7))
        self.mocker.count(0, 1)

        self.variable = MockVariable("PARAM")

    def tearDown(self):
        del _tracers[:]
        super(DebugTracerTest, self).tearDown()

    def test_wb_debug_tracer_uses_stderr_by_default(self):
        self.mocker.replay()

        tracer = DebugTracer()
        self.assertEqual(tracer._stream, sys.stderr)

    def test_wb_debug_tracer_uses_first_arg_as_stream(self):
        self.mocker.replay()

        marker = object()
        tracer = DebugTracer(marker)
        self.assertEqual(tracer._stream, marker)

    def test_connection_raw_execute(self):
        self.stream.write(
            "[04:05:06.000007] EXECUTE: 'STATEMENT', ('PARAM',)\n")
        self.stream.flush()
        self.mocker.replay()

        connection = "CONNECTION"
        raw_cursor = "RAW_CURSOR"
        statement = "STATEMENT"
        params = [self.variable]

        self.tracer.connection_raw_execute(connection, raw_cursor,
                                           statement, params)

    def test_connection_raw_execute_with_non_variable(self):
        self.stream.write(
            "[04:05:06.000007] EXECUTE: 'STATEMENT', ('PARAM', 1)\n")
        self.stream.flush()
        self.mocker.replay()

        connection = "CONNECTION"
        raw_cursor = "RAW_CURSOR"
        statement = "STATEMENT"
        params = [self.variable, 1]

        self.tracer.connection_raw_execute(connection, raw_cursor,
                                           statement, params)

    def test_connection_raw_execute_error(self):
        self.stream.write("[04:05:06.000007] ERROR: ERROR\n")
        self.stream.flush()
        self.mocker.replay()

        connection = "CONNECTION"
        raw_cursor = "RAW_CURSOR"
        statement = "STATEMENT"
        params = "PARAMS"
        error = "ERROR"

        self.tracer.connection_raw_execute_error(connection, raw_cursor,
                                                 statement, params, error)

    def test_connection_raw_execute_success(self):
        self.stream.write("[04:05:06.000007] DONE\n")
        self.stream.flush()
        self.mocker.replay()

        connection = "CONNECTION"
        raw_cursor = "RAW_CURSOR"
        statement = "STATEMENT"
        params = "PARAMS"

        self.tracer.connection_raw_execute_success(connection, raw_cursor,
                                                   statement, params)


class TimeoutTracerTestBase(TestHelper):

    tracer_class = TimeoutTracer

    def setUp(self):
        super(TimeoutTracerTestBase, self).setUp()
        self.tracer = self.tracer_class()
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
        """
        L{TimeoutTracer.connection_raw_execute_error},
        L{TimeoutTracer.set_statement_timeout} and
        L{TimeoutTracer.get_remaining_time} must all be implemented by
        backend-specific subclasses.
        """
        self.assertRaises(NotImplementedError,
                          self.tracer.connection_raw_execute_error,
                          None, None, None, None, None)
        self.assertRaises(NotImplementedError,
                          self.tracer.set_statement_timeout, None, None)
        self.assertRaises(NotImplementedError,
                          self.tracer.get_remaining_time)

    def test_raise_timeout_error_when_no_remaining_time(self):
        """
        A L{TimeoutError} is raised if there isn't any time left when a
        statement is executed.
        """
        tracer_mock = self.mocker.patch(self.tracer)
        tracer_mock.get_remaining_time()
        self.mocker.result(0)
        self.mocker.replay()

        try:
            self.execute()
        except TimeoutError, e:
            self.assertEqual("0 seconds remaining in time budget", e.message)
            self.assertEqual(self.statement, e.statement)
            self.assertEqual(self.params, e.params)
        else:
            self.fail("TimeoutError not raised")

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


class BaseStatementTracerTest(TestCase):

    class LoggingBaseStatementTracer(BaseStatementTracer):
        def _expanded_raw_execute(self, connection, raw_cursor, statement):
            self.__dict__.setdefault('calls', []).append(
                (connection, raw_cursor, statement))

    class StubConnection(Connection):

        def __init__(self):
            self._database = None
            self._event = None
            self._raw_connection = None

    def test_no_params(self):
        """With no parameters the statement is passed through verbatim."""
        tracer = self.LoggingBaseStatementTracer()
        tracer.connection_raw_execute('foo', 'bar', 'baz ? %s', ())
        self.assertEqual([('foo', 'bar', 'baz ? %s')], tracer.calls)

    def test_params_substituted_pyformat(self):
        tracer = self.LoggingBaseStatementTracer()
        conn = self.StubConnection()
        conn.param_mark = '%s'
        var1 = MockVariable(u'VAR1')
        tracer.connection_raw_execute(
            conn, 'cursor', 'SELECT * FROM person where name = %s', [var1])
        self.assertEqual(
            [(conn, 'cursor', "SELECT * FROM person where name = 'VAR1'")],
            tracer.calls)

    def test_params_substituted_single_string(self):
        """String parameters are formatted as a single quoted string."""
        tracer = self.LoggingBaseStatementTracer()
        conn = self.StubConnection()
        var1 = MockVariable(u'VAR1')
        tracer.connection_raw_execute(
            conn, 'cursor', 'SELECT * FROM person where name = ?', [var1])
        self.assertEqual(
            [(conn, 'cursor', "SELECT * FROM person where name = 'VAR1'")],
            tracer.calls)

    def test_qmark_percent_s_literal_preserved(self):
        """With ? parameters %s in the statement can be kept intact."""
        tracer = self.LoggingBaseStatementTracer()
        conn = self.StubConnection()
        var1 = MockVariable(1)
        tracer.connection_raw_execute(
            conn, 'cursor',
            "SELECT * FROM person where id > ? AND name LIKE '%s'", [var1])
        self.assertEqual(
            [(conn, 'cursor',
              "SELECT * FROM person where id > 1 AND name LIKE '%s'")],
            tracer.calls)

    def test_int_variable_as_int(self):
        """Int parameters are formatted as an int literal."""
        tracer = self.LoggingBaseStatementTracer()
        conn = self.StubConnection()
        var1 = MockVariable(1)
        tracer.connection_raw_execute(
            conn, 'cursor', "SELECT * FROM person where id = ?", [var1])
        self.assertEqual(
            [(conn, 'cursor', "SELECT * FROM person where id = 1")],
            tracer.calls)

    def test_like_clause_preserved(self):
        """% operators in LIKE statements are preserved."""
        tracer = self.LoggingBaseStatementTracer()
        conn = self.StubConnection()
        var1 = MockVariable(u'substring')
        tracer.connection_raw_execute(
            conn, 'cursor',
            "SELECT * FROM person WHERE name LIKE '%%' || ? || '-suffix%%'",
            [var1])
        self.assertEqual(
            [(conn, 'cursor', "SELECT * FROM person WHERE name "
                              "LIKE '%%' || 'substring' || '-suffix%%'")],
            tracer.calls)

    def test_unformattable_statements_are_handled(self):
        tracer = self.LoggingBaseStatementTracer()
        conn = self.StubConnection()
        var1 = MockVariable(u'substring')
        tracer.connection_raw_execute(
            conn, 'cursor', "%s %s",
            [var1])
        self.assertEqual(
            [(conn, 'cursor',
              "Unformattable query: '%s %s' with params [u'substring'].")],
            tracer.calls)


class TimelineTracerTest(TestHelper):

    def is_supported(self):
        return timeline is not None

    def factory(self):
        self.timeline = timeline.Timeline()
        return self.timeline

    def test_separate_tracers_own_state(self):
        """"Check that multiple TimelineTracer's could be used at once."""
        tracer1 = TimelineTracer(self.factory)
        tracer2 = TimelineTracer(self.factory)
        tracer1.threadinfo.action = 'foo'
        self.assertEqual(None, getattr(tracer2.threadinfo, 'action', None))

    def test_error_finishes_action(self):
        tracer = TimelineTracer(self.factory)
        action = timeline.Timeline().start('foo', 'bar')
        tracer.threadinfo.action = action
        tracer.connection_raw_execute_error(
            'conn', 'cursor', 'statement', 'params', 'error')
        self.assertNotEqual(None, action.duration)

    def test_success_finishes_action(self):
        tracer = TimelineTracer(self.factory)
        action = timeline.Timeline().start('foo', 'bar')
        tracer.threadinfo.action = action
        tracer.connection_raw_execute_success(
            'conn', 'cursor', 'statement', 'params')
        self.assertNotEqual(None, action.duration)

    def test_finds_timeline_from_factory(self):
        factory_result = timeline.Timeline()
        factory = lambda:factory_result
        tracer = TimelineTracer(lambda:factory_result)
        tracer._expanded_raw_execute('conn', 'cursor', 'statement')
        self.assertEqual(1, len(factory_result.actions))

    def test_action_details_are_statement(self):
        """The detail in the timeline action is the formatted SQL statement."""
        tracer = TimelineTracer(self.factory)
        tracer._expanded_raw_execute('conn', 'cursor', 'statement')
        self.assertEqual('statement', self.timeline.actions[-1].detail)

    def test_category_from_prefix_and_connection_name(self):
        class StubConnection(Connection):

            def __init__(self):
                self._database = None
                self._event = None
                self._raw_connection = None
                self.name = 'Foo'
        tracer = TimelineTracer(self.factory, prefix='bar-')
        tracer._expanded_raw_execute(StubConnection(), 'cursor', 'statement')
        self.assertEqual('bar-Foo', self.timeline.actions[-1].category)

    def test_unnamed_connection(self):
        """A connection with no name has <unknown> put in as a placeholder."""
        tracer = TimelineTracer(self.factory, prefix='bar-')
        tracer._expanded_raw_execute('conn', 'cursor', 'statement')
        self.assertEqual('bar-<unknown>', self.timeline.actions[-1].category)

    def test_default_prefix(self):
        """By default the prefix "SQL-" is added to the action's category."""
        tracer = TimelineTracer(self.factory)
        tracer._expanded_raw_execute('conn', 'cursor', 'statement')
        self.assertEqual('SQL-<unknown>', self.timeline.actions[-1].category)
