import datetime
import os
import pytest
import sys

from freezegun import freeze_time
from storm.compat import StringIO, ustr
from storm.tracer import (
    _tracers,
    BaseStatementTracer,
    debug,
    DebugTracer,
    get_tracers,
    install_tracer,
    remove_all_tracers,
    remove_tracer,
    remove_tracer_type,
    trace,
)
from storm.database import Connection, create_database
from storm.expr import Variable


class StubConnection(Connection):
    def __init__(self, param_mark=None):
        self._database = None
        self._event = None
        self._raw_connection = None
        self.name = 'Foo'
        if param_mark is not None:
            self.param_mark = param_mark


class LoggingBaseStatementTracer(BaseStatementTracer):
    def __init__(self):
        self.calls = []
        self.statements = []

    def _expanded_raw_execute(self, connection, raw_cursor, statement):
        self.calls.append((connection, raw_cursor, statement))
        self.statements.append(statement)


class MockVariable(Variable):
    def __init__(self, value):
        self._value = value

    def get(self, to_db=False):
        return self._value


@pytest.fixture
def cleanup_tracers():
    yield
    # Cleanup tracers
    del _tracers[:]


@pytest.fixture
def stream():
    return StringIO()


@pytest.fixture
def debug_tracer(stream):
    return DebugTracer(stream)


@pytest.fixture
def statement_tracer():
    return LoggingBaseStatementTracer()


@pytest.fixture
def variable():
    return MockVariable("PARAM")


freeze_time_simple = freeze_time(datetime.datetime(1, 2, 3, 4, 5, 6, 7))


def test_install_tracer(cleanup_tracers):
    c = object()
    d = object()
    install_tracer(c)
    install_tracer(d)
    assert get_tracers() == [c, d]


def test_remove_all_tracers(cleanup_tracers):
    install_tracer(object())
    remove_all_tracers()
    assert get_tracers() == []


def test_remove_tracer(cleanup_tracers):
    """The C{remote_tracer} function removes a specific tracer."""
    tracer1 = object()
    tracer2 = object()
    install_tracer(tracer1)
    install_tracer(tracer2)
    remove_tracer(tracer1)
    assert get_tracers() == [tracer2]


def test_remove_tracer_with_not_installed_tracer():
    """C{remote_tracer} exits gracefully if the tracer is not installed."""
    tracer = object()
    remove_tracer(tracer)
    assert get_tracers() == []


def test_remove_tracer_type(cleanup_tracers):
    class C(object):
        pass

    class D(C):
        pass

    c = C()
    d1 = D()
    d2 = D()
    install_tracer(d1)
    install_tracer(c)
    install_tracer(d2)
    remove_tracer_type(C)
    assert get_tracers() == [d1, d2]
    remove_tracer_type(D)
    assert get_tracers() == []


def test_install_debug(cleanup_tracers):
    debug(True)
    debug(True)
    assert [type(x) for x in get_tracers()] == [DebugTracer]


def test_wb_install_debug_with_custom_stream(cleanup_tracers):
    marker = object()
    debug(True, marker)
    [tracer] = get_tracers()
    assert tracer._stream == marker


def test_remove_debug(cleanup_tracers):
    debug(True)
    debug(True)
    debug(False)
    assert get_tracers() == []


def test_trace(cleanup_tracers):
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
    assert stash == ["m1", (1, 2), {"c": 3}, "m2", (), {}]


def test_debug_tracer_uses_stderr_by_default():
    tracer = DebugTracer()
    assert tracer._stream == sys.stderr


def test_debug_tracer_uses_first_arg_as_stream():
    marker = object()
    tracer = DebugTracer(marker)
    assert tracer._stream == marker


@freeze_time_simple
def test_debug_tracer_connection_raw_execute(debug_tracer, stream, variable):
    debug_tracer.connection_raw_execute(
        connection="CONNECTION",
        raw_cursor="RAW_CURSOR",
        statement="STATEMENT",
        params=[variable],
    )
    expected = "[04:05:06.000007] EXECUTE: 'STATEMENT', ('PARAM',)\n"
    assert expected == stream.getvalue()


@freeze_time_simple
def test_debug_tracer_connection_raw_execute_with_non_variable(debug_tracer, stream, variable):
    debug_tracer.connection_raw_execute(
        connection="CONNECTION",
        raw_cursor="RAW_CURSOR",
        statement="STATEMENT",
        params=[variable, 1],
    )
    expected = "[04:05:06.000007] EXECUTE: 'STATEMENT', ('PARAM', 1)\n"
    assert expected == stream.getvalue()


@freeze_time_simple
def test_debug_tracer_connection_raw_execute_error(debug_tracer, stream):
    debug_tracer.connection_raw_execute_error(
        connection="CONNECTION",
        raw_cursor="RAW_CURSOR",
        statement="STATEMENT",
        params="PARAMS",
        error="ERROR",
    )
    assert "[04:05:06.000007] ERROR: ERROR\n" == stream.getvalue()


@freeze_time_simple
def test_debug_tracer_connection_raw_execute_success(debug_tracer, stream):
    debug_tracer.connection_raw_execute_success(
        connection="CONNECTION",
        raw_cursor="RAW_CURSOR",
        statement="STATEMENT",
        params="PARAMS",
    )
    assert "[04:05:06.000007] DONE\n" == stream.getvalue()


@freeze_time_simple
def test_debug_tracer_connection_commit(debug_tracer, stream):
    debug_tracer.connection_commit(connection="CONNECTION")
    assert "[04:05:06.000007] COMMIT xid=None\n" == stream.getvalue()


@freeze_time_simple
def test_debug_tracer_connection_rollback(debug_tracer, stream):
    debug_tracer.connection_rollback(connection="CONNECTION")
    assert "[04:05:06.000007] ROLLBACK xid=None\n" == stream.getvalue()


def test_statement_tracer_no_params(statement_tracer):
    """With no parameters the statement is passed through verbatim."""
    statement_tracer.connection_raw_execute("foo", "bar", "baz ? %s", ())
    assert [("foo", "bar", "baz ? %s")] == statement_tracer.calls


def test_statement_tracer_params_substituted_pyformat(statement_tracer, variable):
    conn = StubConnection(param_mark="%s")
    statement_tracer.connection_raw_execute(
        conn, "cursor", "SELECT * FROM person where name = %s", [variable])
    assert [
        "SELECT * FROM person where name = 'PARAM'",
    ] == statement_tracer.statements


def test_statement_tracer_params_substituted_single_string(statement_tracer, variable):
    """String parameters are formatted as a single quoted string."""
    conn = StubConnection()
    statement_tracer.connection_raw_execute(
        conn, "cursor", "SELECT * FROM person where name = ?", [variable])
    assert [
        "SELECT * FROM person where name = 'PARAM'",
    ] == statement_tracer.statements


def test_statement_tracer_qmark_percent_s_literal_preserved(statement_tracer):
    """With ? parameters %s in the statement can be kept intact."""
    conn = StubConnection()
    var1 = MockVariable(1)
    statement_tracer.connection_raw_execute(
        conn,
        "cursor",
        "SELECT * FROM person where id > ? AND name LIKE '%s'",
        [var1],
    )
    assert [
        "SELECT * FROM person where id > 1 AND name LIKE '%s'",
    ] == statement_tracer.statements

def test_statement_tracer_int_variable_as_int(statement_tracer):
    """Int parameters are formatted as an int literal."""
    conn = StubConnection()
    var1 = MockVariable(1)
    statement_tracer.connection_raw_execute(
        conn,
        "cursor",
        "SELECT * FROM person where id = ?",
        [var1],
    )
    assert ["SELECT * FROM person where id = 1"] == statement_tracer.statements


def test_statement_tracer_like_clause_preserved(statement_tracer, variable):
    """% operators in LIKE statements are preserved."""
    conn = StubConnection()
    statement_tracer.connection_raw_execute(
        conn,
        "cursor",
        "SELECT * FROM person WHERE name LIKE '%%' || ? || '-suffix%%'",
        [variable],
    )
    sql = "SELECT * FROM person WHERE name LIKE '%%' || 'PARAM' || '-suffix%%'"
    assert [sql] == statement_tracer.statements


def test_statement_tracer_unformattable_statements_are_handled(statement_tracer, variable):
    conn = StubConnection()
    statement_tracer.connection_raw_execute(conn, 'cursor', "%s %s", [variable])
    assert len(statement_tracer.statements) == 1
    assert "Unformattable query" in statement_tracer.statements[0]
