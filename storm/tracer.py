from datetime import datetime
import re
import sys
import threading

# Circular import: imported at the end of the module.
# from storm.database import convert_param_marks
from storm.compat import iter_range, ustr
from storm.exceptions import TimeoutError
from storm.expr import Variable


class DebugTracer(object):

    def __init__(self, stream=None):
        if stream is None:
            stream = sys.stderr
        self._stream = stream

    def connection_raw_execute(self, connection, raw_cursor, statement,
                               params):
        time = datetime.now().isoformat()[11:]
        raw_params = []
        for param in params:
            if isinstance(param, Variable):
                raw_params.append(param.get())
            else:
                raw_params.append(param)
        raw_params = tuple(raw_params)
        self._stream.write(
            "[%s] EXECUTE: %r, %r\n" % (time, statement, raw_params))
        self._stream.flush()

    def connection_raw_execute_error(self, connection, raw_cursor,
                                     statement, params, error):
        time = datetime.now().isoformat()[11:]
        self._stream.write("[%s] ERROR: %s\n" % (time, error))
        self._stream.flush()

    def connection_raw_execute_success(self, connection, raw_cursor,
                                       statement, params):
        time = datetime.now().isoformat()[11:]
        self._stream.write("[%s] DONE\n" % time)
        self._stream.flush()

    def connection_commit(self, connection, xid=None):
        time = datetime.now().isoformat()[11:]
        self._stream.write("[%s] COMMIT xid=%s\n" % (time, xid))
        self._stream.flush()

    def connection_rollback(self, connection, xid=None):
        time = datetime.now().isoformat()[11:]
        self._stream.write("[%s] ROLLBACK xid=%s\n" % (time, xid))
        self._stream.flush()


class BaseStatementTracer(object):
    """Storm tracer base class that does query interpolation."""

    def connection_raw_execute(self, connection, raw_cursor,
                               statement, params):
        statement_to_log = statement
        if params:
            # There are some bind parameters so we want to insert them into
            # the sql statement so we can log the statement.
            query_params = list(connection.to_database(params))
            if connection.param_mark == '%s':
                # Double the %'s in the string so that python string formatting
                # can restore them to the correct number. Note that %s needs to
                # be preserved as that is where we are substituting values in.
                quoted_statement = re.sub(
                    "%%%", "%%%%", re.sub("%([^s])", r"%%\1", statement))
            else:
                # Double all the %'s in the statement so that python string
                # formatting can restore them to the correct number. Any %s in
                # the string should be preserved because the param_mark is not
                # %s.
                quoted_statement = re.sub("%", "%%", statement)
                quoted_statement = convert_param_marks(
                    quoted_statement, connection.param_mark, "%s")
            # We need to massage the query parameters a little to deal with
            # string parameters which represent encoded binary data.
            render_params = []
            for param in query_params:
                if isinstance(param, ustr):
                    render_params.append(repr(param).lstrip(u"u"))
                else:
                    render_params.append(repr(param).lstrip(u"b"))
            try:
                statement_to_log = quoted_statement % tuple(render_params)
            except TypeError:
                statement_to_log = \
                    "Unformattable query: %r with params %r." % (
                    statement, query_params)
        self._expanded_raw_execute(connection, raw_cursor, statement_to_log)

    def _expanded_raw_execute(self, connection, raw_cursor, statement):
        """Called by connection_raw_execute after parameter substitution."""
        raise NotImplementedError(self._expanded_raw_execute)


_tracers = []


def trace(name, *args, **kwargs):
    for tracer in _tracers:
        attr = getattr(tracer, name, None)
        if attr:
            attr(*args, **kwargs)


def install_tracer(tracer):
    _tracers.append(tracer)


def get_tracers():
    return _tracers[:]


def remove_all_tracers():
    del _tracers[:]


def remove_tracer(tracer):
    try:
        _tracers.remove(tracer)
    except ValueError:
        pass  # The tracer is not installed, succeed gracefully


def remove_tracer_type(tracer_type):
    _tracers[:] = [
        tracer for tracer in _tracers
        if type(tracer) is not tracer_type
    ]


def debug(flag, stream=None):
    remove_tracer_type(DebugTracer)
    if flag:
        install_tracer(DebugTracer(stream=stream))


# Deal with circular import.
from storm.database import convert_param_marks
