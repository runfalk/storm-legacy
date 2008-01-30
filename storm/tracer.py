import sys


class DebugTracer(object):

    def connection_raw_execute(self, connection, raw_cursor, statement, params):
        sys.stderr.write("%r, %r\n" % (statement, params))
        sys.stderr.flush()


_tracers = []

def trace(name, *args, **kwargs):
    for tracer in _tracers:
        attr = getattr(tracer, name, None)
        if attr:
            attr(*args, **kwargs)

def install_tracer(tracer):
    _tracers.append(tracer)

def remove_all_tracers():
    del _tracers[:]

def remove_tracer_type(tracer_type):
    for i in range(len(_tracers)-1, -1, -1):
        if type(_tracers[i]) is tracer_type:
            del _tracers[i]

def debug(flag):
    remove_tracer_type(DebugTracer)
    if flag:
        install_tracer(DebugTracer())
