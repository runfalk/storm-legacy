#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
from datetime import datetime, date, time
from time import strptime

from storm.databases import dummy

try:
    import psycopg
except:
    psycopg = dummy

from storm.expr import And, Eq
from storm.variables import Variable, UnicodeVariable, StrVariable
from storm.database import *
from storm.exceptions import install_exceptions, DatabaseModuleError
from storm.expr import FuncExpr, compile


install_exceptions(psycopg)
compile = compile.fork()


class currval(FuncExpr):

    name = "currval"

    def __init__(self, column):
        self.column = column

@compile.when(currval)
def compile_currval(compile, state, expr):
    return "currval('%s_%s_seq')" % (compile(state, expr.column.table),
                                     compile(state, expr.column.name))


def compile_str_variable_with_E(compile, state, variable):
    """Include an E just before the placeholder of string variables.

    PostgreSQL 8.2 will issue a warning without it, and old versions
    of psycopg will use plain '' rather than E''.
    """
    state.parameters.append(variable)
    return "E?"

psycopg_needs_E = None


class PostgresResult(Result):

    def get_insert_identity(self, primary_key, primary_variables):
        equals = []
        for column, variable in zip(primary_key, primary_variables):
            if not variable.is_defined():
                variable = currval(column)
            equals.append(Eq(column, variable))
        return And(*equals)

    def set_variable(self, variable, value):
        if isinstance(variable, UnicodeVariable):
            variable.set(unicode(value, self._connection._database._encoding),
                         from_db=True)
        else:
            variable.set(value, from_db=True)


class PostgresConnection(Connection):

    _result_factory = PostgresResult
    _param_mark = "%s"
    _compile = compile

    def _to_database(self, value):
        if isinstance(value, Variable):
            value = value.get(to_db=True)
        if isinstance(value, (datetime, date, time)):
            return str(value)
        if isinstance(value, unicode):
            return value.encode(self._database._encoding)
        if isinstance(value, str):
            return psycopg.Binary(value)
        return value


class Postgres(Database):

    _connection_factory = PostgresConnection

    def __init__(self, dbname, host=None, port=None,
                 username=None, password=None, encoding=None):
        if psycopg is dummy:
            raise DatabaseModuleError("'psycopg' module not found")
        self._dsn = "dbname=%s" % dbname
        if host is not None:
            self._dsn += " host=%s" % host
        if port is not None:
            self._dsn += " port=%d" % port
        if username is not None:
            self._dsn += " user=%s" % username
        if password is not None:
            self._dsn += " password=%s" % password

        self._encoding = encoding or "UTF-8"

    def connect(self):
        global psycopg_needs_E
        raw_connection = psycopg.connect(self._dsn)
        if psycopg_needs_E is None:
            # This will conditionally change the compilation of binary
            # variables (StrVariable) to preceed the placeholder with an
            # 'E', if psycopg isn't doing it by itself.
            #
            # The "failing" code path isn't unittested because that
            # would depend on a different psycopg version.  Both branches
            # were manually tested for correctness at some point.
            cursor = raw_connection.cursor()
            try:
                cursor.execute("SELECT E%s", (psycopg.Binary(""),))
            except psycopg.ProgrammingError:
                raw_connection.rollback()
                psycopg_needs_E = False
            else:
                psycopg_needs_E = True
                compile.when(StrVariable)(compile_str_variable_with_E)
        return self._connection_factory(self, raw_connection)


def str_or_none(value):
    return value and str(value)

psycopg.register_type(psycopg.new_type(psycopg.DATETIME.values,
                                       "DT", str_or_none))


def create_from_uri(uri):
    return Postgres(uri.database, uri.host, uri.port,
                    uri.username, uri.password, uri.options.get("encoding"))


# FIXME Make Postgres constructor use that one.
def make_dsn(uri):
    """Convert a URI object to a PostgreSQL DSN string."""
    dsn = "dbname=%s" % uri.database
    if uri.host is not None:
        dsn += " host=%s" % uri.host
    if uri.port is not None:
        dsn += " port=%d" % uri.port
    if uri.username is not None:
        dsn += " user=%s" % uri.username
    if uri.password is not None:
        dsn += " password=%s" % uri.password
    return dsn
