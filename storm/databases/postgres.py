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
    import psycopg2
    import psycopg2.extensions
except:
    psycopg2 = dummy

from storm.expr import (
    Undef, SetExpr, Select, Alias, And, Eq, FuncExpr, SQLRaw, COLUMN_NAME,
    compile, compile_select, compile_set_expr)
from storm.variables import (
    Variable, UnicodeVariable, StrVariable, ListVariable)
from storm.database import *
from storm.exceptions import install_exceptions, DatabaseModuleError


install_exceptions(psycopg2)
compile = compile.fork()


class currval(FuncExpr):

    name = "currval"

    def __init__(self, column):
        self.column = column

@compile.when(currval)
def compile_currval(compile, state, expr):
    return "currval('%s_%s_seq')" % (compile(state, expr.column.table),
                                     expr.column.name)

@compile.when(ListVariable)
def compile_list_variable(compile, state, list_variable):
    elements = []
    variables = list_variable.get(to_db=True)
    if variables is None:
        return "NULL"
    if not variables:
        return "'{}'"
    for variable in variables:
        elements.append(compile(state, variable))
    return "ARRAY[%s]" % ",".join(elements)

@compile.when(SetExpr)
def compile_set_expr_postgres(compile, state, expr):
    if expr.order_by is not Undef:
        # The following statement breaks in postgres:
        #     SELECT 1 AS id UNION SELECT 1 ORDER BY id+1
        # With the error:
        #     ORDER BY on a UNION/INTERSECT/EXCEPT result must
        #     be on one of the result columns
        # So we transform it into something close to:
        #     SELECT * FROM (SELECT 1 AS id UNION SELECT 1) AS a ORDER BY id+1

        # Build new set expression without arguments (order_by, etc).
        new_expr = expr.__class__()
        new_expr.exprs = expr.exprs
        new_expr.all = expr.all

        # Make sure that state.aliases isn't None, since we want them to
        # compile our order_by statement below.
        no_aliases = state.aliases is None
        if no_aliases:
            state.push("aliases", {})

        # Build set expression, collecting aliases.
        set_stmt = SQLRaw("(%s)" % compile_set_expr(compile, state, new_expr))

        # Build order_by statement, using aliases.
        state.push("context", COLUMN_NAME)
        order_by_stmt = SQLRaw(compile(state, expr.order_by))
        state.pop()

        # Discard aliases, if they were not being collected previously.
        if no_aliases:
            state.pop()

        # Build wrapping select statement.
        select = Select(SQLRaw("*"), tables=Alias(set_stmt), limit=expr.limit,
                        offset=expr.offset, order_by=order_by_stmt)
        return compile_select(compile, state, select)
    else:
        return compile_set_expr(compile, state, expr)

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


class PostgresConnection(Connection):

    _result_factory = PostgresResult
    _param_mark = "%s"
    _compile = compile

    def _raw_execute(self, statement, params):
        if type(statement) is unicode:
            # psycopg breaks with unicode statements.
            statement = statement.encode("UTF-8")
        return Connection._raw_execute(self, statement, params)

    def _to_database(self, params):
        for param in params:
            if isinstance(param, Variable):
                param = param.get(to_db=True)
            if isinstance(param, (datetime, date, time)):
                yield str(param)
            elif isinstance(param, unicode):
                yield param.encode("UTF-8")
            elif isinstance(param, str):
                yield psycopg2.Binary(param)
            else:
                yield param


class Postgres(Database):

    _connection_factory = PostgresConnection

    def __init__(self, dbname, host=None, port=None,
                 username=None, password=None):
        if psycopg2 is dummy:
            raise DatabaseModuleError("'psycopg2' module not found")
        self._dsn = "dbname=%s" % dbname
        if host is not None:
            self._dsn += " host=%s" % host
        if port is not None:
            self._dsn += " port=%d" % port
        if username is not None:
            self._dsn += " user=%s" % username
        if password is not None:
            self._dsn += " password=%s" % password

    def connect(self):
        global psycopg_needs_E
        raw_connection = psycopg2.connect(self._dsn)
        raw_connection.set_client_encoding("UTF8")
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
                cursor.execute("SELECT E%s", (psycopg2.Binary(""),))
            except psycopg2.ProgrammingError:
                raw_connection.rollback()
                psycopg_needs_E = False
            else:
                psycopg_needs_E = True
                compile.when(StrVariable)(compile_str_variable_with_E)
        return self._connection_factory(self, raw_connection)


if psycopg2 is not dummy:
    psycopg2.extensions.register_type(psycopg2.extensions.DATE)
    psycopg2.extensions.register_type(psycopg2.extensions.INTERVAL)
    psycopg2.extensions.register_type(psycopg2.extensions.TIME)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    psycopg2.extensions.register_type(psycopg2._psycopg.UNICODEARRAY)


def create_from_uri(uri):
    return Postgres(uri.database, uri.host, uri.port,
                    uri.username, uri.password)


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
