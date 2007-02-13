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
from storm.variables import Variable, UnicodeVariable, ListVariable
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

@compile.when(ListVariable)
def compile_list_variable(compile, state, list_variable):
    elements = []
    variables = list_variable.get(to_db=True)
    if variables is None:
        return "NULL"
    for variable in variables:
        elements.append(compile(state, variable))
    return "ARRAY[%s]" % ",".join(elements)


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
            value = unicode(value, self._connection._database._encoding)
        elif isinstance(variable, ListVariable):
            if value == "{}":
                value = []
            else:
                value = parse_array(value)
        variable.set(value, from_db=True)


class PostgresConnection(Connection):

    _result_factory = PostgresResult
    _param_mark = "%s"
    _compile = compile

    def _to_database(self, params):
        for param in params:
            if isinstance(param, Variable):
                param = param.get(to_db=True)
            if isinstance(param, (datetime, date, time)):
                yield str(param)
            elif isinstance(param, unicode):
                yield param.encode(self._database._encoding)
            elif isinstance(param, str):
                yield psycopg.Binary(param)
            else:
                yield param


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
        raw_connection = psycopg.connect(self._dsn)
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


def parse_array(array):
    """Parse a PostgreSQL-formatted array literal.

    E.g. r'{{meeting,lunch},{ training , "presentation" },"{}","\"", NULL}'
    makes [["meeting", "lunch"], ["training", "presentation"], "{}", '"', None]
    """

    if array[0] != "{" or array[-1] != "}":
        raise ValueError("Invalid array")
    stack = []
    current = []
    token = ""
    nesting = 0
    quoting = False
    quoted = False
    chars = iter(array)
    for c in chars:
        if quoting:
            if c == "\\":
                token += chars.next()
            elif c == '"':
                quoting = False
            else:
                token += c
        elif c == " ":
            pass
        elif c in ",}":
            if token:
                if quoted:
                    quoted = False
                elif token == "NULL":
                    token = None
                current.append(token)
                token = ""
            if c == "}":
                current = stack.pop()
        elif token:
            token += c
        elif c == '"':
            quoting = True
            quoted = True
        elif c == "{":
            lst = []
            current.append(lst)
            stack.append(current)
            current = lst
        else:
            token = c
    return current[0]
