#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
import sys

from datetime import datetime, date, time

from storm.databases import dummy

try:
    from pysqlite2 import dbapi2 as sqlite
except ImportError:
    sqlite = dummy

from storm.variables import Variable
from storm.database import *
from storm.exceptions import install_exceptions, DatabaseModuleError
from storm.expr import (
    compile, Select, Undef, SQLRaw, SetExpr, Union, compile_select,
    compile_set_expr)


install_exceptions(sqlite)


compile = compile.fork()

@compile.when(Select)
def compile_select_sqlite(compile, state, select):
    if select.offset is not Undef and select.limit is Undef:
        select.limit = sys.maxint
        # SQLite breaks with (SELECT ...) UNION (SELECT ...), so we
        # do SELECT * FROM (SELECT ...) instead.  This is important
        # because SELECT ... UNION SELECT ... ORDER BY binds the ORDER BY
        # to the UNION instead of SELECT.
    state.push("inside_set_expr", False) # XXX UNTESTED!
    statement = compile_select(compile, state, select)
    state.pop()
    if getattr(state, "inside_set_expr", False):
        return "SELECT * FROM (%s)" % statement
    return statement

@compile.when(SetExpr)
def compile_set_expr_sqlite(compile, state, expr):
    state.push("inside_set_expr", True)
    statement = compile_set_expr(compile, state, expr)
    state.pop()
    return statement

compile.set_precedence(10, Union)


class SQLiteResult(Result):

    def get_insert_identity(self, primary_key, primary_variables):
        return SQLRaw("(OID=%d)" % self._raw_cursor.lastrowid)

    @staticmethod
    def _from_database(row):
        for value in row:
            if isinstance(value, buffer):
                yield str(value)
            else:
                yield value


class SQLiteConnection(Connection):

    _result_factory = SQLiteResult
    _compile = compile

    @staticmethod
    def _to_database(params):
        for param in params:
            if isinstance(param, Variable):
                param = param.get(to_db=True)
            if isinstance(param, (datetime, date, time)):
                yield str(param)
            elif isinstance(param, str):
                yield buffer(param)
            else:
                yield param


class SQLite(Database):

    _connection_factory = SQLiteConnection

    def __init__(self, filename=None):
        if sqlite is dummy:
            raise DatabaseModuleError("'pysqlite2' module not found")
        self._filename = filename or ":memory:"

    def connect(self):
        raw_connection = sqlite.connect(self._filename)
        return self._connection_factory(self, raw_connection)


def create_from_uri(uri):
    return SQLite(uri.database)
