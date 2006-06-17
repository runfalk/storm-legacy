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
from storm.exceptions import install_exceptions, UnsupportedDatabaseError
from storm.expr import compile, Select, compile_select, Undef


install_exceptions(sqlite)


compile = compile.copy()

@compile.when(Select)
def compile_select_sqlite(compile, state, select):
    if select.offset is not Undef and select.limit is Undef:
        select.limit = sys.maxint
    return compile_select(compile, state, select)


class SQLiteResult(Result):

    def get_insert_identity(self, primary_key, primary_variables):
        return "(OID=%d)" % self._raw_cursor.lastrowid

    def _from_database(self, value):
        if isinstance(value, buffer):
            return str(value)
        return value


class SQLiteConnection(Connection):

    _result_factory = SQLiteResult
    _compile = compile

    def _to_database(self, value):
        if isinstance(value, Variable):
            value = value.get(to_db=True)
        if isinstance(value, (datetime, date, time)):
            return str(value)
        elif isinstance(value, str):
            return buffer(value)
        return value


class SQLite(Database):

    _connection_factory = SQLiteConnection

    def __init__(self, filename=None):
        if sqlite is dummy:
            raise UnsupportedDatabaseError("'pysqlite2' module not found")
        self._filename = filename or ":memory:"

    def connect(self):
        raw_connection = sqlite.connect(self._filename)
        return self._connection_factory(self, raw_connection)


def create_from_uri(uri):
    return SQLite(uri.database)
