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
from storm.expr import compile, Select, compile_select, Undef


install_exceptions(sqlite)


compile = compile.fork()

@compile.when(Select)
def compile_select_sqlite(compile, state, select):
    if select.offset is not Undef and select.limit is Undef:
        select.limit = sys.maxint
    return compile_select(compile, state, select)


class SQLiteResult(Result):

    def get_insert_identity(self, primary_key, primary_variables):
        return "(OID=%d)" % self._raw_cursor.lastrowid

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
