#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
from datetime import time
from array import array
import sys

from storm.databases import dummy

try:
    import MySQLdb
    import MySQLdb.converters
except ImportError:
    MySQLdb = dummy

from storm.expr import compile, Select, compile_select, Undef, And, Eq, SQLRaw
from storm.database import *
from storm.exceptions import install_exceptions, DatabaseModuleError


install_exceptions(MySQLdb)


compile = compile.fork()

@compile.when(Select)
def compile_select_mysql(compile, state, select):
    if select.offset is not Undef and select.limit is Undef:
        select.limit = sys.maxint
    return compile_select(compile, state, select)


class MySQLResult(Result):

    def get_insert_identity(self, primary_key, primary_variables):
        equals = []
        for column, variable in zip(primary_key, primary_variables):
            if not variable.is_defined():
                variable = SQLRaw(self._raw_cursor.lastrowid)
            equals.append(Eq(column, variable))
        return And(*equals)

    @staticmethod
    def _from_database(row):
        for value in row:
            if isinstance(value, array):
                yield value.tostring()
            else:
                yield value


class MySQLConnection(Connection):

    _result_factory = MySQLResult
    _param_mark = "%s"
    _compile = compile


class MySQL(Database):

    _connection_factory = MySQLConnection
    _converters = None

    def __init__(self, uri):
        if MySQLdb is dummy:
            raise DatabaseModuleError("'MySQLdb' module not found")
        self._connect_kwargs = {}
        if uri.database is not None:
            self._connect_kwargs["db"] = uri.database
        if uri.host is not None:
            self._connect_kwargs["host"] = uri.host
        if uri.port is not None:
            self._connect_kwargs["port"] = uri.port
        if uri.username is not None:
            self._connect_kwargs["user"] = uri.username
        if uri.password is not None:
            self._connect_kwargs["passwd"] = uri.password
        for option in ["unix_socket"]:
            if option in uri.options:
                self._connect_kwargs[option] = uri.options.get(option)

        if self._converters is None:
            # MySQLdb returns a timedelta by default on TIME fields.
            converters = MySQLdb.converters.conversions.copy()
            converters[MySQLdb.converters.FIELD_TYPE.TIME] = _convert_time
            self.__class__._converters = converters

        self._connect_kwargs["conv"] = self._converters

    def connect(self):
        raw_connection = MySQLdb.connect(**self._connect_kwargs)
        return self._connection_factory(self, raw_connection)


create_from_uri = MySQL


def _convert_time(time_str):
    h, m, s = time_str.split(":")
    if "." in s:
        f = float(s)
        s = int(f)
        return time(int(h), int(m), s, (f-s)*1000000)
    return time(int(h), int(m), int(s), 0)
