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

from storm.expr import compile, Select, compile_select, Undef, And, Eq
from storm.database import *
from storm.exceptions import install_exceptions, UnsupportedDatabaseError


install_exceptions(MySQLdb)


compile = compile.copy()

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
                variable = str(self._raw_cursor.lastrowid)
            equals.append(Eq(column, variable))
        return And(*equals)

    @staticmethod
    def _from_database(value):
        if isinstance(value, array):
            return value.tostring()
        return value


class MySQLConnection(Connection):

    _result_factory = MySQLResult
    _param_mark = "%s"
    _compile = compile


class MySQL(Database):

    _connection_factory = MySQLConnection
    _converters = None

    def __init__(self, dbname, host=None, port=None,
                 username=None, password=None, unix_socket=None):
        if MySQLdb is dummy:
            raise UnsupportedDatabaseError("'MySQLdb' module not found")
        self._connect_kwargs = {}
        if dbname is not None:
            self._connect_kwargs["db"] = dbname
        if host is not None:
            self._connect_kwargs["host"] = host
        if port is not None:
            self._connect_kwargs["port"] = port
        if username is not None:
            self._connect_kwargs["user"] = username
        if password is not None:
            self._connect_kwargs["passwd"] = password
        if unix_socket is not None:
            self._connect_kwargs["unix_socket"] = unix_socket

        if self._converters is None:
            # MySQLdb returns a timedelta by default on TIME fields.
            converters = MySQLdb.converters.conversions.copy()
            converters[MySQLdb.converters.FIELD_TYPE.TIME] = _convert_time
            self.__class__._converters = converters

        self._connect_kwargs["conv"] = self._converters

    def connect(self):
        raw_connection = MySQLdb.connect(**self._connect_kwargs)
        return self._connection_factory(self, raw_connection)


def _convert_time(time_str):
    h, m, s = time_str.split(":")
    if "." in s:
        f = float(s)
        s = int(f)
        return time(int(h), int(m), s, (f-s)*1000000)
    return time(int(h), int(m), int(s), 0)


def create_from_uri(uri):
    return MySQL(uri.database, uri.host, uri.port,
                 uri.username, uri.password, uri.options.get("unix_socket"))
