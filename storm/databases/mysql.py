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

import MySQLdb.converters
import MySQLdb

from storm.expr import And, Eq 
from storm.database import *


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


class MySQL(Database):

    _connection_factory = MySQLConnection
    _converters = None

    def __init__(self, dbname, host=None, port=None,
                 username=None, password=None, unix_socket=None):
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
