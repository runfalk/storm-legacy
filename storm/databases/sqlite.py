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
from pysqlite2 import dbapi2 as sqlite

from storm.variables import Variable
from storm.database import *
from storm.exceptions import install_exceptions


install_exceptions(sqlite)


class SQLiteResult(Result):

    def get_insert_identity(self, primary_key, primary_variables):
        return "(OID=%d)" % self._raw_cursor.lastrowid

    def _from_database(self, value):
        if isinstance(value, buffer):
            return str(value)
        return value


class SQLiteConnection(Connection):

    _result_factory = SQLiteResult

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
        self._filename = filename or ":memory:"

    def connect(self):
        raw_connection = sqlite.connect(self._filename)
        return self._connection_factory(self, raw_connection)


def create_from_uri(uri):
    return SQLite(uri.database)
