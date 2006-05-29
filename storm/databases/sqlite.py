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

from storm.database import *


class SQLiteResult(Result):

    def get_insert_identity(self, primary_key, primary_values):
        return "(OID=%d)" % self._raw_cursor.lastrowid


class SQLiteConnection(Connection):

    _result_factory = SQLiteResult

    def _to_database(self, value):
        if isinstance(value, (datetime, date, time)):
            return str(value)
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
