from pysqlite2 import dbapi2 as sqlite

from storm.database import *


class SQLiteResult(Result):

    def get_insert_identity(self, primary_key, primary_values):
        return "(OID=%d)" % self._raw_cursor.lastrowid


class SQLiteConnection(Connection):

    _result_factory = SQLiteResult


class SQLite(Database):

    _connection_factory = SQLiteConnection

    def __init__(self, filename=None):
        self._filename = filename or ":memory:"

    def connect(self):
        raw_connection = sqlite.connect(self._filename)
        return self._connection_factory(self, raw_connection)

