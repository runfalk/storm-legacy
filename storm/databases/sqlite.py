from pysqlite2 import dbapi2 as sqlite

from storm.database import *


class SQLite(Database):

    def __init__(self, filename=None):
        self._filename = filename or ":memory:"

    def connect(self):
        raw_connection = sqlite.connect(self._filename)
        return self._connection_factory(self, raw_connection)
