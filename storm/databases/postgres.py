import psycopg

from storm.expr import Param, Eq, Undef
from storm.database import *


class PostgresResult(Result):

    def get_insert_identity(self, primary_key, primary_values):
        where = Undef
        for prop, value in zip(primary_key, primary_values):
            if value is Undef:
                value = "currval('%s_%s_seq')" % (prop.table, prop.name)
            else:
                value = Param(value)
            if where is Undef:
                where = Eq(prop, value)
            else:
                where &= Eq(prop, value)
        return where


class PostgresConnection(Connection):

    _result_factory = PostgresResult
    _param_mark = "%s"


class Postgres(Database):

    _connection_factory = PostgresConnection

    def __init__(self, dbname, host=None, username=None, password=None):
        self._dsn = "dbname=%s" % dbname
        if host is not None:
            self._dsn += " host=%s" % host
        if username is not None:
            self._dsn += " user=%s" % username
        if password is not None:
            self._dsn += " password=%s" % password

    def connect(self):
        raw_connection = psycopg.connect(self._dsn)
        return self._connection_factory(self, raw_connection)
