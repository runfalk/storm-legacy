from datetime import datetime, date, time
from time import strptime

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

    @staticmethod
    def _to_database(value):
        if isinstance(value, (datetime, date, time)):
            return value.isoformat()
        return value


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


def convert_datetime(value):
    if value is None:
        return None
    elif " " in value:
        # TODO Add suport for timezones.
        tokens = value.split(".")
        time_tuple = strptime(tokens[0], "%Y-%m-%d %H:%M:%S")
        return datetime(*time_tuple[:6]+(int(tokens[1]),))
    elif ":" in value:
        tokens = value.split(".")
        time_tuple = strptime(tokens[0], "%H:%M:%S")
        return time(*time_tuple[3:6]+(int(tokens[1]),))
    else:
        time_tuple = strptime(value, "%Y-%m-%d")
        return date(*time_tuple[:3])

psycopg.register_type(psycopg.new_type(psycopg.DATETIME.values,
                                       "DT", convert_datetime))
