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
from time import strptime

import psycopg

from storm.expr import Param, Eq, Undef
from storm.kinds import UnicodeKind
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

    def to_kind(self, value, kind):
        if isinstance(kind, UnicodeKind):
            return unicode(value, self._connection._database._encoding)
        return value


class PostgresConnection(Connection):

    _result_factory = PostgresResult
    _param_mark = "%s"

    @staticmethod
    def _to_database(value):
        if isinstance(value, (datetime, date, time)):
            return str(value)
        return value


class Postgres(Database):

    _connection_factory = PostgresConnection

    def __init__(self, dbname, host=None, port=None,
                 username=None, password=None, encoding=None):
        self._dsn = "dbname=%s" % dbname
        if host is not None:
            self._dsn += " host=%s" % host
        if port is not None:
            self._dsn += " port=%d" % port
        if username is not None:
            self._dsn += " user=%s" % username
        if password is not None:
            self._dsn += " password=%s" % password

        self._encoding = encoding or "UTF-8"

    def connect(self):
        raw_connection = psycopg.connect(self._dsn)
        return self._connection_factory(self, raw_connection)


psycopg.register_type(psycopg.new_type(psycopg.DATETIME.values, "DT", str))


def create_from_uri(uri):
    return Postgres(uri.database, uri.host, uri.port,
                    uri.username, uri.password, uri.options.get("encoding"))

