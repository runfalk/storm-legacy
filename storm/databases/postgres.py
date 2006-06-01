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

from storm.expr import And, Eq
from storm.variables import Variable, UnicodeVariable
from storm.database import *
from storm.exceptions import install_exceptions


install_exceptions(psycopg)


class PostgresResult(Result):

    def get_insert_identity(self, primary_key, primary_variables):
        equals = []
        for column, variable in zip(primary_key, primary_variables):
            if not variable.is_defined():
                variable = "currval('%s_%s_seq')" % (column.table, column.name)
            equals.append(Eq(column, variable))
        return And(*equals)

    def set_variable(self, variable, value):
        if isinstance(variable, UnicodeVariable):
            variable.set(unicode(value, self._connection._database._encoding),
                         from_db=True)
        else:
            variable.set(value, from_db=True)


class PostgresConnection(Connection):

    _result_factory = PostgresResult
    _param_mark = "%s"

    def _to_database(self, value):
        if isinstance(value, Variable):
            value = value.get(to_db=True)
        if isinstance(value, (datetime, date, time)):
            return str(value)
        if isinstance(value, unicode):
            return value.encode(self._database._encoding)
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

