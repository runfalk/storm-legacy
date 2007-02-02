#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
from storm.expr import Expr, compile
from storm.variables import Variable
from storm.exceptions import Error, ClosedError
from storm.uri import URI
import storm


__all__ = ["Database", "Connection", "Result",
           "convert_param_marks", "create_database"]


DEBUG = False


class Result(object):

    _closed = False

    def __init__(self, connection, raw_cursor):
        self._connection = connection # Ensures deallocation order.
        self._raw_cursor = raw_cursor

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def close(self):
        if not self._closed:
            self._closed = True
            self._raw_cursor.close()
            self._raw_cursor = None

    def get_one(self):
        row = self._raw_cursor.fetchone()
        if row is not None:
            return tuple(self._from_database(row))
        return None

    def get_all(self):
        result = self._raw_cursor.fetchall()
        if result:
            return [tuple(self._from_database(row)) for row in result]
        return result

    def __iter__(self):
        if self._raw_cursor.arraysize == 1:
            while True:
                result = self._raw_cursor.fetchone()
                if not result:
                    break
                yield result
        else:
            while True:
                results = self._raw_cursor.fetchmany()
                if not results:
                    break
                for result in results:
                    yield result

    def get_insert_identity(self, primary_columns, primary_variables):
        raise NotImplementedError

    @staticmethod
    def set_variable(variable, value):
        variable.set(value, from_db=True)

    @staticmethod
    def _from_database(row):
        return row


class Connection(object):

    _result_factory = Result
    _param_mark = "?"
    _compile = compile

    _closed = False

    def __init__(self, database, raw_connection):
        self._database = database # Ensures deallocation order.
        self._raw_connection = raw_connection

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def _build_raw_cursor(self):
        return self._raw_connection.cursor()

    def execute(self, statement, params=None, noresult=False):
        if self._closed:
            raise ClosedError("Connection is closed")
        if isinstance(statement, Expr):
            if params is not None:
                raise ValueError("Can't pass parameters with expressions")
            statement, params = self._compile(statement)
        raw_cursor = self._build_raw_cursor()
        statement = convert_param_marks(statement, "?", self._param_mark)
        if params is None:
            if DEBUG:
                print statement, () 
            raw_cursor.execute(statement)
        else:
            params = tuple(self._to_database(params))
            if DEBUG:
                print statement, params
            raw_cursor.execute(statement, params)
        if noresult:
            raw_cursor.close()
            return None
        return self._result_factory(self, raw_cursor)

    def close(self):
        if not self._closed:
            self._closed = True
            self._raw_connection.close()
            self._raw_connection = None

    def commit(self):
        self._raw_connection.commit()

    def rollback(self):
        self._raw_connection.rollback()

    @staticmethod
    def _to_database(params):
        for param in params:
            if isinstance(param, Variable):
                yield param.get(to_db=True)
            else:
                yield param


class Database(object):

    _connection_factory = Connection

    def connect(self):
        raise NotImplementedError


def convert_param_marks(statement, from_param_mark, to_param_mark):
    # TODO: Add support for $foo$bar$foo$ literals.
    if from_param_mark == to_param_mark or from_param_mark not in statement:
        return statement
    tokens = statement.split("'")
    for i in range(0, len(tokens), 2):
        tokens[i] = tokens[i].replace(from_param_mark, to_param_mark)
    return "'".join(tokens)


def create_database(uri):
    """Create a database instance.

    @param uri: An URI instance, or a string describing the URI. Some examples:
        "sqlite:" An in memory sqlite database.
        "sqlite:example.db" A SQLite database called example.db
        "postgres:test" The database 'test' from the local postgres server.
        "postgres://user:password@host/test" The database test on machine host
            with supplied user credentials, using postgres.
    """
    if isinstance(uri, basestring):
        uri = URI.parse(uri)
    module = __import__("%s.databases.%s" % (storm.__name__, uri.scheme),
                        None, None, [""])
    return module.create_from_uri(uri)
