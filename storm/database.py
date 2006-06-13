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
from storm.exceptions import Error
from storm.uri import URI
import storm


__all__ = ["Database", "Connection", "Result", "convert_param_marks"]


class Result(object):

    _closed = False

    def __init__(self, connection, raw_cursor):
        self._connection = connection # Ensures deallocation order.
        self._raw_cursor = raw_cursor

    def __del__(self):
        if not self._closed:
            try:
                self._raw_cursor.close()
            except Error: # XXX UNTESTED
                pass

    def close(self):
        # XXX UNTESTED
        if not self._closed:
            self._closed = True
            self._raw_cursor.close()
            self._raw_cursor = None

    def get_one(self):
        result = self._raw_cursor.fetchone()
        if result is not None:
            from_database = self._from_database
            return tuple(from_database(x) for x in result)
        return None

    def get_all(self):
        result = self._raw_cursor.fetchall()
        if result:
            from_database = self._from_database
            return [tuple(from_database(x) for x in item) for item in result]
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
    def _from_database(value):
        return value


class Connection(object):

    _result_factory = Result
    _param_mark = "?"
    _compile = compile

    _closed = False

    def __init__(self, database, raw_connection):
        self._database = database # Ensures deallocation order.
        self._raw_connection = raw_connection

    def __del__(self):
        if not self._closed:
            try:
                self._raw_connection.close()
            except Error: # XXX UNTESTED
                pass

    def _build_raw_cursor(self):
        return self._raw_connection.cursor()

    def execute(self, statement, params=None, noresult=False):
        if isinstance(statement, Expr):
            if params is not None:
                raise ValueError("Can't pass parameters with expressions")
            statement, params = self._compile(statement)
        raw_cursor = self._build_raw_cursor()
        statement = convert_param_marks(statement, "?", self._param_mark)
        if params is None:
            raw_cursor.execute(statement)
        else:
            to_database = self._to_database
            raw_cursor.execute(statement, tuple(to_database(param)
                                                for param in params))
        if noresult:
            raw_cursor.close()
            return None
        return self._result_factory(self, raw_cursor)

    def close(self):
        # XXX UNTESTED
        if not self._closed:
            self._closed = True
            self._raw_connection.close()
            self._raw_connection = None

    def commit(self):
        self._raw_connection.commit()

    def rollback(self):
        self._raw_connection.rollback()

    @staticmethod
    def _to_database(value):
        if isinstance(value, Variable):
            return value.get(to_db=True)
        return value


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
    if isinstance(uri, basestring): # XXX UNTESTED
        uri = URI.parse(uri)
    module = __import__("%s.databases.%s" % (storm.__name__, uri.scheme),
                        None, None, [""])
    return module.create_from_uri(uri)
