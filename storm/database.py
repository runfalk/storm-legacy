from storm.expr import Expr, compile


__all__ = ["Database", "Connection", "Result", "convert_param_marks"]


class Result(object):

    def __init__(self, connection, raw_cursor):
        self._connection = connection # Ensures deallocation order.
        self._raw_cursor = raw_cursor

    def fetch_one(self):
        return self._raw_cursor.fetchone()

    def fetch_all(self):
        return self._raw_cursor.fetchall()

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


class Connection(object):

    _result_factory = Result
    _param_mark = "?"
    _compile = compile

    def __init__(self, database, raw_connection):
        self._database = database # Ensures deallocation order.
        self._raw_connection = raw_connection

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
            raw_cursor.execute(statement, params)
        if noresult:
            raw_cursor.close()
            return None
        return self._result_factory(self, raw_cursor)

    def commit(self):
        self._raw_connection.commit()

    def rollback(self):
        self._raw_connection.rollback()


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
