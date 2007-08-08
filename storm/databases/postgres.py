#
# Copyright (c) 2006, 2007 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# Storm is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 2.1 of
# the License, or (at your option) any later version.
#
# Storm is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
from datetime import datetime, date, time

from storm.databases import dummy

try:
    import psycopg2
    import psycopg2.extensions
except:
    psycopg2 = dummy

from storm.expr import (
    Undef, SetExpr, Insert, Select, Alias, And, Eq, FuncExpr, SQLRaw, Sequence,
    COLUMN_NAME, compile, compile_insert, compile_select, compile_set_expr)
from storm.variables import Variable, ListVariable
from storm.database import Database, Connection, Result
from storm.exceptions import install_exceptions, DatabaseModuleError


install_exceptions(psycopg2)
compile = compile.create_child()


class currval(FuncExpr):

    name = "currval"

    def __init__(self, column):
        self.column = column

@compile.when(currval)
def compile_currval(compile, expr, state):
    return "currval('%s_%s_seq')" % (compile(expr.column.table, state),
                                     expr.column.name)

@compile.when(ListVariable)
def compile_list_variable(compile, list_variable, state):
    elements = []
    variables = list_variable.get(to_db=True)
    if variables is None:
        return "NULL"
    if not variables:
        return "'{}'"
    for variable in variables:
        elements.append(compile(variable, state))
    return "ARRAY[%s]" % ",".join(elements)

@compile.when(SetExpr)
def compile_set_expr_postgres(compile, expr, state):
    if expr.order_by is not Undef:
        # The following statement breaks in postgres:
        #     SELECT 1 AS id UNION SELECT 1 ORDER BY id+1
        # With the error:
        #     ORDER BY on a UNION/INTERSECT/EXCEPT result must
        #     be on one of the result columns
        # So we transform it into something close to:
        #     SELECT * FROM (SELECT 1 AS id UNION SELECT 1) AS a ORDER BY id+1

        # Build new set expression without arguments (order_by, etc).
        new_expr = expr.__class__()
        new_expr.exprs = expr.exprs
        new_expr.all = expr.all

        # Make sure that state.aliases isn't None, since we want them to
        # compile our order_by statement below.
        no_aliases = state.aliases is None
        if no_aliases:
            state.push("aliases", {})

        # Build set expression, collecting aliases.
        set_stmt = SQLRaw("(%s)" % compile_set_expr(compile, new_expr, state))

        # Build order_by statement, using aliases.
        state.push("context", COLUMN_NAME)
        order_by_stmt = SQLRaw(compile(expr.order_by, state))
        state.pop()

        # Discard aliases, if they were not being collected previously.
        if no_aliases:
            state.pop()

        # Build wrapping select statement.
        select = Select(SQLRaw("*"), tables=Alias(set_stmt), limit=expr.limit,
                        offset=expr.offset, order_by=order_by_stmt)
        return compile_select(compile, select, state)
    else:
        return compile_set_expr(compile, expr, state)


@compile.when(Insert)
def compile_insert_postgres(compile, insert, state):
    # PostgreSQL fails with INSERT INTO table VALUES (), so we transform
    # that to INSERT INTO table (id) VALUES (DEFAULT).
    if not insert.map and insert.primary_columns is not Undef:
        insert.map.update(dict.fromkeys(insert.primary_columns,
                                        SQLRaw("DEFAULT")))
    return compile_insert(compile, insert, state)


@compile.when(Sequence)
def compile_sequence_postgres(compile, sequence, state):
    return "nextval('%s')" % sequence.name


class PostgresResult(Result):

    def get_insert_identity(self, primary_key, primary_variables):
        equals = []
        for column, variable in zip(primary_key, primary_variables):
            if not variable.is_defined():
                variable = currval(column)
            equals.append(Eq(column, variable))
        return And(*equals)


class PostgresConnection(Connection):

    result_factory = PostgresResult
    param_mark = "%s"
    compile = compile

    def raw_execute(self, statement, params):
        """
        Like L{Connection.raw_execute}, but encode the statement to
        UTF-8 if it is unicode.
        """
        if type(statement) is unicode:
            # psycopg breaks with unicode statements.
            statement = statement.encode("UTF-8")
        return Connection.raw_execute(self, statement, params)

    def to_database(self, params):
        """
        Like L{Connection.to_database}, but this converts datetime
        types to strings, unicode to UTF-8 encoded strings, and
        strings to L{psycopg2.Binary} instances.
        """
        for param in params:
            if isinstance(param, Variable):
                param = param.get(to_db=True)
            if isinstance(param, (datetime, date, time)):
                yield str(param)
            elif isinstance(param, unicode):
                yield param.encode("UTF-8")
            elif isinstance(param, str):
                yield psycopg2.Binary(param)
            else:
                yield param


class Postgres(Database):

    connection_factory = PostgresConnection

    def __init__(self, uri):
        if psycopg2 is dummy:
            raise DatabaseModuleError("'psycopg2' module not found")
        self._dsn = make_dsn(uri)

    def connect(self):
        raw_connection = psycopg2.connect(self._dsn)
        raw_connection.set_client_encoding("UTF8")
        raw_connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        return self.connection_factory(self, raw_connection)


create_from_uri = Postgres


if psycopg2 is not dummy:
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    psycopg2.extensions.register_type(psycopg2._psycopg.UNICODEARRAY)


def make_dsn(uri):
    """Convert a URI object to a PostgreSQL DSN string."""
    dsn = "dbname=%s" % uri.database
    if uri.host is not None:
        dsn += " host=%s" % uri.host
    if uri.port is not None:
        dsn += " port=%d" % uri.port
    if uri.username is not None:
        dsn += " user=%s" % uri.username
    if uri.password is not None:
        dsn += " password=%s" % uri.password
    return dsn
