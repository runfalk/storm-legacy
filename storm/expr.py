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
from datetime import datetime, date, time, timedelta
from copy import copy
import sys

from storm.exceptions import CompileError, NoTableError, ExprError
from storm.variables import (
    Variable, BinaryVariable, UnicodeVariable, LazyValue,
    DateTimeVariable, DateVariable, TimeVariable, TimeDeltaVariable,
    BoolVariable, IntVariable, FloatVariable)
from storm import Undef


# --------------------------------------------------------------------
# Basic compiler infrastructure

class Compile(object):
    """Compiler based on the concept of generic functions."""

    def __init__(self, parent=None):
        self._local_dispatch_table = {}
        self._local_precedence = {}
        self._dispatch_table = {}
        self._precedence = {}
        self._hash = None
        self._parents_hash = None
        self._parents = []
        if parent:
            self._parents.extend(parent._parents)
            self._parents.append(parent)

    def _check_parents(self):
        parents_hash = hash(tuple(parent._hash for parent in self._parents))
        if parents_hash != self._parents_hash:
            self._parents_hash = parents_hash
            for parent in self._parents:
                self._dispatch_table.update(parent._local_dispatch_table)
                self._precedence.update(parent._local_precedence)
            self._dispatch_table.update(self._local_dispatch_table)
            self._precedence.update(self._local_precedence)

    def _update(self):
        self._dispatch_table.update(self._local_dispatch_table)
        self._precedence.update(self._local_precedence)
        self._hash = hash(tuple(sorted(self._local_dispatch_table.items() +
                                       self._local_precedence.items())))

    def fork(self):
        return self.__class__(self)

    def when(self, *types):
        def decorator(method):
            for type in types:
                self._local_dispatch_table[type] = method
            self._update()
            return method
        return decorator

    def get_precedence(self, type):
        self._check_parents()
        return self._precedence.get(type, MAX_PRECEDENCE)

    def set_precedence(self, precedence, *types):
        for type in types:
            self._local_precedence[type] = precedence
        self._update()

    def _compile_single(self, state, expr, outer_precedence):
        cls = expr.__class__
        for class_ in cls.__mro__:
            handler = self._dispatch_table.get(class_)
            if handler is not None:
                inner_precedence = state.precedence = \
                                   self._precedence.get(cls, MAX_PRECEDENCE)
                statement = handler(self._compile, state, expr)
                if inner_precedence < outer_precedence:
                    statement = "(%s)" % statement
                return statement
        else:
            raise CompileError("Don't know how to compile type %r of %r"
                               % (expr.__class__, expr))

    def _compile(self, state, expr, join=", ", raw=False):
        # This docstring is in a pretty crappy place; where could it
        # go that would be more discoverable?
        """
        @type state: L{State}.
        @param expr: The expression to compile.
        @param join: The string token to use to put between
                     subexpressions. Defaults to ", ".
        @param raw: If true, any string or unicode expression or
                    subexpression will not be further compiled.
        """
        outer_precedence = state.precedence
        expr_type = type(expr)
        if (expr_type is SQLRaw or
            expr_type is SQLToken or
            raw and (expr_type is str or expr_type is unicode)):
            return expr
        if expr_type in (tuple, list):
            compiled = []
            for subexpr in expr:
                subexpr_type = type(subexpr)
                if (subexpr_type is SQLRaw or
                    subexpr_type is SQLToken or
                    raw and (subexpr_type is str or subexpr_type is unicode)):
                    statement = subexpr
                elif subexpr_type in (tuple, list):
                    state.precedence = outer_precedence
                    statement = self._compile(state, subexpr, join, raw)
                else:
                    statement = self._compile_single(state, subexpr,
                                                     outer_precedence)
                compiled.append(statement)
            statement = join.join(compiled)
        else:
            statement = self._compile_single(state, expr, outer_precedence)
        state.precedence = outer_precedence
        return statement

    def __call__(self, expr):
        self._check_parents()
        state = State()
        return self._compile(state, expr), state.parameters


class CompilePython(Compile):

    def get_expr(self, expr):
        return self._compile(State(), expr)

    def __call__(self, expr):
        exec ("def match(get_column): return bool(%s)" %
              self._compile(State(), expr))
        return match


class State(object):
    """All the data necessary during compilation of an expression.

    @ivar aliases: Dict of L{Column} instances to L{Alias} instances,
        specifying how columns should be compiled as aliases in very
        specific situations.  This is typically used to work around
        strange deficiencies in various databases.

    @ivar auto_tables: The list of all implicitly-used tables.  e.g.,
        in store.find(Foo, Foo.attr==Bar.id), the tables of Bar and
        Foo are implicitly used because columns in them are
        referenced. This is used when building tables.

    @ivar join_tables: If not None, when Join expressions are
        compiled, tables seen will be added to this set. This acts as
        a blacklist against auto_tables when compiling Joins, because
        the generated statements should not refer to the table twice.

    @ivar context: an instance of L{Context}, specifying the context
        of the expression currently being compiled.

    @ivar precedence: Current precedence, automatically set and restored
        by the compiler. If an inner precedence is lower than an outer
        precedence, parenthesis around the inner expression are
        automatically emitted.
    """

    def __init__(self):
        self._stack = []
        self.precedence = 0
        self.parameters = []
        self.auto_tables = []
        self.join_tables = None
        self.context = None
        self.aliases = None

    def push(self, attr, new_value=Undef):
        """Set an attribute in a way that can later be reverted with L{pop}.
        """
        old_value = getattr(self, attr, None)
        self._stack.append((attr, old_value))
        if new_value is Undef:
            new_value = copy(old_value)
        setattr(self, attr, new_value)
        return old_value

    def pop(self):
        """Revert the topmost L{push}.
        """
        setattr(self, *self._stack.pop(-1))


compile = Compile()
compile_python = CompilePython()


# --------------------------------------------------------------------
# Expression contexts

class Context(object):
    """
    An object used to specify the nature of expected SQL expressions
    being compiled in a given context.
    """

    def __init__(self, name):
        self._name = name

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self._name)


TABLE = Context("TABLE")
EXPR = Context("EXPR")
COLUMN = Context("COLUMN")
COLUMN_PREFIX = Context("COLUMN_PREFIX")
COLUMN_NAME = Context("COLUMN_NAME")
SELECT = Context("SELECT")


# --------------------------------------------------------------------
# Builtin type support

@compile.when(str)
def compile_str(compile, state, expr):
    state.parameters.append(BinaryVariable(expr))
    return "?"

@compile.when(unicode)
def compile_unicode(compile, state, expr):
    state.parameters.append(UnicodeVariable(expr))
    return "?"

@compile.when(int, long)
def compile_int(compile, state, expr):
    state.parameters.append(IntVariable(expr))
    return "?"

@compile.when(float)
def compile_float(compile, state, expr):
    state.parameters.append(FloatVariable(expr))
    return "?"

@compile.when(bool)
def compile_bool(compile, state, expr):
    state.parameters.append(BoolVariable(expr))
    return "?"

@compile.when(datetime)
def compile_datetime(compile, state, expr):
    state.parameters.append(DateTimeVariable(expr))
    return "?"

@compile.when(date)
def compile_date(compile, state, expr):
    state.parameters.append(DateVariable(expr))
    return "?"

@compile.when(time)
def compile_time(compile, state, expr):
    state.parameters.append(TimeVariable(expr))
    return "?"

@compile.when(timedelta)
def compile_timedelta(compile, state, expr):
    state.parameters.append(TimeDeltaVariable(expr))
    return "?"

@compile.when(type(None))
def compile_none(compile, state, expr):
    return "NULL"


@compile_python.when(str, unicode, bool, int, long, float,
                     datetime, date, time, timedelta, type(None))
def compile_python_builtin(compile, state, expr):
    return repr(expr)


@compile.when(Variable)
def compile_variable(compile, state, variable):
    state.parameters.append(variable)
    return "?"

@compile_python.when(Variable)
def compile_python_variable(compile, state, variable):
    return repr(variable.get())


class SQLToken(str):
    """Marker for strings the should be considered as a single SQL token.

    In the future, these strings will be quoted, when needed.
    """

@compile.when(SQLToken)
def compile_str(compile, state, expr):
    return expr


# --------------------------------------------------------------------
# Base classes for expressions

MAX_PRECEDENCE = 1000

class Expr(LazyValue):
    pass

@compile_python.when(Expr)
def compile_python_unsupported(compile, state, expr):
    raise CompileError("Can't compile python expressions with %r" % type(expr))


class Comparable(object):

    def __eq__(self, other):
        if other is not None and not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return Eq(self, other)

    def __ne__(self, other):
        if other is not None and not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return Ne(self, other)

    def __gt__(self, other):
        if not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return Gt(self, other)

    def __ge__(self, other):
        if not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return Ge(self, other)

    def __lt__(self, other):
        if not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return Lt(self, other)

    def __le__(self, other):
        if not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return Le(self, other)

    def __rshift__(self, other):
        if not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return RShift(self, other)

    def __lshift__(self, other):
        if not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return LShift(self, other)

    def __and__(self, other):
        if not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return And(self, other)

    def __or__(self, other):
        if not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return Or(self, other)

    def __add__(self, other):
        if not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return Add(self, other)

    def __sub__(self, other):
        if not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return Sub(self, other)

    def __mul__(self, other):
        if not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return Mul(self, other)

    def __div__(self, other):
        if not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return Div(self, other)
    
    def __mod__(self, other):
        if not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return Mod(self, other)

    def is_in(self, others):
        if not isinstance(others, Expr):
            if not others:
                return None
            others = list(others)
            variable_factory = getattr(self, "variable_factory", Variable)
            for i, other in enumerate(others):
                if not isinstance(other, (Expr, Variable)):
                    others[i] = variable_factory(value=other)
        return In(self, others)

    def like(self, other, escape=Undef):
        if not isinstance(other, (Expr, Variable)):
            other = getattr(self, "variable_factory", Variable)(value=other)
        return Like(self, other, escape)

    def lower(self):
        return Lower(self)

    def upper(self):
        return Upper(self)


class ComparableExpr(Expr, Comparable):
    pass

class BinaryExpr(ComparableExpr):

    def __init__(self, expr1, expr2):
        self.expr1 = expr1
        self.expr2 = expr2

class CompoundExpr(ComparableExpr):

    def __init__(self, *exprs):
        self.exprs = exprs


# --------------------------------------------------------------------
# Statement expressions

def has_tables(state, expr):
    return (expr.tables is not Undef or
            expr.default_tables is not Undef or
            state.auto_tables)

def build_tables(compile, state, tables, default_tables):
    """Compile provided tables.

    Tables will be built from either C{tables}, C{state.auto_tables}, or
    C{default_tables}.  If C{tables} is not C{Undef}, it will be used. If
    C{tables} is C{Undef} and C{state.auto_tables} is available, that's used
    instead. If neither C{tables} nor C{state.auto_tables} are available,
    C{default_tables} is tried as a last resort. If none of them are available,
    C{NoTableError} is raised.
    """
    if tables is Undef:
        if state.auto_tables:
            tables = state.auto_tables
        elif default_tables is not Undef:
            tables = default_tables
        else:
            tables = None

    # If we have no elements, it's an error.
    if not tables:
        raise NoTableError("Couldn't find any tables")

    # If it's a single element, it's trivial.
    if type(tables) not in (list, tuple) or len(tables) == 1:
        return compile(state, tables, raw=True)
    
    # If we have no joins, it's trivial as well.
    for elem in tables:
        if isinstance(elem, JoinExpr):
            break
    else:
        if tables is state.auto_tables:
            tables = set(compile(state, table, raw=True) for table in tables)
            return ", ".join(sorted(tables))
        else:
            return compile(state, tables, raw=True)

    # Ok, now we have to be careful.

    # If we're dealing with auto_tables, we have to take care of
    # duplicated tables, join ordering, and so on.
    if tables is state.auto_tables:
        table_stmts = set()
        join_stmts = set()
        half_join_stmts = set()

        # push a join_tables onto the state: compile calls below will
        # populate this set so that we know what tables not to include.
        state.push("join_tables", set())

        for elem in tables:
            statement = compile(state, elem, raw=True)
            if isinstance(elem, JoinExpr):
                if elem.left is Undef:
                    half_join_stmts.add(statement)
                else:
                    join_stmts.add(statement)
            else:
                table_stmts.add(statement)

        # Remove tables that were seen in join statements.
        table_stmts -= state.join_tables

        state.pop()

        result = ", ".join(sorted(table_stmts)+sorted(join_stmts))
        if half_join_stmts:
            result += " " + " ".join(sorted(half_join_stmts))

        return "".join(result)

    # Otherwise, it's just a matter of putting it together.
    result = []
    for elem in tables:
        if result:
            if isinstance(elem, JoinExpr) and elem.left is Undef: #half-join
                result.append(" ")
            else:
                result.append(", ")
        result.append(compile(state, elem, raw=True))
    return "".join(result)


class Select(Expr):

    def __init__(self, columns, where=Undef,
                 tables=Undef, default_tables=Undef,
                 order_by=Undef, group_by=Undef,
                 limit=Undef, offset=Undef, distinct=False):
        self.columns = columns
        self.where = where
        self.tables = tables
        self.default_tables = default_tables
        self.order_by = order_by
        self.group_by = group_by
        self.limit = limit
        self.offset = offset
        self.distinct = distinct

@compile.when(Select)
def compile_select(compile, state, select):
    tokens = ["SELECT "]
    if select.distinct:
        tokens.append("DISTINCT ")
    state.push("auto_tables", [])
    state.push("context", COLUMN)
    tokens.append(compile(state, select.columns))
    tables_pos = len(tokens)
    parameters_pos = len(state.parameters)
    state.context = EXPR
    if select.where is not Undef:
        tokens.append(" WHERE ")
        tokens.append(compile(state, select.where, raw=True))
    if select.order_by is not Undef:
        tokens.append(" ORDER BY ")
        tokens.append(compile(state, select.order_by, raw=True))
    if select.group_by is not Undef:
        tokens.append(" GROUP BY ")
        tokens.append(compile(state, select.group_by, raw=True))
    if select.limit is not Undef:
        tokens.append(" LIMIT %d" % select.limit)
    if select.offset is not Undef:
        tokens.append(" OFFSET %d" % select.offset)
    if has_tables(state, select):
        state.context = TABLE
        state.push("parameters", [])
        tokens.insert(tables_pos, " FROM ")
        tokens.insert(tables_pos+1, build_tables(compile, state, select.tables,
                                                 select.default_tables))
        parameters = state.parameters
        state.pop()
        state.parameters[parameters_pos:parameters_pos] = parameters
    state.pop()
    state.pop()
    return "".join(tokens)


class Insert(Expr):

    def __init__(self, columns, values, table=Undef, default_table=Undef):
        self.columns = columns
        self.values = values
        self.table = table
        self.default_table = default_table

@compile.when(Insert)
def compile_insert(compile, state, insert):
    state.push("context", COLUMN_NAME)
    columns = compile(state, insert.columns)
    state.context = TABLE
    table = build_tables(compile, state, insert.table, insert.default_table)
    state.context = EXPR
    values = compile(state, insert.values)
    state.pop()
    return "".join(["INSERT INTO ", table, " (", columns,
                    ") VALUES (", values, ")"])


class Update(Expr):

    def __init__(self, set, where=Undef, table=Undef, default_table=Undef):
        self.set = set
        self.where = where
        self.table = table
        self.default_table = default_table

@compile.when(Update)
def compile_update(compile, state, update):
    set = update.set
    state.push("context", COLUMN_NAME)
    sets = ["%s=%s" % (compile(state, col), compile(state, set[col]))
            for col in set]
    state.context = TABLE
    tokens = ["UPDATE ", build_tables(compile, state, update.table,
                                      update.default_table),
              " SET ", ", ".join(sets)]
    if update.where is not Undef:
        state.context = EXPR
        tokens.append(" WHERE ")
        tokens.append(compile(state, update.where, raw=True))
    state.pop()
    return "".join(tokens)


class Delete(Expr):

    def __init__(self, where=Undef, table=Undef, default_table=Undef):
        self.where = where
        self.table = table
        self.default_table = default_table

@compile.when(Delete)
def compile_delete(compile, state, delete):
    tokens = ["DELETE FROM ", None]
    state.push("context", EXPR)
    if delete.where is not Undef:
        tokens.append(" WHERE ")
        tokens.append(compile(state, delete.where, raw=True))
    # Compile later for auto_tables support.
    state.context = TABLE
    tokens[1] = build_tables(compile, state, delete.table,
                             delete.default_table)
    state.pop()
    return "".join(tokens)


# --------------------------------------------------------------------
# Columns

class Column(ComparableExpr):
    """Representation of a column in some table.

    @ivar name: Column name.
    @ivar table: Column table (maybe another expression).
    @ivar primary: Integer representing the primary key position of
        this column, or 0 if it's not a primary key. May be provided as
        a bool.
    @ivar variable_factory: Factory producing C{Variable} instances typed
        according to this column.
    """

    def __init__(self, name=Undef, table=Undef, primary=False,
                 variable_factory=None):
        self.name = name
        self.table = table
        self.primary = int(primary)
        self.variable_factory = variable_factory or Variable

@compile.when(Column)
def compile_column(compile, state, column):
    if column.table is not Undef:
        state.auto_tables.append(column.table)
    if column.table is Undef or state.context is COLUMN_NAME:
        if state.aliases is not None:
            # See compile_set_expr().
            alias = state.aliases.get(column)
            if alias is not None:
                return alias.name
        return column.name
    state.push("context", COLUMN_PREFIX)
    table = compile(state, column.table, raw=True)
    state.pop()
    return "%s.%s" % (table, column.name)

@compile_python.when(Column)
def compile_python_column(compile, state, column):
    return "get_column(%s)" % repr(column.name)


# --------------------------------------------------------------------
# Alias expressions

class Alias(ComparableExpr):
    """A representation of "AS" alias clauses. e.g., SELECT foo AS bar.
    """

    auto_counter = 0

    def __init__(self, expr, name=Undef):
        """Create alias of C{expr} AS C{name}.

        If C{name} is not given, then a name will automatically be
        generated.
        """
        self.expr = expr
        if name is Undef:
            Alias.auto_counter += 1
            name = "_%x" % Alias.auto_counter
        self.name = name

@compile.when(Alias)
def compile_alias(compile, state, alias):
    if state.context is COLUMN or state.context is TABLE:
        return "%s AS %s" % (compile(state, alias.expr), alias.name)
    return alias.name


# --------------------------------------------------------------------
# From expressions

class FromExpr(Expr):
    pass


class Table(FromExpr):

    def __init__(self, name):
        self.name = name

@compile.when(Table)
def compile_table(compile, state, table):
    return table.name


class JoinExpr(FromExpr):

    left = on = Undef
    oper = "(unknown)"

    def __init__(self, arg1, arg2=Undef, on=Undef):
        # http://www.postgresql.org/docs/8.1/interactive/explicit-joins.html
        if arg2 is Undef:
            self.right = arg1
            if on is not Undef:
                self.on = on
        elif not isinstance(arg2, Expr) or isinstance(arg2, (FromExpr, Alias)):
            self.left = arg1
            self.right = arg2
            if on is not Undef:
                self.on = on
        else:
            self.right = arg1
            self.on = arg2
            if on is not Undef:
                raise ExprError("Improper join arguments: (%r, %r, %r)" %
                                (arg1, arg2, on))

@compile.when(JoinExpr)
def compile_join(compile, state, expr):
    result = []
    # Ensure that nested JOINs get parentheses.
    state.precedence += 0.5
    if expr.left is not Undef:
        statement = compile(state, expr.left, raw=True)
        result.append(statement)
        if state.join_tables is not None:
            state.join_tables.add(statement)
    result.append(expr.oper)
    statement = compile(state, expr.right, raw=True)
    result.append(statement)
    if state.join_tables is not None:
        state.join_tables.add(statement)
    if expr.on is not Undef:
        state.push("context", EXPR)
        result.append("ON")
        result.append(compile(state, expr.on, raw=True))
        state.pop()
    return " ".join(result)


class Join(JoinExpr):
    oper = "JOIN"

class LeftJoin(JoinExpr):
    oper = "LEFT JOIN"

class RightJoin(JoinExpr):
    oper = "RIGHT JOIN"

class NaturalJoin(JoinExpr):
    oper = "NATURAL JOIN"

class NaturalLeftJoin(JoinExpr):
    oper = "NATURAL LEFT JOIN"

class NaturalRightJoin(JoinExpr):
    oper = "NATURAL RIGHT JOIN"


# --------------------------------------------------------------------
# Operators

class BinaryOper(BinaryExpr):
    oper = " (unknown) "

@compile.when(BinaryOper)
@compile_python.when(BinaryOper)
def compile_binary_oper(compile, state, oper):
    return "%s%s%s" % (compile(state, oper.expr1), oper.oper,
                       compile(state, oper.expr2))


class NonAssocBinaryOper(BinaryOper):
    oper = " (unknown) "

@compile.when(NonAssocBinaryOper)
@compile_python.when(NonAssocBinaryOper)
def compile_non_assoc_binary_oper(compile, state, oper):
    expr1 = compile(state, oper.expr1)
    state.precedence += 0.5 # Enforce parentheses.
    expr2 = compile(state, oper.expr2)
    return "%s%s%s" % (expr1, oper.oper, expr2)


class CompoundOper(CompoundExpr):
    oper = " (unknown) "

@compile.when(CompoundOper)
def compile_compound_oper(compile, state, oper):
    return compile(state, oper.exprs, oper.oper)

@compile_python.when(CompoundOper)
def compile_compound_oper(compile, state, oper):
    return compile(state, oper.exprs, oper.oper.lower())


class Eq(BinaryOper):
    oper = " = "

@compile.when(Eq)
def compile_eq(compile, state, eq):
    if eq.expr2 is None:
        return "%s IS NULL" % compile(state, eq.expr1)
    return "%s = %s" % (compile(state, eq.expr1), compile(state, eq.expr2))

@compile_python.when(Eq)
def compile_eq(compile, state, eq):
    return "%s == %s" % (compile(state, eq.expr1), compile(state, eq.expr2))


class Ne(BinaryOper):
    oper = " != "

@compile.when(Ne)
def compile_ne(compile, state, ne):
    if ne.expr2 is None:
        return "%s IS NOT NULL" % compile(state, ne.expr1)
    return "%s != %s" % (compile(state, ne.expr1), compile(state, ne.expr2))


class Gt(BinaryOper):
    oper = " > "

class Ge(BinaryOper):
    oper = " >= "

class Lt(BinaryOper):
    oper = " < "

class Le(BinaryOper):
    oper = " <= "

class RShift(BinaryOper):
    oper = ">>"

class LShift(BinaryOper):
    oper = "<<"


class Like(BinaryOper):
    oper = " LIKE "

    def __init__(self, expr1, expr2, escape=Undef):
        self.expr1 = expr1
        self.expr2 = expr2
        self.escape = escape

@compile.when(Like)
def compile_binary_oper(compile, state, like):
    statement = "%s%s%s" % (compile(state, like.expr1), like.oper,
                            compile(state, like.expr2))
    if like.escape is not Undef:
        statement = "%s ESCAPE %s" % (statement, compile(state, like.escape))
    return statement

# It's easy to support it. Later.
compile_python.when(Like)(compile_python_unsupported)


class In(BinaryOper):
    oper = " IN "

@compile.when(In)
def compile_in(compile, state, expr):
    expr1 = compile(state, expr.expr1)
    state.precedence = 0 # We're forcing parenthesis here.
    return "%s IN (%s)" % (expr1, compile(state, expr.expr2))

@compile_python.when(In)
def compile_in(compile, state, expr):
    expr1 = compile(state, expr.expr1)
    state.precedence = 0 # We're forcing parenthesis here.
    return "%s in (%s,)" % (expr1, compile(state, expr.expr2))


class Add(CompoundOper):
    oper = "+"

class Sub(NonAssocBinaryOper):
    oper = "-"

class Mul(CompoundOper):
    oper = "*"

class Div(NonAssocBinaryOper):
    oper = "/"

class Mod(NonAssocBinaryOper):
    oper = "%"


class And(CompoundOper):
    oper = " AND "

class Or(CompoundOper):
    oper = " OR "

@compile.when(And, Or)
def compile_compound_oper(compile, state, oper):
    return compile(state, oper.exprs, oper.oper, raw=True)


# --------------------------------------------------------------------
# Set expressions.

class SetExpr(Expr):
    oper = " (unknown) "

    def __init__(self, *exprs, **kwargs):
        self.exprs = exprs
        self.all = kwargs.get("all", False)
        self.order_by = kwargs.get("order_by", Undef)
        self.limit = kwargs.get("limit", Undef)
        self.offset = kwargs.get("offset", Undef)

@compile.when(SetExpr)
def compile_set_expr(compile, state, expr):
    if expr.order_by is not Undef:
        # When ORDER BY is present, databases usually have trouble using
        # fully qualified column names.  Because of that, we transform
        # pure column names into aliases, and use them in the ORDER BY.
        aliases = {}
        for subexpr in expr.exprs:
            if isinstance(subexpr, Select):
                columns = subexpr.columns
                if not isinstance(columns, (tuple, list)):
                    columns = [columns]
                else:
                    columns = list(columns)
                for i, column in enumerate(columns):
                    if column not in aliases:
                        if isinstance(column, Column):
                            aliases[column] = columns[i] = Alias(column)
                        elif isinstance(column, Alias):
                            aliases[column.expr] = column
                subexpr.columns = columns

    state.push("context", SELECT)
    # In the statement:
    #   SELECT foo UNION SELECT bar LIMIT 1
    # The LIMIT 1 applies to the union results, not the SELECT bar
    # This ensures that parentheses will be placed around the
    # sub-selects in the expression.
    state.precedence += 0.5
    oper = expr.oper
    if expr.all:
        oper += "ALL "
    statement = compile(state, expr.exprs, oper)
    state.precedence -= 0.5
    if expr.order_by is not Undef:
        state.context = COLUMN_NAME
        if state.aliases is None:
            state.push("aliases", aliases)
        else:
            # Previously defined aliases have precedence.
            aliases.update(state.aliases)
            state.aliases = aliases
            aliases = None
        statement += " ORDER BY " + compile(state, expr.order_by)
        if aliases is not None:
            state.pop()
    if expr.limit is not Undef:
        statement += " LIMIT %d" % expr.limit
    if expr.offset is not Undef:
        statement += " OFFSET %d" % expr.offset
    state.pop()
    return statement


class Union(SetExpr):
    oper = " UNION "

class Except(SetExpr):
    oper = " EXCEPT "

class Intersect(SetExpr):
    oper = " INTERSECT "


# --------------------------------------------------------------------
# Functions

class FuncExpr(ComparableExpr):
    name = "(unknown)"


class Count(FuncExpr):
    name = "COUNT"

    def __init__(self, column=Undef, distinct=False):
        if distinct and column is Undef:
            raise ValueError("Must specify column when using distinct count")
        self.column = column
        self.distinct = distinct

@compile.when(Count)
def compile_count(compile, state, count):
    if count.column is not Undef:
        if count.distinct:
            return "COUNT(DISTINCT %s)" % compile(state, count.column)
        return "COUNT(%s)" % compile(state, count.column)
    return "COUNT(*)"


class Func(FuncExpr):

    def __init__(self, name, *args):
        self.name = name
        self.args = args

class NamedFunc(FuncExpr):

    def __init__(self, *args):
        self.args = args

@compile.when(Func, NamedFunc)
def compile_func(compile, state, func):
    return "%s(%s)" % (func.name, compile(state, func.args))


class Max(NamedFunc):
    name = "MAX"

class Min(NamedFunc):
    name = "MIN"

class Avg(NamedFunc):
    name = "AVG"

class Sum(NamedFunc):
    name = "SUM"


class Lower(NamedFunc):
    name = "LOWER"

class Upper(NamedFunc):
    name = "UPPER"


# --------------------------------------------------------------------
# Prefix and suffix expressions

class PrefixExpr(Expr):
    prefix = "(unknown)"

    def __init__(self, expr):
        self.expr = expr

@compile.when(PrefixExpr)
def compile_prefix_expr(compile, state, expr):
    return "%s %s" % (expr.prefix, compile(state, expr.expr))


class SuffixExpr(Expr):
    suffix = "(unknown)"

    def __init__(self, expr):
        self.expr = expr

@compile.when(SuffixExpr)
def compile_suffix_expr(compile, state, expr):
    return "%s %s" % (compile(state, expr.expr, raw=True), expr.suffix)


class Not(PrefixExpr):
    prefix = "NOT"

class Exists(PrefixExpr):
    prefix = "EXISTS"

class Asc(SuffixExpr):
    suffix = "ASC"

class Desc(SuffixExpr):
    suffix = "DESC"


# --------------------------------------------------------------------
# Plain SQL expressions.

class SQLRaw(str):
    """Subtype to mark a string as something that shouldn't be compiled.

    This is handled internally by the compiler.
    """

class SQLToken(str):
    """Marker for strings the should be considered as a single SQL token.

    In the future, these strings will be quoted, when needed.

    This is handled internally by the compiler.
    """

class SQL(ComparableExpr):

    def __init__(self, expr, params=Undef, tables=Undef):
        self.expr = expr
        self.params = params
        self.tables = tables

@compile.when(SQL)
def compile_sql(compile, state, expr):
    if expr.params is not Undef:
        if type(expr.params) not in (tuple, list):
            raise CompileError("Parameters should be a list or a tuple, "
                               "not %r" % type(expr.params))
        for param in expr.params:
            state.parameters.append(param)
    if expr.tables is not Undef:
        state.auto_tables.append(expr.tables)
    return expr.expr


# --------------------------------------------------------------------
# Utility functions.

def compare_columns(columns, values):
    if not columns:
        return Undef
    equals = []
    if len(columns) == 1:
        value = values[0]
        if not isinstance(value, (Expr, Variable)) and value is not None:
            value = columns[0].variable_factory(value=value)
        return Eq(columns[0], value)
    else:
        for column, value in zip(columns, values):
            if not isinstance(value, (Expr, Variable)) and value is not None:
                value = column.variable_factory(value=value)
            equals.append(Eq(column, value))
        return And(*equals)


# --------------------------------------------------------------------
# Auto table

class AutoTable(Expr):
    """This class will inject an entry in state.auto_tables.
    
    If the constructor is passed replace=True, it will also discard any
    auto_table entries injected by compiling the given expression.
    """

    def __init__(self, expr, table, replace=False):
        self.expr = expr
        self.table = table
        self.replace = replace

@compile.when(AutoTable)
def compile_auto_table(compile, state, expr):
    if expr.replace:
        state.push("auto_tables", [])
    statement = compile(state, expr.expr)
    if expr.replace:
        state.pop()
    state.auto_tables.append(expr.table)
    return statement


# --------------------------------------------------------------------
# Set operator precedences.

compile.set_precedence(10, Select, Insert, Update, Delete)
compile.set_precedence(10, Join, LeftJoin, RightJoin)
compile.set_precedence(10, NaturalJoin, NaturalLeftJoin, NaturalRightJoin)
compile.set_precedence(10, Union, Except, Intersect)
compile.set_precedence(20, SQL)
compile.set_precedence(30, Or)
compile.set_precedence(40, And)
compile.set_precedence(50, Eq, Ne, Gt, Ge, Lt, Le, Like, In)
compile.set_precedence(60, LShift, RShift)
compile.set_precedence(70, Add, Sub)
compile.set_precedence(80, Mul, Div, Mod)

compile_python.set_precedence(10, Or)
compile_python.set_precedence(20, And)
compile_python.set_precedence(30, Eq, Ne, Gt, Ge, Lt, Le, Like, In)
compile_python.set_precedence(40, LShift, RShift)
compile_python.set_precedence(50, Add, Sub)
compile_python.set_precedence(60, Mul, Div, Mod)
