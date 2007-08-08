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
from decimal import Decimal
from datetime import datetime, date, time, timedelta
from weakref import WeakKeyDictionary
from copy import copy
import re

from storm.exceptions import CompileError, NoTableError, ExprError
from storm.variables import (
    Variable, RawStrVariable, UnicodeVariable, LazyValue,
    DateTimeVariable, DateVariable, TimeVariable, TimeDeltaVariable,
    BoolVariable, IntVariable, FloatVariable, DecimalVariable)
from storm import Undef


# --------------------------------------------------------------------
# Basic compiler infrastructure

class Compile(object):
    """Compiler based on the concept of generic functions."""

    def __init__(self, parent=None):
        self._local_dispatch_table = {}
        self._local_precedence = {}
        self._local_reserved_words = {}
        self._dispatch_table = {}
        self._precedence = {}
        self._reserved_words = {}
        self._hash = None
        self._children = WeakKeyDictionary()
        self._parents = []
        if parent:
            self._parents.extend(parent._parents)
            self._parents.append(parent)
            parent._children[self] = True
            self._update_cache()

    def _update_cache(self):
        for parent in self._parents:
            self._dispatch_table.update(parent._local_dispatch_table)
            self._precedence.update(parent._local_precedence)
            self._reserved_words.update(parent._local_reserved_words)
        self._dispatch_table.update(self._local_dispatch_table)
        self._precedence.update(self._local_precedence)
        self._reserved_words.update(self._local_reserved_words)
        for child in self._children:
            child._update_cache()

    def add_reserved_words(self, words):
        """Include words to be considered reserved and thus escaped.

        Reserved words are escaped during compilation when they're
        seen in a SQLToken expression.
        """
        self._local_reserved_words.update((word.lower(), True)
                                          for word in words)
        self._update_cache()

    def remove_reserved_words(self, words):
        self._local_reserved_words.update((word.lower(), None)
                                          for word in words)
        self._update_cache()

    def is_reserved_word(self, word):
        return self._reserved_words.get(word.lower()) is not None

    def create_child(self):
        """Create a new instance of L{Compile} which inherits from this one.

        This is most commonly used to customize a compiler for
        database-specific compilation strategies.
        """
        return self.__class__(self)

    def when(self, *types):
        def decorator(method):
            for type in types:
                self._local_dispatch_table[type] = method
            self._update_cache()
            return method
        return decorator

    def get_precedence(self, type):
        return self._precedence.get(type, MAX_PRECEDENCE)

    def set_precedence(self, precedence, *types):
        for type in types:
            self._local_precedence[type] = precedence
        self._update_cache()

    def _compile_single(self, expr, state, outer_precedence):
        # FASTPATH This method is part of the fast path.  Be careful when
        #          changing it (try to profile any changes).

        cls = expr.__class__
        dispatch_table = self._dispatch_table
        if cls in dispatch_table:
            handler = dispatch_table[cls]
        else:
            for mro_cls in cls.__mro__:
                # First iteration will always fail because we've already
                # tested that the class itself isn't in the dispatch table.
                if mro_cls in dispatch_table:
                    handler = dispatch_table[mro_cls]
                    break
            else:
                raise CompileError("Don't know how to compile type %r of %r"
                                   % (expr.__class__, expr))
        inner_precedence = state.precedence = \
                           self._precedence.get(cls, MAX_PRECEDENCE)
        statement = handler(self, expr, state)
        if inner_precedence < outer_precedence:
            return "(%s)" % statement
        return statement

    def __call__(self, expr, state=None, join=", ", raw=False, token=False):
        """Compile the given expression into a SQL statement.

        @param expr: The expression to compile.
        @param state: An instance of State, or None, in which case it's
            created internally (and thus can't be accessed).
        @param join: The string token to use to put between
            subexpressions. Defaults to ", ".
        @param raw: If true, any string or unicode expression or
            subexpression will not be further compiled.
        @param token: If true, any string or unicode expression will
            be considered as a SQLToken, and quoted properly.
        """
        # FASTPATH This method is part of the fast path.  Be careful when
        #          changing it (try to profile any changes).

        expr_type = type(expr)

        if (expr_type is SQLRaw or
            raw and (expr_type is str or expr_type is unicode)):
            return expr

        if token and (expr_type is str or expr_type is unicode):
            expr = SQLToken(expr)

        if state is None:
            state = State()

        outer_precedence = state.precedence
        if expr_type is tuple or expr_type is list:
            compiled = []
            for subexpr in expr:
                subexpr_type = type(subexpr)
                if subexpr_type is SQLRaw or raw and (subexpr_type is str or
                                                      subexpr_type is unicode):
                    statement = subexpr
                elif subexpr_type is tuple or subexpr_type is list:
                    state.precedence = outer_precedence
                    statement = self(subexpr, state, join, raw)
                else:
                    if token and (subexpr_type is unicode or
                                  subexpr_type is str):
                        subexpr = SQLToken(subexpr)
                    statement = self._compile_single(subexpr, state,
                                                     outer_precedence)
                compiled.append(statement)
            statement = join.join(compiled)
        else:
            statement = self._compile_single(expr, state, outer_precedence)
        state.precedence = outer_precedence

        return statement


class CompilePython(Compile):

    def get_matcher(self, expr):
        exec "def match(get_column): return bool(%s)" % self(expr)
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
def compile_str(compile, expr, state):
    state.parameters.append(RawStrVariable(expr))
    return "?"

@compile.when(unicode)
def compile_unicode(compile, expr, state):
    state.parameters.append(UnicodeVariable(expr))
    return "?"

@compile.when(int, long)
def compile_int(compile, expr, state):
    state.parameters.append(IntVariable(expr))
    return "?"

@compile.when(float)
def compile_float(compile, expr, state):
    state.parameters.append(FloatVariable(expr))
    return "?"

@compile.when(Decimal)
def compile_decimal(compile, expr, state):
    state.parameters.append(DecimalVariable(expr))
    return "?"

@compile.when(bool)
def compile_bool(compile, expr, state):
    state.parameters.append(BoolVariable(expr))
    return "?"

@compile.when(datetime)
def compile_datetime(compile, expr, state):
    state.parameters.append(DateTimeVariable(expr))
    return "?"

@compile.when(date)
def compile_date(compile, expr, state):
    state.parameters.append(DateVariable(expr))
    return "?"

@compile.when(time)
def compile_time(compile, expr, state):
    state.parameters.append(TimeVariable(expr))
    return "?"

@compile.when(timedelta)
def compile_timedelta(compile, expr, state):
    state.parameters.append(TimeDeltaVariable(expr))
    return "?"

@compile.when(type(None))
def compile_none(compile, expr, state):
    return "NULL"


@compile_python.when(str, unicode, bool, int, long, float,
                     datetime, date, time, timedelta, type(None))
def compile_python_builtin(compile, expr, state):
    return repr(expr)


@compile.when(Variable)
def compile_variable(compile, variable, state):
    state.parameters.append(variable)
    return "?"

@compile_python.when(Variable)
def compile_python_variable(compile, variable, state):
    return repr(variable.get())


# --------------------------------------------------------------------
# Base classes for expressions

MAX_PRECEDENCE = 1000

class Expr(LazyValue):
    pass

@compile_python.when(Expr)
def compile_python_unsupported(compile, expr, state):
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

def build_tables(compile, tables, default_tables, state):
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
        return compile(tables, state, token=True)
 
    # If we have no joins, it's trivial as well.
    for elem in tables:
        if isinstance(elem, JoinExpr):
            break
    else:
        if tables is state.auto_tables:
            tables = set(compile(table, state, token=True) for table in tables)
            return ", ".join(sorted(tables))
        else:
            return compile(tables, state, token=True)

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
            statement = compile(elem, state, token=True)
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
        result.append(compile(elem, state, token=True))
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
def compile_select(compile, select, state):
    tokens = ["SELECT "]
    if select.distinct:
        tokens.append("DISTINCT ")
    state.push("auto_tables", [])
    state.push("context", COLUMN)
    tokens.append(compile(select.columns, state))
    tables_pos = len(tokens)
    parameters_pos = len(state.parameters)
    state.context = EXPR
    if select.where is not Undef:
        tokens.append(" WHERE ")
        tokens.append(compile(select.where, state, raw=True))
    if select.group_by is not Undef:
        tokens.append(" GROUP BY ")
        tokens.append(compile(select.group_by, state, raw=True))
    if select.order_by is not Undef:
        tokens.append(" ORDER BY ")
        tokens.append(compile(select.order_by, state, raw=True))
    if select.limit is not Undef:
        tokens.append(" LIMIT %d" % select.limit)
    if select.offset is not Undef:
        tokens.append(" OFFSET %d" % select.offset)
    if has_tables(state, select):
        state.context = TABLE
        state.push("parameters", [])
        tokens.insert(tables_pos, " FROM ")
        tokens.insert(tables_pos+1, build_tables(compile, select.tables,
                                                 select.default_tables, state))
        parameters = state.parameters
        state.pop()
        state.parameters[parameters_pos:parameters_pos] = parameters
    state.pop()
    state.pop()
    return "".join(tokens)


class Insert(Expr):
    """Expression representing an insert statement.

    @ivar map: Dictionary mapping columns to values.
    @ivar table: Table where the row should be inserted.
    @ivar default_table: Table to use if no table is explicitly provided, and
        no tables may be inferred from provided columns.
    @ivar primary_columns: Tuple of columns forming the primary key of the
        table where the row will be inserted.  This is a hint used by backends
        to process the insertion of rows.
    @ivar primary_variables: Tuple of variables with values for the primary
        key of the table where the row will be inserted.  This is a hint used
        by backends to process the insertion of rows.
    """

    def __init__(self, map, table=Undef, default_table=Undef,
                 primary_columns=Undef, primary_variables=Undef):
        self.map = map
        self.table = table
        self.default_table = default_table
        self.primary_columns = primary_columns
        self.primary_variables = primary_variables

@compile.when(Insert)
def compile_insert(compile, insert, state):
    state.push("context", COLUMN_NAME)
    columns = compile(tuple(insert.map), state)
    state.context = TABLE
    table = build_tables(compile, insert.table, insert.default_table, state)
    state.context = EXPR
    values = compile(tuple(insert.map.itervalues()), state)
    state.pop()
    return "".join(["INSERT INTO ", table, " (", columns,
                    ") VALUES (", values, ")"])


class Update(Expr):

    def __init__(self, map, where=Undef, table=Undef, default_table=Undef):
        self.map = map
        self.where = where
        self.table = table
        self.default_table = default_table

@compile.when(Update)
def compile_update(compile, update, state):
    map = update.map
    state.push("context", COLUMN_NAME)
    sets = ["%s=%s" % (compile(col, state), compile(map[col], state))
            for col in map]
    state.context = TABLE
    tokens = ["UPDATE ", build_tables(compile, update.table,
                                      update.default_table, state),
              " SET ", ", ".join(sets)]
    if update.where is not Undef:
        state.context = EXPR
        tokens.append(" WHERE ")
        tokens.append(compile(update.where, state, raw=True))
    state.pop()
    return "".join(tokens)


class Delete(Expr):

    def __init__(self, where=Undef, table=Undef, default_table=Undef):
        self.where = where
        self.table = table
        self.default_table = default_table

@compile.when(Delete)
def compile_delete(compile, delete, state):
    tokens = ["DELETE FROM ", None]
    state.push("context", EXPR)
    if delete.where is not Undef:
        tokens.append(" WHERE ")
        tokens.append(compile(delete.where, state, raw=True))
    # Compile later for auto_tables support.
    state.context = TABLE
    tokens[1] = build_tables(compile, delete.table,
                             delete.default_table, state)
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
def compile_column(compile, column, state):
    if column.table is not Undef:
        state.auto_tables.append(column.table)
    if column.table is Undef or state.context is COLUMN_NAME:
        if state.aliases is not None:
            # See compile_set_expr().
            alias = state.aliases.get(column)
            if alias is not None:
                return compile(alias.name, state, token=True)
        return column.name
    state.push("context", COLUMN_PREFIX)
    table = compile(column.table, state, token=True)
    state.pop()
    return "%s.%s" % (table, compile(column.name, state, token=True))

@compile_python.when(Column)
def compile_python_column(compile, column, state):
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
def compile_alias(compile, alias, state):
    name = compile(alias.name, state, token=True)
    if state.context is COLUMN or state.context is TABLE:
        return "%s AS %s" % (compile(alias.expr, state), name)
    return name


# --------------------------------------------------------------------
# From expressions

class FromExpr(Expr):
    pass


class Table(FromExpr):

    def __init__(self, name):
        self.name = name

@compile.when(Table)
def compile_table(compile, table, state):
    return compile(table.name, state, token=True)


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
def compile_join(compile, join, state):
    result = []
    # Ensure that nested JOINs get parentheses.
    state.precedence += 0.5
    if join.left is not Undef:
        statement = compile(join.left, state, token=True)
        result.append(statement)
        if state.join_tables is not None:
            state.join_tables.add(statement)
    result.append(join.oper)
    statement = compile(join.right, state, token=True)
    result.append(statement)
    if state.join_tables is not None:
        state.join_tables.add(statement)
    if join.on is not Undef:
        state.push("context", EXPR)
        result.append("ON")
        result.append(compile(join.on, state, raw=True))
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
def compile_binary_oper(compile, expr, state):
    return "%s%s%s" % (compile(expr.expr1, state), expr.oper,
                       compile(expr.expr2, state))


class NonAssocBinaryOper(BinaryOper):
    oper = " (unknown) "

@compile.when(NonAssocBinaryOper)
@compile_python.when(NonAssocBinaryOper)
def compile_non_assoc_binary_oper(compile, expr, state):
    expr1 = compile(expr.expr1, state)
    state.precedence += 0.5 # Enforce parentheses.
    expr2 = compile(expr.expr2, state)
    return "%s%s%s" % (expr1, expr.oper, expr2)


class CompoundOper(CompoundExpr):
    oper = " (unknown) "

@compile.when(CompoundOper)
def compile_compound_oper(compile, expr, state):
    return compile(expr.exprs, state, join=expr.oper)

@compile_python.when(CompoundOper)
def compile_compound_oper(compile, expr, state):
    return compile(expr.exprs, state, join=expr.oper.lower())


class Eq(BinaryOper):
    oper = " = "

@compile.when(Eq)
def compile_eq(compile, eq, state):
    if eq.expr2 is None:
        return "%s IS NULL" % compile(eq.expr1, state)
    return "%s = %s" % (compile(eq.expr1, state), compile(eq.expr2, state))

@compile_python.when(Eq)
def compile_eq(compile, eq, state):
    return "%s == %s" % (compile(eq.expr1, state), compile(eq.expr2, state))


class Ne(BinaryOper):
    oper = " != "

@compile.when(Ne)
def compile_ne(compile, ne, state):
    if ne.expr2 is None:
        return "%s IS NOT NULL" % compile(ne.expr1, state)
    return "%s != %s" % (compile(ne.expr1, state), compile(ne.expr2, state))


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
def compile_binary_oper(compile, like, state):
    statement = "%s%s%s" % (compile(like.expr1, state), like.oper,
                            compile(like.expr2, state))
    if like.escape is not Undef:
        statement = "%s ESCAPE %s" % (statement, compile(like.escape, state))
    return statement

# It's easy to support it. Later.
compile_python.when(Like)(compile_python_unsupported)


class In(BinaryOper):
    oper = " IN "

@compile.when(In)
def compile_in(compile, expr, state):
    expr1 = compile(expr.expr1, state)
    state.precedence = 0 # We're forcing parenthesis here.
    return "%s IN (%s)" % (expr1, compile(expr.expr2, state))

@compile_python.when(In)
def compile_in(compile, expr, state):
    expr1 = compile(expr.expr1, state)
    state.precedence = 0 # We're forcing parenthesis here.
    return "%s in (%s,)" % (expr1, compile(expr.expr2, state))


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
def compile_compound_oper(compile, expr, state):
    return compile(expr.exprs, state, join=expr.oper, raw=True)


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
def compile_set_expr(compile, expr, state):
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
    statement = compile(expr.exprs, state, join=oper)
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
        statement += " ORDER BY " + compile(expr.order_by, state)
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
def compile_count(compile, count, state):
    if count.column is not Undef:
        if count.distinct:
            return "COUNT(DISTINCT %s)" % compile(count.column, state)
        return "COUNT(%s)" % compile(count.column, state)
    return "COUNT(*)"


class Func(FuncExpr):

    def __init__(self, name, *args):
        self.name = name
        self.args = args

class NamedFunc(FuncExpr):

    def __init__(self, *args):
        self.args = args

@compile.when(Func, NamedFunc)
def compile_func(compile, func, state):
    return "%s(%s)" % (func.name, compile(func.args, state))


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
def compile_prefix_expr(compile, expr, state):
    return "%s %s" % (expr.prefix, compile(expr.expr, state))


class SuffixExpr(Expr):
    suffix = "(unknown)"

    def __init__(self, expr):
        self.expr = expr

@compile.when(SuffixExpr)
def compile_suffix_expr(compile, expr, state):
    return "%s %s" % (compile(expr.expr, state, raw=True), expr.suffix)


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
    """Marker for strings that should be considered as a single SQL token.

    These strings will be quoted, when needed.
    """

is_safe_token = re.compile("^[a-zA-Z][a-zA-Z0-9_]*$").match

@compile.when(SQLToken)
def compile_sql_token(compile, expr, state):
    if is_safe_token(expr) and not compile.is_reserved_word(expr):
        return expr
    return '"%s"' % expr.replace('"', '""')

@compile_python.when(SQLToken)
def compile_python_sql_token(compile, expr, state):
    return expr


class SQL(ComparableExpr):

    def __init__(self, expr, params=Undef, tables=Undef):
        self.expr = expr
        self.params = params
        self.tables = tables

@compile.when(SQL)
def compile_sql(compile, expr, state):
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
# Sequences.

class Sequence(Expr):
    """Expression representing auto-incrementing support from the database.

    This should be translated into the *next* value of the named
    auto-incrementing sequence.  There's no standard way to compile a
    sequence, since it's very database-dependent.

    This may be used as follows::

      class Class(object):
          (...)
          id = Int(default=Sequence("my_sequence_name"))
    """

    def __init__(self, name):
        self.name = name


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
def compile_auto_table(compile, expr, state):
    if expr.replace:
        state.push("auto_tables", [])
    statement = compile(expr.expr, state)
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


# --------------------------------------------------------------------
# Reserved words, from SQL1992

compile.add_reserved_words(
    """
    absolute action add all allocate alter and any are as asc assertion at
    authorization avg begin between bit bit_length both by cascade cascaded
    case cast catalog char character char_ length character_length check close
    coalesce collate collation column commit connect connection constraint
    constraints continue convert corresponding count create cross current
    current_date current_time current_timestamp current_ user cursor date day
    deallocate dec decimal declare default deferrable deferred delete desc
    describe descriptor diagnostics disconnect distinct domain double drop
    else end end-exec escape except exception exec execute exists external
    extract false fetch first float for foreign found from full get global go
    goto grant group having hour identity immediate in indicator initially
    inner input insensitive insert int integer intersect interval into is
    isolation join key language last leading left level like local lower
    match max min minute module month names national natural nchar next no
    not null nullif numeric octet_length of on only open option or order
    outer output overlaps pad partial position precision prepare preserve
    primary prior privileges procedure public read real references relative
    restrict revoke right rollback rows schema scroll second section select
    session session_ user set size smallint some space sql sqlcode sqlerror
    sqlstate substring sum system_user table temporary then time timestamp
    timezone_ hour timezone_minute to trailing transaction translate
    translation trim true union unique unknown update upper usage user using
    value values varchar varying view when whenever where with work write
    year zone
    """.split())
