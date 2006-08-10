#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
from copy import copy
import sys

from storm.exceptions import CompileError, NoTableError, ExprError
from storm.variables import Variable
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

    def _compile(self, state, expr, join=", "):
        outer_precedence = state.precedence
        if type(expr) is str:
            return expr
        if type(expr) in (tuple, list):
            compiled = []
            for subexpr in expr:
                if type(subexpr) is str:
                    statement = subexpr
                elif type(subexpr) in (tuple, list):
                    state.precedence = outer_precedence
                    statement = self._compile(state, subexpr, join)
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

    def __init__(self):
        self._stack = []
        self.precedence = 0
        self.parameters = []
        self.auto_tables = []
        self.omit_column_tables = False

    def push(self, attr, new_value=Undef):
        old_value = getattr(self, attr, None)
        self._stack.append((attr, old_value))
        if new_value is Undef:
            new_value = copy(old_value)
        setattr(self, attr, new_value)

    def pop(self):
        setattr(self, *self._stack.pop(-1))


compile = Compile()
compile_python = CompilePython()


# --------------------------------------------------------------------
# Builtin type support

# Most common case. Optimized in Compile._compile.
#@compile.when(str)
#def compile_str(compile, state, expr):
#    return expr

@compile.when(type(None))
def compile_none(compile, state, expr):
    return "NULL"

@compile_python.when(type(None))
def compile_python_none(compile, state, expr):
    return "None"


@compile.when(Variable)
def compile_variable(compile, state, variable):
    state.parameters.append(variable)
    return "?"

@compile_python.when(Variable)
def compile_python_variable(compile, state, variable):
    return repr(variable.get())

# --------------------------------------------------------------------
# Base classes for expressions

MAX_PRECEDENCE = 1000

class Expr(object):
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
        # FIXME others should support non-list argument
        others = list(others)
        variable_factory = getattr(self, "variable_factory", Variable)
        for i, other in enumerate(others):
            if not isinstance(other, (Expr, Variable)):
                others[i] = variable_factory(value=other)
        return In(self, others)


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

def build_tables(compile, state, expr):
    if expr.tables is not Undef:
        result = []
        if type(expr.tables) not in (list, tuple):
            return compile(state, expr.tables)
        else:
            for elem in expr.tables:
                if result:
                    if not (isinstance(elem, JoinExpr) and elem.left is Undef):
                        result.append(", ")
                    else:
                        result.append(" ")
                result.append(compile(state, elem))
            return "".join(result)
    elif state.auto_tables:
        tables = []
        for expr in state.auto_tables:
            table = compile(state, expr)
            if table not in tables:
                tables.append(table)
        return ", ".join(tables)
    elif expr.default_tables is not Undef:
        return compile(state, expr.default_tables)
    raise NoTableError("Couldn't find any tables")

def build_table(compile, state, expr):
    if expr.table is not Undef:
        return compile(state, expr.table)
    elif state.auto_tables:
        tables = []
        for expr in state.auto_tables:
            table = compile(state, expr)
            if table not in tables:
                tables.append(table)
        return ", ".join(tables)
    elif expr.default_table is not Undef:
        return compile(state, expr.default_table)
    raise NoTableError("Couldn't find any table")


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
    state.push("auto_tables", [])
    tokens = ["SELECT "]
    if select.distinct:
        tokens.append("DISTINCT ")
    tokens.append(compile(state, select.columns))
    tables_pos = len(tokens)
    if select.where is not Undef:
        tokens.append(" WHERE ")
        tokens.append(compile(state, select.where))
    if select.order_by is not Undef:
        tokens.append(" ORDER BY ")
        tokens.append(compile(state, select.order_by))
    if select.group_by is not Undef:
        tokens.append(" GROUP BY ")
        tokens.append(compile(state, select.group_by))
    if select.limit is not Undef:
        tokens.append(" LIMIT %d" % select.limit)
    if select.offset is not Undef:
        tokens.append(" OFFSET %d" % select.offset)
    if has_tables(state, select):
        tokens.insert(tables_pos, " FROM ")
        tokens.insert(tables_pos+1, build_tables(compile, state, select))
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
    state.push("omit_column_tables", True)
    columns = compile(state, insert.columns)
    state.pop()
    tokens = ["INSERT INTO ", build_table(compile, state, insert),
              " (", columns, ") VALUES (", compile(state, insert.values), ")"]
    return "".join(tokens)


class Update(Expr):

    def __init__(self, set, where=Undef, table=Undef, default_table=Undef):
        self.set = set
        self.where = where
        self.table = table
        self.default_table = default_table

@compile.when(Update)
def compile_update(compile, state, update):
    state.push("omit_column_tables", True)
    set = update.set
    sets = ["%s=%s" % (compile(state, col), compile(state, set[col]))
            for col in set]
    state.pop()
    tokens = ["UPDATE ", build_table(compile, state, update),
              " SET ", ", ".join(sets)]
    if update.where is not Undef:
        tokens.append(" WHERE ")
        tokens.append(compile(state, update.where))
    return "".join(tokens)


class Delete(Expr):

    def __init__(self, where=Undef, table=Undef, default_table=Undef):
        self.where = where
        self.table = table
        self.default_table = default_table

@compile.when(Delete)
def compile_delete(compile, state, delete):
    tokens = ["DELETE FROM ", None]
    if delete.where is not Undef:
        tokens.append(" WHERE ")
        tokens.append(compile(state, delete.where))
    # Compile later for auto_tables support.
    tokens[1] = build_table(compile, state, delete)
    return "".join(tokens)


# --------------------------------------------------------------------
# Columns

class Column(ComparableExpr):

    def __init__(self, name=Undef, table=Undef, variable_factory=None):
        self.name = name
        self.table = table
        self.variable_factory = variable_factory or Variable

@compile.when(Column)
def compile_column(compile, state, column):
    if column.table is not Undef:
        state.auto_tables.append(column.table)
    if column.table is Undef or state.omit_column_tables:
        return column.name
    return "%s.%s" % (compile(state, column.table), column.name)

@compile_python.when(Column)
def compile_python_column(compile, state, column):
    return "get_column(%s)" % repr(column.name)


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


class As(FromExpr):
    
    auto_counter = 0

    def __init__(self, expr, name=Undef):
        self.expr = expr
        if name is Undef:
            As.auto_counter += 1
            name = "_%x" % As.auto_counter
        self.name = name


@compile.when(As)
def compile_as(compile, state, expr):
    return "%s AS %s" % (compile(state, expr.expr), expr.name)


class JoinExpr(FromExpr):

    left = on = Undef
    oper = "(unknown)"

    def __init__(self, arg1, arg2=Undef, on=Undef):
        # http://www.postgresql.org/docs/8.1/interactive/explicit-joins.html
        if arg2 is Undef:
            self.right = arg1
            if on is not Undef:
                self.on = on
        elif not isinstance(arg2, Expr) or isinstance(arg2, FromExpr):
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
    state.precedence += 0.5
    if expr.left is not Undef:
        result.append(compile(state, expr.left))
    result.append(expr.oper)
    result.append(compile(state, expr.right))
    if expr.on is not Undef:
        result.append("ON")
        result.append(compile(state, expr.on))
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
    state.precedence += 0.5
    expr2 = compile(state, oper.expr2)
    return "%s%s%s" % (expr1, oper.oper, expr2)


class CompoundOper(CompoundExpr):
    oper = " (unknown) "

@compile.when(CompoundOper)
def compile_compound_oper(compile, state, oper):
    return "%s" % compile(state, oper.exprs, oper.oper)

@compile_python.when(CompoundOper)
def compile_compound_oper(compile, state, oper):
    return "%s" % compile(state, oper.exprs, oper.oper.lower())


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


class And(CompoundOper):
    oper = " AND "

class Or(CompoundOper):
    oper = " OR "


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


# --------------------------------------------------------------------
# Functions

class Func(ComparableExpr):
    name = "(unknown)"

    def __init__(self, *args):
        self.args = args

@compile.when(Func)
def compile_func(compile, state, func):
    return "%s(%s)" % (func.name, compile(state, func.args))


class Count(Func):
    name = "COUNT"

@compile.when(Count)
def compile_count(compile, state, count):
    if count.args:
        return "COUNT(%s)" % compile(state, count.args)
    return "COUNT(*)"


class Max(Func):
    name = "MAX"

class Min(Func):
    name = "MIN"

class Avg(Func):
    name = "AVG"

class Sum(Func):
    name = "SUM"


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
    return "%s %s" % (compile(state, expr.expr), expr.suffix)


class Exists(PrefixExpr):
    prefix = "EXISTS"

class Asc(SuffixExpr):
    suffix = "ASC"

class Desc(SuffixExpr):
    suffix = "DESC"


# --------------------------------------------------------------------
# Plain SQL expressions.

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
# Set operator precedences.

compile.set_precedence(10, Select, Insert, Update, Delete)
compile.set_precedence(10, Join, LeftJoin, RightJoin)
compile.set_precedence(10, NaturalJoin, NaturalLeftJoin, NaturalRightJoin)
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
