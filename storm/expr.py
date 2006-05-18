from copy import copy
import sys


# --------------------------------------------------------------------
# Basic compiler infrastructure

class Compile(object):
    """Compiler based on the concept of generic functions."""

    def __init__(self):
        self._dispatch_table = {}

    def copy(self):
        copy = self.__class__()
        copy._dispatch_table = self._dispatch_table.copy()
        return copy

    def when(self, *types):
        def decorator(method):
            for type in types:
                self._dispatch_table[type] = method
            return method
        return decorator

    def _compile(self, state, expr):
        for class_ in expr.__class__.__mro__:
            handler = self._dispatch_table.get(class_)
            if handler is not None:
                return handler(self._compile, state, expr)
        raise RuntimeError, "Don't know how to compile %r" % expr.__class__

    def __call__(self, expr):
        state = State()
        return self._compile(state, expr), state.parameters


marker = object()

class State(object):

    def __init__(self):
        self._stack = []
        self.parameters = []
        self.tables = []
        self.omit_column_tables = False

    def push(self, attr, new_value=marker):
        old_value = getattr(self, attr, None)
        self._stack.append((attr, old_value))
        if new_value is marker:
            new_value = copy(old_value)
        setattr(self, attr, new_value)

    def pop(self):
        setattr(self, *self._stack.pop(-1))


compile = Compile()


# --------------------------------------------------------------------
# Builtin type support

@compile.when(str)
def compile_str(compile, state, expr):
    return expr

@compile.when(type(None))
def compile_none(compile, state, expr):
    return "NULL"


# --------------------------------------------------------------------
# Base classes for expressions

class Expr(object):
    pass

class Comparable(object):

    def __eq__(self, other):
        if not isinstance(other, Expr) and other is not None:
            other = Param(other)
        return Eq(self, other)

    def __ne__(self, other):
        if not isinstance(other, Expr) and other is not None:
            other = Param(other)
        return Ne(self, other)

    def __gt__(self, other):
        if not isinstance(other, Expr):
            other = Param(other)
        return Gt(self, other)

    def __ge__(self, other):
        if not isinstance(other, Expr):
            other = Param(other)
        return Ge(self, other)

    def __lt__(self, other):
        if not isinstance(other, Expr):
            other = Param(other)
        return Lt(self, other)

    def __le__(self, other):
        if not isinstance(other, Expr):
            other = Param(other)
        return Le(self, other)

    def __rshift__(self, other):
        if not isinstance(other, Expr):
            other = Param(other)
        return RShift(self, other)

    def __lshift__(self, other):
        if not isinstance(other, Expr):
            other = Param(other)
        return LShift(self, other)

    def __and__(self, other):
        if not isinstance(other, Expr):
            other = Param(other)
        return And(self, other)

    def __or__(self, other):
        if not isinstance(other, Expr):
            other = Param(other)
        return Or(self, other)

    def __add__(self, other):
        if not isinstance(other, Expr):
            other = Param(other)
        return Add(self, other)

    def __sub__(self, other):
        if not isinstance(other, Expr):
            other = Param(other)
        return Sub(self, other)

    def __mul__(self, other):
        if not isinstance(other, Expr):
            other = Param(other)
        return Mul(self, other)

    def __div__(self, other):
        if not isinstance(other, Expr):
            other = Param(other)
        return Div(self, other)
    
    def __mod__(self, other):
        if not isinstance(other, Expr):
            other = Param(other)
        return Mod(self, other)

class ComparableExpr(Expr, Comparable):
    pass

#class UnaryExpr(ComparableExpr):
#
#    def __init__(self, expr):
#        self.expr = expr

class BinaryExpr(ComparableExpr):

    def __init__(self, expr1, expr2):
        self.expr1 = expr1
        self.expr2 = expr2

class CompoundExpr(ComparableExpr):

    def __init__(self, *exprs):
        self.exprs = exprs


# --------------------------------------------------------------------
# Statement expressions

class Select(Expr):

    def __init__(self, columns=None, tables=None, where=None,
                 order_by=None, group_by=None, limit=None, offset=None):
        self.columns = tuple(columns or ())
        self.tables = tuple(tables or ())
        self.where = where
        self.order_by = tuple(order_by or ())
        self.group_by = tuple(group_by or ())
        self.limit = limit
        self.offset = offset

@compile.when(Select)
def compile_select(compile, state, select):
    tokens = ["SELECT ", ", ".join([compile(state, expr)
                                    for expr in select.columns])]
    if select.tables:
        tokens.append(" FROM ")
        tokens.append(", ".join([compile(state, expr)
                                 for expr in select.tables]))
    if select.where:
        tokens.append(" WHERE ")
        tokens.append(compile(state, select.where))
    if select.order_by:
        tokens.append(" ORDER BY ")
        tokens.append(", ".join([compile(state, expr)
                                 for expr in select.order_by]))
    if select.group_by:
        tokens.append(" GROUP BY ")
        tokens.append(", ".join([compile(state, expr)
                                 for expr in select.group_by]))
    if select.limit:
        tokens.append(" LIMIT %d" % select.limit)
    if select.offset:
        tokens.append(" OFFSET %d" % select.offset)
    return "".join(tokens)


class Insert(Expr):

    def __init__(self, table=None, columns=None, values=None):
        self.table = table
        self.columns = tuple(columns or ())
        self.values = tuple(values or ())

@compile.when(Insert)
def compile_insert(compile, state, insert):
    state.push("omit_column_tables", True)
    columns = ", ".join([compile(state, expr) for expr in insert.columns])
    state.pop()
    tokens = ["INSERT INTO ",
              compile(state, insert.table),
              " (", columns, ") VALUES (",
              ", ".join([compile(state, expr) for expr in insert.values]),
              ")"]
    return "".join(tokens)


class Update(Expr):

    def __init__(self, table=None, columns=None, values=None, where=None):
        self.table = table
        self.columns = tuple(columns or ())
        self.values = tuple(values or ())
        self.where = where

@compile.when(Update)
def compile_update(compile, state, update):
    state.push("omit_column_tables", True)
    columns = [compile(state, expr) for expr in update.columns]
    state.pop()
    values = [compile(state, expr) for expr in update.values]
    tokens = ["UPDATE ", compile(state, update.table),
              " SET ", ", ".join(["%s=%s" % x for x in zip(columns, values)])]
    if update.where:
        tokens.append(" WHERE ")
        tokens.append(compile(state, update.where))
    return "".join(tokens)


class Delete(Expr):

    def __init__(self, table=None, where=None):
        self.table = table
        self.where = where

@compile.when(Delete)
def compile_delete(compile, state, delete):
    tokens = ["DELETE FROM ", compile(state, delete.table)]
    if delete.where:
        tokens.append(" WHERE ")
        tokens.append(compile(state, delete.where))
    return "".join(tokens)


# --------------------------------------------------------------------
# Columns and parameters

class Column(ComparableExpr):

    def __init__(self, name=None, table=None):
        self.name = name
        self.table = table

@compile.when(Column)
def compile_column(compile, state, column):
    if not column.table or state.omit_column_tables:
        return column.name
    return "%s.%s" % (compile(state, column.table), column.name)


class Param(ComparableExpr):

    def __init__(self, value):
        self.value = value

@compile.when(Param)
def compile_param(compile, state, param):
    state.parameters.append(param.value)
    return "?"


# --------------------------------------------------------------------
# Operators

#class UnaryOper(UnaryExpr):
#    oper = " (unknown) "


class BinaryOper(BinaryExpr):
    oper = " (unknown) "

@compile.when(BinaryOper)
def compile_binary_oper(compile, state, oper):
    return "(%s%s%s)" % (compile(state, oper.expr1), oper.oper,
                         compile(state, oper.expr2))


class CompoundOper(CompoundExpr):
    oper = " (unknown) "

@compile.when(CompoundOper)
def compile_compound_oper(compile, state, oper):
    return "(%s)" % oper.oper.join([compile(state, expr)
                                    for expr in oper.exprs])


class Eq(BinaryOper):
    oper = " = "

@compile.when(Eq)
def compile_eq(compile, state, eq):
    if eq.expr2 is None:
        return "(%s IS NULL)" % compile(state, eq.expr1)
    return "(%s = %s)" % (compile(state, eq.expr1), compile(state, eq.expr2))

class Ne(BinaryOper):
    oper = " != "

@compile.when(Ne)
def compile_ne(compile, state, ne):
    if ne.expr2 is None:
        return "(%s IS NOT NULL)" % compile(state, ne.expr1)
    return "(%s != %s)" % (compile(state, ne.expr1), compile(state, ne.expr2))


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


class And(CompoundOper):
    oper = " AND "

class Or(CompoundOper):
    oper = " OR "


class Add(CompoundOper):
    oper = "+"

class Sub(CompoundOper):
    oper = "-"

class Mul(CompoundOper):
    oper = "*"

class Div(CompoundOper):
    oper = "/"

class Mod(CompoundOper):
    oper = "%"


# --------------------------------------------------------------------
# Functions

class Func(ComparableExpr):
    name = "(unknown)"

    def __init__(self, *args):
        self.args = args

@compile.when(Func)
def compile_func(compile, state, func):
    return "%s(%s)" % (func.name, ", ".join([compile(state, expr)
                                             for expr in func.args]))


class Count(Func):
    name = "COUNT"

@compile.when(Count)
def compile_count(compile, state, count):
    if count.args:
        return "COUNT(%s)" % ", ".join([compile(state, expr)
                                        for expr in count.args])
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
# Suffix expressions

class SuffixExpr(Expr):
    suffix = "(unknown)"

    def __init__(self, expr):
        self.expr = expr

@compile.when(SuffixExpr)
def compile_suffix_expr(compile, state, expr):
    return "%s %s" % (compile(state, expr.expr), expr.suffix)


class Asc(SuffixExpr):
    suffix = "ASC"

class Desc(SuffixExpr):
    suffix = "DESC"
