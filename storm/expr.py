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

    def _compile(self, expr, parameters):
        for class_ in expr.__class__.__mro__:
            handler = self._dispatch_table.get(class_)
            if handler is not None:
                return handler(self._compile, expr, parameters)
        raise RuntimeError, "Don't know how to compile %r" % expr.__class__

    def __call__(self, expr):
        parameters = []
        return self._compile(expr, parameters), parameters


compile = Compile()


# --------------------------------------------------------------------
# Builtin type support

@compile.when(str)
def compile_str(compile, expr, parameters):
    return expr

@compile.when(type(None))
def compile_none(compile, expr, parameters):
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
def compile_select(compile, select, parameters):
    tokens = ["SELECT ", ", ".join([compile(expr, parameters)
                                    for expr in select.columns])]
    if select.tables:
        tokens.append(" FROM ")
        tokens.append(", ".join([compile(expr, parameters)
                                 for expr in select.tables]))
    if select.where:
        tokens.append(" WHERE ")
        tokens.append(compile(select.where, parameters))
    if select.order_by:
        tokens.append(" ORDER BY ")
        tokens.append(", ".join([compile(expr, parameters)
                                 for expr in select.order_by]))
    if select.group_by:
        tokens.append(" GROUP BY ")
        tokens.append(", ".join([compile(expr, parameters)
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
def compile_insert(compile, insert, parameters):
    tokens = ["INSERT INTO ",
              compile(insert.table, parameters),
              " (",
              ", ".join([compile(expr, parameters)
                         for expr in insert.columns]),
              ") VALUES (",
              ", ".join([compile(expr, parameters)
                         for expr in insert.values]),
              ")"]
    return "".join(tokens)


class Update(Expr):

    def __init__(self, table=None, sets=None, where=None):
        self.table = table
        self.sets = tuple(sets or ())
        self.where = where

@compile.when(Update)
def compile_update(compile, update, parameters):
    tokens = ["UPDATE ", compile(update.table, parameters),
              " SET ", ", ".join(["%s=%s" % (compile(expr1, parameters),
                                             compile(expr2, parameters))
                                  for (expr1, expr2) in update.sets])]
    if update.where:
        tokens.append(" WHERE ")
        tokens.append(compile(update.where, parameters))
    return "".join(tokens)


class Delete(Expr):

    def __init__(self, table=None, where=None):
        self.table = table
        self.where = where

@compile.when(Delete)
def compile_delete(compile, delete, parameters):
    tokens = ["DELETE FROM ", compile(delete.table, parameters)]
    if delete.where:
        tokens.append(" WHERE ")
        tokens.append(compile(delete.where, parameters))
    return "".join(tokens)


# --------------------------------------------------------------------
# Columns and parameters

class Column(ComparableExpr):

    def __init__(self, name=None, table=None):
        self.name = name
        self.table = table

@compile.when(Column)
def compile_column(compile, column, parameters):
    if not column.table:
        return column.name
    return "%s.%s" % (compile(column.table, parameters), column.name)


class Param(ComparableExpr):

    def __init__(self, value):
        self.value = value

@compile.when(Param)
def compile_param(compile, param, parameters):
    parameters.append(param.value)
    return "?"


# --------------------------------------------------------------------
# Operators

#class UnaryOper(UnaryExpr):
#    oper = " (unknown) "


class BinaryOper(BinaryExpr):
    oper = " (unknown) "

@compile.when(BinaryOper)
def compile_binary_oper(compile, oper, parameters):
    return "(%s%s%s)" % (compile(oper.expr1, parameters), oper.oper,
                         compile(oper.expr2, parameters))


class CompoundOper(CompoundExpr):
    oper = " (unknown) "

@compile.when(CompoundOper)
def compile_compound_oper(compile, oper, parameters):
    return "(%s)" % oper.oper.join([compile(expr, parameters)
                                    for expr in oper.exprs])


class Eq(BinaryOper):
    oper = " = "

@compile.when(Eq)
def compile_eq(compile, eq, parameters):
    if eq.expr2 is None:
        return "(%s IS NULL)" % compile(eq.expr1, parameters)
    return "(%s = %s)" % (compile(eq.expr1, parameters),
                          compile(eq.expr2, parameters))

class Ne(BinaryOper):
    oper = " != "

@compile.when(Ne)
def compile_ne(compile, ne, parameters):
    if ne.expr2 is None:
        return "(%s IS NOT NULL)" % compile(ne.expr1, parameters)
    return "(%s != %s)" % (compile(ne.expr1, parameters),
                           compile(ne.expr2, parameters))


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
def compile_func(compile, func, parameters):
    return "%s(%s)" % (func.name, ", ".join([compile(expr, parameters)
                                             for expr in func.args]))


class Count(Func):
    name = "COUNT"

@compile.when(Count)
def compile_count(compile, count, parameters):
    if count.args:
        return "COUNT(%s)" % ", ".join([compile(expr, parameters)
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
def compile_suffix_expr(compile, expr, parameters):
    return "%s %s" % (compile(expr.expr, parameters), expr.suffix)


class Asc(SuffixExpr):
    suffix = "ASC"

class Desc(SuffixExpr):
    suffix = "DESC"
