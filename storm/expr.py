import sys


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


class Expr(object):
    pass

class ComparableExpr(Expr, Comparable):
    pass


class UnaryExpr(ComparableExpr):

    def __init__(self, expr):
        self.expr = expr

class BinaryExpr(ComparableExpr):

    def __init__(self, expr1, expr2):
        self.expr1 = expr1
        self.expr2 = expr2

class CompoundExpr(ComparableExpr):

    def __init__(self, *exprs):
        self.exprs = exprs


class UnaryOper(UnaryExpr):
    oper = " (unknown) "

class BinaryOper(BinaryExpr):
    oper = " (unknown) "

class CompoundOper(CompoundExpr):
    oper = " (unknown) "


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

class Insert(Expr):

    def __init__(self, table=None, columns=None, values=None):
        self.table = table
        self.columns = tuple(columns or ())
        self.values = tuple(values or ())

class Update(Expr):

    def __init__(self, table=None, sets=None, where=None):
        self.table = table
        self.sets = tuple(sets or ())
        self.where = where

class Delete(Expr):

    def __init__(self, table=None, where=None):
        self.table = table
        self.where = where


class Column(ComparableExpr):

    def __init__(self, name=None, table=None):
        self.name = name
        self.table = table

class Param(ComparableExpr):

    def __init__(self, value):
        self.value = value

class Func(ComparableExpr):
    name = "(unknown)"

    def __init__(self, *args):
        self.args = args


class Eq(BinaryOper):
    oper = " = "

class Ne(BinaryOper):
    oper = " != "

class Gt(BinaryOper):
    oper = " > "

class Ge(BinaryOper):
    oper = " >= "

class Lt(BinaryOper):
    oper = " < "

class Le(BinaryOper):
    oper = " <= "

class Like(BinaryOper):
    oper = " LIKE "

class RShift(BinaryOper):
    oper = ">>"

class LShift(BinaryOper):
    oper = "<<"


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


class Count(Func):
    name = "COUNT"

class Max(Func):
    name = "MAX"

class Min(Func):
    name = "MIN"

class Avg(Func):
    name = "AVG"

class Sum(Func):
    name = "SUM"


class SuffixExpr(Expr):
    suffix = "(unknown)"

    def __init__(self, expr):
        self.expr = expr

class Asc(SuffixExpr):
    suffix = "ASC"

class Desc(SuffixExpr):
    suffix = "DESC"


def used_for(*types):
    """Convenience decorator to fill the dispatch table.

    Subclasses of Compiler don't need it.
    """
    dispatch_table = sys._getframe(1).f_locals["_dispatch_table"]
    def decorator(method):
        for type in types:
            dispatch_table[type] = method
        return method
    return decorator


class Compiler(object):

    _dispatch_table = {}

    def compile(self, expr):
        parameters = []
        return self._compile(expr, parameters), parameters

    def _compile(self, expr, parameters):
        for class_ in expr.__class__.__mro__:
            handler = self._dispatch_table.get(class_)
            if handler is not None:
                return handler(self, expr, parameters)
        raise RuntimeError, "Don't know how to compile %r" % expr.__class__

    @used_for(str)
    def compile_literal(self, literal, parameters):
        return literal

    @used_for(Select)
    def compile_select(self, select, parameters):
        tokens = ["SELECT ", ", ".join([self._compile(expr, parameters)
                                        for expr in select.columns])]
        if select.tables:
            tokens.append(" FROM ")
            tokens.append(", ".join([self._compile(expr, parameters)
                                     for expr in select.tables]))
        if select.where:
            tokens.append(" WHERE ")
            tokens.append(self._compile(select.where, parameters))
        if select.order_by:
            tokens.append(" ORDER BY ")
            tokens.append(", ".join([self._compile(expr, parameters)
                                     for expr in select.order_by]))
        if select.group_by:
            tokens.append(" GROUP BY ")
            tokens.append(", ".join([self._compile(expr, parameters)
                                     for expr in select.group_by]))
        if select.limit:
            tokens.append(" LIMIT %d" % select.limit)
        if select.offset:
            tokens.append(" OFFSET %d" % select.offset)
        return "".join(tokens)

    @used_for(Insert)
    def compile_insert(self, insert, parameters):
        tokens = ["INSERT INTO ",
                  self._compile(insert.table, parameters),
                  " (",
                  ", ".join([self._compile(expr, parameters)
                             for expr in insert.columns]),
                  ") VALUES (",
                  ", ".join([self._compile(expr, parameters)
                             for expr in insert.values]),
                  ")"]
        return "".join(tokens)

    @used_for(Update)
    def compile_update(self, update, parameters):
        tokens = ["UPDATE ",
                  self._compile(update.table, parameters),
                  " SET ",
                  ", ".join(["%s=%s" % (self._compile(expr1, parameters),
                                        self._compile(expr2, parameters))
                             for (expr1, expr2) in update.sets])]
        if update.where:
            tokens.append(" WHERE ")
            tokens.append(self._compile(update.where, parameters))
        return "".join(tokens)

    @used_for(Delete)
    def compile_delete(self, delete, parameters):
        tokens = ["DELETE FROM ",
                  self._compile(delete.table, parameters)]
        if delete.where:
            tokens.append(" WHERE ")
            tokens.append(self._compile(delete.where, parameters))
        return "".join(tokens)

    @used_for(Column)
    def compile_column(self, column, parameters):
        if not column.table:
            return column.name
        return "%s.%s" % (self._compile(column.table, parameters), column.name)

    @used_for(Param)
    def compile_param(self, param, parameters):
        parameters.append(param.value)
        return "?"

    @used_for(Func)
    def compile_func(self, func, parameters):
        return "%s(%s)" % (func.name,
                           ", ".join([self._compile(expr, parameters)
                                      for expr in func.args]))

    @used_for(Count)
    def compile_count(self, count, parameters):
        if count.args:
            return "COUNT(%s)" % (", ".join([self._compile(expr, parameters)
                                             for expr in count.args]))
        else:
            return "COUNT(*)"

    @used_for(type(None))
    def compile_none(self, none, parameters):
        return "NULL"

    @used_for(BinaryOper)
    def compile_binary_oper(self, oper, parameters):
        return "(%s%s%s)" % \
               (self._compile(oper.expr1, parameters), oper.oper,
                self._compile(oper.expr2, parameters))

    @used_for(CompoundOper)
    def compile_compound_oper(self, oper, parameters):
        return "(%s)" % \
               oper.oper.join([self._compile(expr, parameters)
                               for expr in oper.exprs])

    @used_for(SuffixExpr)
    def compile_suffix_expr(self, expr, parameters):
        return "%s %s" % (self._compile(expr.expr, parameters), expr.suffix)

    @used_for(Eq)
    def compile_eq(self, eq, parameters):
        if eq.expr2 is None:
            return "(%s IS NULL)" % self._compile(eq.expr1, parameters)
        else:
            return "(%s = %s)" % (self._compile(eq.expr1, parameters),
                                  self._compile(eq.expr2, parameters))

    @used_for(Ne)
    def compile_eq(self, eq, parameters):
        if eq.expr2 is None:
            return "(%s IS NOT NULL)" % self._compile(eq.expr1, parameters)
        else:
            return "(%s != %s)" % (self._compile(eq.expr1, parameters),
                                  self._compile(eq.expr2, parameters))
