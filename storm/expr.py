from copy import copy
import sys


# --------------------------------------------------------------------
# Basic compiler infrastructure

class CompileError(Exception):
    pass

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

    def _compile(self, state, expr, join=", "):
        if type(expr) in (tuple, list):
            compiled = []
            for subexpr in expr:
                if type(subexpr) in (tuple, list):
                    compiled.append(self._compile(state, subexpr, join))
                    continue
                for class_ in subexpr.__class__.__mro__:
                    handler = self._dispatch_table.get(class_)
                    if handler is not None:
                        compiled.append(handler(self._compile, state, subexpr))
                        break
                else:
                    raise CompileError("Don't know how to compile %r"
                                       % subexpr.__class__)
            return join.join(compiled)
        else:
            for class_ in expr.__class__.__mro__:
                handler = self._dispatch_table.get(class_)
                if handler is not None:
                    return handler(self._compile, state, expr)
            raise CompileError, "Don't know how to compile %r" % expr.__class__

    def __call__(self, expr):
        state = State()
        return self._compile(state, expr), state.parameters


Undef = object()

class State(object):

    def __init__(self):
        self._stack = []
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

    def __init__(self, columns, tables=Undef, where=Undef, order_by=Undef,
                 group_by=Undef, limit=Undef, offset=Undef, distinct=False):
        self.columns = columns
        self.tables = tables
        self.where = where
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
    
    tokens.append(compile(state, select.columns))

    if select.tables is not Undef:
        tokens.append(" FROM ")
        # Add a placeholder and compile later to support AutoTable.
        tables_pos = len(tokens)
        tokens.append(None)
    else:
        tables_pos = None
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
    if tables_pos is not None:
        tokens[tables_pos] = compile(state, select.tables)
    return "".join(tokens)


class Insert(Expr):

    def __init__(self, table, columns, values):
        self.table = table
        self.columns = columns
        self.values = values

@compile.when(Insert)
def compile_insert(compile, state, insert):
    state.push("omit_column_tables", True)
    columns = compile(state, insert.columns)
    state.pop()
    tokens = ["INSERT INTO ", compile(state, insert.table),
              " (", columns, ") VALUES (", compile(state, insert.values), ")"]
    return "".join(tokens)


class Update(Expr):

    def __init__(self, table, set, where=Undef):
        self.table = table
        self.set = set
        self.where = where

@compile.when(Update)
def compile_update(compile, state, update):
    state.push("omit_column_tables", True)
    set = update.set
    sets = ["%s=%s" % (compile(state, col), compile(state, set[col]))
            for col in set]
    state.pop()
    tokens = ["UPDATE ", compile(state, update.table),
              " SET ", ", ".join(sets)]
    if update.where is not Undef:
        tokens.append(" WHERE ")
        tokens.append(compile(state, update.where))
    return "".join(tokens)


class Delete(Expr):

    def __init__(self, table, where=Undef):
        self.table = table
        self.where = where

@compile.when(Delete)
def compile_delete(compile, state, delete):
    tokens = ["DELETE FROM ", None]
    if delete.where is not Undef:
        tokens.append(" WHERE ")
        tokens.append(compile(state, delete.where))
    # Compile later for AutoTable support.
    tokens[1] = compile(state, delete.table)
    return "".join(tokens)


# --------------------------------------------------------------------
# Columns and parameters

class Column(ComparableExpr):

    def __init__(self, name, table=Undef):
        self.name = name
        self.table = table

@compile.when(Column)
def compile_column(compile, state, column):
    if column.table is not Undef:
        state.auto_tables.append(column.table)
    if column.table is Undef or state.omit_column_tables:
        return column.name
    return "%s.%s" % (compile(state, column.table), column.name)


class AutoTable(Expr):

    def __init__(self, *default_tables):
        self.default_tables = default_tables

@compile.when(AutoTable)
def compile_auto_table(compile, state, auto_table):
    if state.auto_tables:
        tables = []
        for expr in state.auto_tables:
            table = compile(state, expr)
            if table not in tables:
                tables.append(table)
        return ", ".join(tables)
    elif auto_table.default_tables:
        return compile(state, auto_table.default_tables)
    raise CompileError("AutoTable can't provide any tables")


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
    return "(%s)" % compile(state, oper.exprs, oper.oper)


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
