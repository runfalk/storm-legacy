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
from __future__ import division

import pytest

from decimal import Decimal

from storm.compat import is_python2, iter_range, long_int
from storm.variables import *
from storm.expr import *
from tests.helper import assert_variables_equal, TestHelper


class Func1(NamedFunc):
    name = "func1"

    if not is_python2:
        def __hash__(self):
            return id(self)


class Func2(NamedFunc):
    name = "func2"


class TrackContext(FromExpr):
    context = None


@compile.when(TrackContext)
def compile_track_context(compile, expr, state):
    expr.context = state.context
    return ""


def track_contexts(n):
    return [TrackContext() for i in iter_range(n)]


# Create columnN, tableN, and elemN variables.
for i in iter_range(10):
    for name in ["column", "elem"]:
        exec("%s%d = SQLToken('%s%d')" % (name, i, name, i))
    for name in ["table"]:
        exec("%s%d = '%s %d'" % (name, i, name, i))


@pytest.fixture
def state():
    return State()


@pytest.mark.parametrize("attr, value", [
    ("columns", ()),
    ("where", Undef),
    ("tables", Undef),
    ("default_tables", Undef),
    ("order_by", Undef),
    ("group_by", Undef),
    ("limit", Undef),
    ("offset", Undef),
    ("distinct", False),
])
def test_expr_select_default(attr, value):
    expr = Select(())
    assert getattr(expr, attr) == value



def test_expr_select_constructor():
    objects = [object() for i in iter_range(9)]
    expr = Select(*objects)
    assert expr.columns == objects[0]
    assert expr.where == objects[1]
    assert expr.tables == objects[2]
    assert expr.default_tables == objects[3]
    assert expr.order_by == objects[4]
    assert expr.group_by == objects[5]
    assert expr.limit == objects[6]
    assert expr.offset == objects[7]
    assert expr.distinct == objects[8]


def test_expr_insert_default():
    expr = Insert(None)
    assert expr.map == None
    assert expr.table == Undef
    assert expr.default_table == Undef
    assert expr.primary_columns == Undef
    assert expr.primary_variables == Undef


def test_expr_insert_constructor():
    objects = [object() for i in iter_range(5)]
    expr = Insert(*objects)
    assert expr.map == objects[0]
    assert expr.table == objects[1]
    assert expr.default_table == objects[2]
    assert expr.primary_columns == objects[3]
    assert expr.primary_variables == objects[4]


def test_expr_update_default():
    expr = Update(None)
    assert expr.map == None
    assert expr.where == Undef
    assert expr.table == Undef
    assert expr.default_table == Undef


def test_expr_update_constructor():
    objects = [object() for i in iter_range(4)]
    expr = Update(*objects)
    assert expr.map == objects[0]
    assert expr.where == objects[1]
    assert expr.table == objects[2]
    assert expr.default_table == objects[3]


def test_expr_delete_default():
    expr = Delete()
    assert expr.where == Undef
    assert expr.table == Undef


def test_expr_delete_constructor():
    objects = [object() for i in iter_range(3)]
    expr = Delete(*objects)
    assert expr.where == objects[0]
    assert expr.table == objects[1]
    assert expr.default_table == objects[2]


def test_expr_and():
    expr = And(elem1, elem2, elem3)
    assert expr.exprs == (elem1, elem2, elem3)


def test_expr_or():
    expr = Or(elem1, elem2, elem3)
    assert expr.exprs == (elem1, elem2, elem3)


def test_expr_column_default():
    expr = Column()
    assert expr.name == Undef
    assert expr.table == Undef
    assert expr.compile_cache is None

    # Test for identity. We don't want False there.
    assert expr.primary is 0

    assert expr.variable_factory == Variable


def test_expr_column_constructor():
    objects = [object() for i in iter_range(3)]
    objects.insert(2, True)
    expr = Column(*objects)
    assert expr.name == objects[0]
    assert expr.table == objects[1]

    # Test for identity. We don't want True there either.
    assert expr.primary is 1

    assert expr.variable_factory == objects[3]


def test_expr_func():
    expr = Func("myfunc", elem1, elem2)
    assert expr.name == "myfunc"
    assert expr.args == (elem1, elem2)


def test_expr_named_func():
    class MyFunc(NamedFunc):
        name = "myfunc"
    expr = MyFunc(elem1, elem2)
    assert expr.name == "myfunc"
    assert expr.args == (elem1, elem2)


def test_expr_like():
    expr = Like(elem1, elem2)
    assert expr.expr1 == elem1
    assert expr.expr2 == elem2


def test_expr_like_escape():
    expr = Like(elem1, elem2, elem3)
    assert expr.expr1 == elem1
    assert expr.expr2 == elem2
    assert expr.escape == elem3


def test_expr_like_case():
    expr = Like(elem1, elem2, elem3)
    assert expr.case_sensitive == None
    expr = Like(elem1, elem2, elem3, True)
    assert expr.case_sensitive == True
    expr = Like(elem1, elem2, elem3, False)
    assert expr.case_sensitive == False


def test_expr_startswith():
    expr = Func1()
    with pytest.raises(ExprError):
        expr.startswith(b"not a unicode string")

    like_expr = expr.startswith(u"abc!!_%")
    assert isinstance(like_expr, Like)
    assert like_expr.expr1 is expr
    assert like_expr.expr2 == u"abc!!!!!_!%%"
    assert like_expr.escape == u"!"


def test_expr_endswith():
    expr = Func1()
    with pytest.raises(ExprError):
        expr.endswith(b"not a unicode string")

    like_expr = expr.endswith(u"abc!!_%")
    assert isinstance(like_expr, Like)
    assert like_expr.expr1 is expr
    assert like_expr.expr2 == u"%abc!!!!!_!%"
    assert like_expr.escape == u"!"


def test_expr_contains_string():
    expr = Func1()
    with pytest.raises(ExprError):
        expr.contains_string(b"not a unicode string")

    like_expr = expr.contains_string(u"abc!!_%")
    assert isinstance(like_expr, Like)
    assert like_expr.expr1 is expr
    assert like_expr.expr2 == u"%abc!!!!!_!%%"
    assert like_expr.escape == u"!"


def test_expr_eq():
    expr = Eq(elem1, elem2)
    assert expr.expr1 == elem1
    assert expr.expr2 == elem2


def test_expr_sql_default():
    expr = SQL(None)
    assert expr.expr == None
    assert expr.params == Undef
    assert expr.tables == Undef


def test_expr_sql_constructor():
    objects = [object() for i in iter_range(3)]
    expr = SQL(*objects)
    assert expr.expr == objects[0]
    assert expr.params == objects[1]
    assert expr.tables == objects[2]


def test_expr_join_expr_right():
    expr = JoinExpr(None)
    assert expr.right == None
    assert expr.left == Undef
    assert expr.on == Undef


def test_expr_join_expr_on():
    on = Expr()
    expr = JoinExpr(None, on)
    assert expr.right == None
    assert expr.left == Undef
    assert expr.on == on


def test_expr_join_expr_on_keyword():
    on = Expr()
    expr = JoinExpr(None, on=on)
    assert expr.right == None
    assert expr.left == Undef
    assert expr.on == on


def test_expr_join_expr_on_invalid():
    on = Expr()
    with pytest.raises(ExprError):
        JoinExpr(None, on, None)


def test_expr_join_expr_right_left():
    objects = [object() for i in iter_range(2)]
    expr = JoinExpr(*objects)
    assert expr.left == objects[0]
    assert expr.right == objects[1]
    assert expr.on == Undef


def test_expr_join_expr_right_left_on():
    objects = [object() for i in iter_range(3)]
    expr = JoinExpr(*objects)
    assert expr.left == objects[0]
    assert expr.right == objects[1]
    assert expr.on == objects[2]


def test_expr_join_expr_right_join():
    join = JoinExpr(None)
    expr = JoinExpr(None, join)
    assert expr.right == join
    assert expr.left == None
    assert expr.on == Undef


def test_expr_table():
    objects = [object() for i in iter_range(1)]
    expr = Table(*objects)
    assert expr.name == objects[0]


def test_expr_alias_default():
    expr = Alias(None)
    assert expr.expr == None
    assert isinstance(expr.name, str)


def test_expr_alias_constructor():
    objects = [object() for i in iter_range(2)]
    expr = Alias(*objects)
    assert expr.expr == objects[0]
    assert expr.name == objects[1]


def test_expr_union():
    expr = Union(elem1, elem2, elem3)
    assert expr.exprs == (elem1, elem2, elem3)


def test_expr_union_with_kwargs():
    expr = Union(elem1, elem2, all=True, order_by=(), limit=1, offset=2)
    assert expr.exprs == (elem1, elem2)
    assert expr.all == True
    assert expr.order_by == ()
    assert expr.limit == 1
    assert expr.offset == 2


def test_expr_union_collapse():
    expr = Union(Union(elem1, elem2), elem3)
    assert expr.exprs == (elem1, elem2, elem3)

    # Only first expression is collapsed.
    expr = Union(elem1, Union(elem2, elem3))
    assert expr.exprs[0] == elem1
    assert isinstance(expr.exprs[1], Union)

    # Don't collapse if all is different.
    expr = Union(Union(elem1, elem2, all=True), elem3)
    assert isinstance(expr.exprs[0], Union)
    expr = Union(Union(elem1, elem2), elem3, all=True)
    assert isinstance(expr.exprs[0], Union)
    expr = Union(Union(elem1, elem2, all=True), elem3, all=True)
    assert expr.exprs == (elem1, elem2, elem3)

    # Don't collapse if limit or offset are set.
    expr = Union(Union(elem1, elem2, limit=1), elem3)
    assert isinstance(expr.exprs[0], Union)
    expr = Union(Union(elem1, elem2, offset=3), elem3)
    assert isinstance(expr.exprs[0], Union)

    # Don't collapse other set expressions.
    expr = Union(Except(elem1, elem2), elem3)
    assert isinstance(expr.exprs[0], Except)
    expr = Union(Intersect(elem1, elem2), elem3)
    assert isinstance(expr.exprs[0], Intersect)


def test_expr_except():
    expr = Except(elem1, elem2, elem3)
    assert expr.exprs == (elem1, elem2, elem3)


def test_expr_except_with_kwargs():
    expr = Except(elem1, elem2, all=True, order_by=(), limit=1, offset=2)
    assert expr.exprs == (elem1, elem2)
    assert expr.all == True
    assert expr.order_by == ()
    assert expr.limit == 1
    assert expr.offset == 2


def test_expr_except_collapse():
    expr = Except(Except(elem1, elem2), elem3)
    assert expr.exprs == (elem1, elem2, elem3)

    # Only first expression is collapsed.
    expr = Except(elem1, Except(elem2, elem3))
    assert expr.exprs[0] == elem1
    assert isinstance(expr.exprs[1], Except)

    # Don't collapse if all is different.
    expr = Except(Except(elem1, elem2, all=True), elem3)
    assert isinstance(expr.exprs[0], Except)
    expr = Except(Except(elem1, elem2), elem3, all=True)
    assert isinstance(expr.exprs[0], Except)
    expr = Except(Except(elem1, elem2, all=True), elem3, all=True)
    assert expr.exprs == (elem1, elem2, elem3)

    # Don't collapse if limit or offset are set.
    expr = Except(Except(elem1, elem2, limit=1), elem3)
    assert isinstance(expr.exprs[0], Except)
    expr = Except(Except(elem1, elem2, offset=3), elem3)
    assert isinstance(expr.exprs[0], Except)

    # Don't collapse other set expressions.
    expr = Except(Union(elem1, elem2), elem3)
    assert isinstance(expr.exprs[0], Union)
    expr = Except(Intersect(elem1, elem2), elem3)
    assert isinstance(expr.exprs[0], Intersect)


def test_expr_intersect():
    expr = Intersect(elem1, elem2, elem3)
    assert expr.exprs == (elem1, elem2, elem3)


def test_expr_intersect_with_kwargs():
    expr = Intersect(
        elem1, elem2, all=True, order_by=(), limit=1, offset=2)
    assert expr.exprs == (elem1, elem2)
    assert expr.all == True
    assert expr.order_by == ()
    assert expr.limit == 1
    assert expr.offset == 2


def test_expr_intersect_collapse():
    expr = Intersect(Intersect(elem1, elem2), elem3)
    assert expr.exprs == (elem1, elem2, elem3)

    # Only first expression is collapsed.
    expr = Intersect(elem1, Intersect(elem2, elem3))
    assert expr.exprs[0] == elem1
    assert isinstance(expr.exprs[1], Intersect)

    # Don't collapse if all is different.
    expr = Intersect(Intersect(elem1, elem2, all=True), elem3)
    assert isinstance(expr.exprs[0], Intersect)
    expr = Intersect(Intersect(elem1, elem2), elem3, all=True)
    assert isinstance(expr.exprs[0], Intersect)
    expr = Intersect(Intersect(elem1, elem2, all=True), elem3, all=True)
    assert expr.exprs == (elem1, elem2, elem3)

    # Don't collapse if limit or offset are set.
    expr = Intersect(Intersect(elem1, elem2, limit=1), elem3)
    assert isinstance(expr.exprs[0], Intersect)
    expr = Intersect(Intersect(elem1, elem2, offset=3), elem3)
    assert isinstance(expr.exprs[0], Intersect)

    # Don't collapse other set expressions.
    expr = Intersect(Union(elem1, elem2), elem3)
    assert isinstance(expr.exprs[0], Union)
    expr = Intersect(Except(elem1, elem2), elem3)
    assert isinstance(expr.exprs[0], Except)


def test_expr_auto_tables():
    expr = AutoTables(elem1, [elem2])
    assert expr.expr == elem1
    assert expr.tables == [elem2]


def test_expr_sequence():
    expr = Sequence(elem1)
    assert expr.name == elem1


def test_state_attrs(state):
    assert state.parameters == []
    assert state.auto_tables == []
    assert state.context == None


def test_state_push_pop(state):
    state.parameters.extend([1, 2])
    state.push("parameters", [])
    assert state.parameters == []
    state.pop()
    assert state.parameters == [1, 2]
    state.push("parameters")
    assert state.parameters == [1, 2]
    state.parameters.append(3)
    assert state.parameters == [1, 2, 3]
    state.pop()
    assert state.parameters == [1, 2]


def test_state_push_pop_unexistent(state):
    state.push("nonexistent")
    assert state.nonexistent == None
    state.nonexistent = "something"
    state.pop()
    assert state.nonexistent == None


def test_compile_simple_inheritance():
    custom_compile = compile.create_child()
    statement = custom_compile(Func1())
    assert statement == "func1()"


def test_compile_customize():
    custom_compile = compile.create_child()
    @custom_compile.when(type(None))
    def compile_none(compile, state, expr):
        return "None"
    statement = custom_compile(Func1(None))
    assert statement == "func1(None)"


def test_compile_customize_inheritance():
    class C(object): pass
    compile_parent = Compile()
    compile_child = compile_parent.create_child()

    @compile_parent.when(C)
    def compile_in_parent(compile, state, expr):
        return "parent"
    statement = compile_child(C())
    assert statement == "parent"

    @compile_child.when(C)
    def compile_in_child(compile, state, expr):
        return "child"
    statement = compile_child(C())
    assert statement == "child"


def test_compile_precedence():
    expr = And(
        SQLRaw(1),
        Or(SQLRaw(2), SQLRaw(3)),
        Add(
            SQLRaw(4),
            Mul(
                SQLRaw(5),
                Sub(SQLRaw(6), Div(SQLRaw(7), Div(SQLRaw(8), SQLRaw(9)))),
            ),
        ),
    )
    statement = compile(expr)
    assert statement == "1 AND (2 OR 3) AND 4+5*(6-7/(8/9))"

    expr = Func1(Select(Count()), [Select(Count())])
    statement = compile(expr)
    assert statement == "func1((SELECT COUNT(*)), (SELECT COUNT(*)))"


def test_compile_get_precedence():
    assert compile.get_precedence(Or) < compile.get_precedence(And)
    assert compile.get_precedence(Add) < compile.get_precedence(Mul)
    assert compile.get_precedence(Sub) < compile.get_precedence(Div)


def test_compile_customize_precedence():
    expr = And(elem1, Or(elem2, elem3))
    custom_compile = compile.create_child()
    custom_compile.set_precedence(10, And)

    custom_compile.set_precedence(11, Or)
    statement = custom_compile(expr)
    assert statement == "elem1 AND elem2 OR elem3"

    custom_compile.set_precedence(10, Or)
    statement = custom_compile(expr)
    assert statement == "elem1 AND elem2 OR elem3"

    custom_compile.set_precedence(9, Or)
    statement = custom_compile(expr)
    assert statement == "elem1 AND (elem2 OR elem3)"


def test_compile_customize_precedence_inheritance():
    compile_parent = compile.create_child()
    compile_child = compile_parent.create_child()

    expr = And(elem1, Or(elem2, elem3))

    compile_parent.set_precedence(10, And)

    compile_parent.set_precedence(11, Or)
    assert compile_child.get_precedence(Or) == 11
    assert compile_parent.get_precedence(Or) == 11
    statement = compile_child(expr)
    assert statement == "elem1 AND elem2 OR elem3"

    compile_parent.set_precedence(10, Or)
    assert compile_child.get_precedence(Or) == 10
    assert compile_parent.get_precedence(Or) == 10
    statement = compile_child(expr)
    assert statement == "elem1 AND elem2 OR elem3"

    compile_child.set_precedence(9, Or)
    assert compile_child.get_precedence(Or) == 9
    assert compile_parent.get_precedence(Or) == 10
    statement = compile_child(expr)
    assert statement == "elem1 AND (elem2 OR elem3)"


def test_compile_compile_sequence():
    expr = [elem1, Func1(), (Func2(), None)]
    statement = compile(expr)
    assert statement == "elem1, func1(), func2(), NULL"


def test_compile_compile_invalid():
    with pytest.raises(CompileError):
        compile(object())
    with pytest.raises(CompileError):
        compile([object()])


def test_compile_str(state):
    statement = compile(b"str", state)
    assert statement == "?"
    assert_variables_equal(state.parameters, [RawStrVariable(b"str")])


def test_compile_unicode(state):
    statement = compile(u"str", state)
    assert statement == "?"
    assert_variables_equal(state.parameters, [UnicodeVariable(u"str")])


def test_compile_int(state):
    statement = compile(1, state)
    assert statement == "?"
    assert_variables_equal(state.parameters, [IntVariable(1)])


def test_compile_long(state):
    statement = compile(1, state)
    assert statement == "?"
    assert_variables_equal(state.parameters, [IntVariable(1)])


def test_compile_bool(state):
    statement = compile(True, state)
    assert statement == "?"
    assert_variables_equal(state.parameters, [BoolVariable(1)])


def test_compile_float(state):
    statement = compile(1.1, state)
    assert statement == "?"
    assert_variables_equal(state.parameters, [FloatVariable(1.1)])


def test_compile_decimal(state):
    statement = compile(Decimal("1.1"), state)
    assert statement == "?"
    assert_variables_equal(
        state.parameters, [DecimalVariable(Decimal("1.1"))])


def test_compile_datetime(state):
    dt = datetime(1977, 5, 4, 12, 34)
    statement = compile(dt, state)
    assert statement == "?"
    assert_variables_equal(state.parameters, [DateTimeVariable(dt)])


def test_compile_date(state):
    d = date(1977, 5, 4)
    statement = compile(d, state)
    assert statement == "?"
    assert_variables_equal(state.parameters, [DateVariable(d)])


def test_compile_time(state):
    t = time(12, 34)
    statement = compile(t, state)
    assert statement == "?"
    assert_variables_equal(state.parameters, [TimeVariable(t)])


def test_compile_timedelta(state):
    td = timedelta(days=1, seconds=2, microseconds=3)
    statement = compile(td, state)
    assert statement == "?"
    assert_variables_equal(state.parameters, [TimeDeltaVariable(td)])


def test_compile_none(state):
    statement = compile(None, state)
    assert statement == "NULL"
    assert state.parameters == []


def test_compile_select(state):
    expr = Select([column1, column2])
    statement = compile(expr, state)
    assert statement == "SELECT column1, column2"
    assert state.parameters == []


def test_compile_select_distinct(state):
    expr = Select([column1, column2], Undef, [table1], distinct=True)
    statement = compile(expr, state)
    assert statement == 'SELECT DISTINCT column1, column2 FROM "table 1"'
    assert state.parameters == []


def test_compile_select_distinct_on(state):
    expr = Select([column1, column2], Undef, [table1],
                  distinct=[column2, column1])
    statement = compile(expr, state)
    assert statement == (
        'SELECT DISTINCT ON (column2, column1) '
        'column1, column2 FROM "table 1"'
    )
    assert state.parameters == []


def test_compile_select_where(state):
    expr = Select([column1, Func1()],
                  Func1(),
                  [table1, Func1()],
                  order_by=[column2, Func1()],
                  group_by=[column3, Func1()],
                  limit=3, offset=4)
    statement = compile(expr, state)
    assert statement == (
        'SELECT column1, func1() '
        'FROM "table 1", func1() '
        'WHERE func1() '
        'GROUP BY column3, func1() '
        'ORDER BY column2, func1() '
        'LIMIT 3 OFFSET 4'
    )
    assert state.parameters == []


def test_compile_select_join_where(state):
    expr = Select(column1,
                  Func1() == "value1",
                  Join(table1, Func2() == "value2"))
    statement = compile(expr, state)
    assert statement == (
        'SELECT column1 FROM '
        'JOIN "table 1" ON func2() = ? '
        'WHERE func1() = ?'
    )
    params = [variable.get() for variable in state.parameters]
    assert params == ["value2", "value1"]


def test_compile_select_auto_table(state):
    expr = Select(Column(column1, table1),
                  Column(column2, table2) == 1),
    statement = compile(expr, state)
    assert statement == (
        'SELECT "table 1".column1 '
        'FROM "table 1", "table 2" '
        'WHERE "table 2".column2 = ?'
    )
    assert_variables_equal(state.parameters, [Variable(1)])


def test_compile_select_auto_table_duplicated(state):
    expr = Select(Column(column1, table1),
                  Column(column2, table1) == 1),
    statement = compile(expr, state)
    assert statement == (
        'SELECT "table 1".column1 '
        'FROM "table 1" WHERE '
        '"table 1".column2 = ?'
    )
    assert_variables_equal(state.parameters, [Variable(1)])


def test_compile_select_auto_table_default(state):
    expr = Select(Column(column1),
                  Column(column2) == 1,
                  default_tables=table1),
    statement = compile(expr, state)
    assert statement == 'SELECT column1 FROM "table 1" WHERE column2 = ?'
    assert_variables_equal(state.parameters, [Variable(1)])


def test_compile_select_auto_table_default_with_joins(state):
    expr = Select(Column(column1),
                  default_tables=[table1, Join(table2)]),
    statement = compile(expr, state)
    assert statement == 'SELECT column1 FROM "table 1" JOIN "table 2"'
    assert state.parameters == []


def test_compile_select_auto_table_unknown():
    statement = compile(Select(elem1))
    assert statement == "SELECT elem1"


def test_compile_select_auto_table_sub():
    col1 = Column(column1, table1)
    col2 = Column(column2, table2)
    expr = Select(col1, In(elem1, Select(col2, col1 == col2, col2.table)))
    statement = compile(expr)
    assert statement == (
        'SELECT "table 1".column1 FROM "table 1" WHERE '
        'elem1 IN (SELECT "table 2".column2 FROM "table 2" '
        'WHERE "table 1".column1 = "table 2".column2)'
    )


def test_compile_select_join(state):
    expr = Select([column1, Func1()], Func1(),
                  [table1, Join(table2), Join(table3)])
    statement = compile(expr, state)
    assert statement == (
        'SELECT column1, func1() '
        'FROM "table 1" JOIN "table 2" '
        'JOIN "table 3" '
        'WHERE func1()'
    )
    assert state.parameters == []


def test_compile_select_join_right_left(state):
    expr = Select([column1, Func1()], Func1(),
                  [table1, Join(table2, table3)])
    statement = compile(expr, state)
    assert statement == (
        'SELECT column1, func1() '
        'FROM "table 1", "table 2" '
        'JOIN "table 3" WHERE func1()'
    )
    assert state.parameters == []


def test_compile_select_with_strings(state):
    expr = Select(column1, "1 = 2", table1, order_by="column1",
                  group_by="column2")
    statement = compile(expr, state)
    assert statement == ('SELECT column1 FROM "table 1" '
         'WHERE 1 = 2 GROUP BY column2 '
         'ORDER BY column1'
     )
    assert state.parameters == []


def test_compile_select_with_unicode(state):
    expr = Select(column1, u"1 = 2", table1, order_by=u"column1",
                  group_by=[u"column2"])
    statement = compile(expr, state)
    assert statement == (
        'SELECT column1 FROM "table 1" '
         'WHERE 1 = 2 GROUP BY column2 '
         'ORDER BY column1'
    )
    assert state.parameters == []


def test_compile_select_having(state):
    expr = Select(column1, tables=table1, order_by=u"column1",
                  group_by=[u"column2"], having=u"1 = 2")
    statement = compile(expr, state)
    assert statement == (
        'SELECT column1 FROM "table 1" '
         'GROUP BY column2 HAVING 1 = 2 '
         'ORDER BY column1'
    )
    assert state.parameters == []


def test_compile_select_contexts():
    column, where, table, order_by, group_by = track_contexts(5)
    expr = Select(column, where, table,
                  order_by=order_by, group_by=group_by)
    compile(expr)
    assert column.context == COLUMN
    assert where.context == EXPR
    assert table.context == TABLE
    assert order_by.context == EXPR
    assert group_by.context == EXPR


def test_compile_insert(state):
    expr = Insert({column1: elem1, Func1(): Func2()}, Func2())
    statement = compile(expr, state)
    assert statement in (
        "INSERT INTO func2() (column1, func1()) VALUES (elem1, func2())",
        "INSERT INTO func2() (func1(), column1) VALUES (func2(), elem1)",
    )
    assert state.parameters == []


def test_compile_insert_with_columns(state):
    expr = Insert({Column(column1, table1): elem1,
                   Column(column2, table1): elem2}, table2)
    statement = compile(expr, state)
    assert statement in (
        'INSERT INTO "table 2" (column1, column2) VALUES (elem1, elem2)',
        'INSERT INTO "table 2" (column2, column1) VALUES (elem2, elem1)',
    )
    assert state.parameters == []


def test_compile_insert_with_columns_to_escape(state):
    expr = Insert({Column("column 1", table1): elem1}, table2)
    statement = compile(expr, state)
    assert statement == 'INSERT INTO "table 2" ("column 1") VALUES (elem1)'
    assert state.parameters == []


def test_compile_insert_with_columns_as_raw_strings(state):
    expr = Insert({"column 1": elem1}, table2)
    statement = compile(expr, state)
    assert statement == 'INSERT INTO "table 2" ("column 1") VALUES (elem1)'
    assert state.parameters == []


def test_compile_insert_auto_table(state):
    expr = Insert({Column(column1, table1): elem1})
    statement = compile(expr, state)
    assert statement == 'INSERT INTO "table 1" (column1) VALUES (elem1)'
    assert state.parameters == []


def test_compile_insert_auto_table_default(state):
    expr = Insert({Column(column1): elem1}, default_table=table1)
    statement = compile(expr, state)
    assert statement == 'INSERT INTO "table 1" (column1) VALUES (elem1)'
    assert state.parameters == []


def test_compile_insert_auto_table_unknown():
    expr = Insert({Column(column1): elem1})
    with pytest.raises(NoTableError):
        compile(expr)


def test_compile_insert_contexts():
    column, value, table = track_contexts(3)
    expr = Insert({column: value}, table)
    compile(expr)
    assert column.context == COLUMN_NAME
    assert value.context == EXPR
    assert table.context == TABLE


def test_compile_insert_bulk(state):
    expr = Insert((Column(column1, table1), Column(column2, table1)),
                  values=[(elem1, elem2), (elem3, elem4)])
    statement = compile(expr, state)
    sql = (
        'INSERT INTO "table 1" (column1, column2) '
        'VALUES (elem1, elem2), (elem3, elem4)'
    )
    assert statement == sql
    assert state.parameters == []


def test_compile_insert_select(state):
    expr = Insert((Column(column1, table1), Column(column2, table1)),
                  values=Select(
                    (Column(column3, table3), Column(column4, table4))))
    statement = compile(expr, state)
    sql = (
        'INSERT INTO "table 1" (column1, column2) '
        'SELECT "table 3".column3, "table 4".column4 '
        'FROM "table 3", "table 4"'
    )
    assert statement == sql
    assert state.parameters == []


def test_compile_update(state):
    expr = Update({column1: elem1, Func1(): Func2()}, table=Func1())
    statement = compile(expr, state)
    assert statement in (
        "UPDATE func1() SET column1=elem1, func1()=func2()",
        "UPDATE func1() SET func1()=func2(), column1=elem1"
    )
    assert state.parameters == []


def test_compile_update_with_columns(state):
    expr = Update({Column(column1, table1): elem1}, table=table1)
    statement = compile(expr, state)
    assert statement == 'UPDATE "table 1" SET column1=elem1'
    assert state.parameters == []


def test_compile_update_with_columns_to_escape(state):
    expr = Update({Column("column x", table1): elem1}, table=table1)
    statement = compile(expr, state)
    assert statement == 'UPDATE "table 1" SET "column x"=elem1'
    assert state.parameters == []


def test_compile_update_with_columns_as_raw_strings(state):
    expr = Update({"column 1": elem1}, table=table2)
    statement = compile(expr, state)
    assert statement == 'UPDATE "table 2" SET "column 1"=elem1'
    assert state.parameters == []


def test_compile_update_where(state):
    expr = Update({column1: elem1}, Func1(), Func2())
    statement = compile(expr, state)
    assert statement == "UPDATE func2() SET column1=elem1 WHERE func1()"
    assert state.parameters == []


def test_compile_update_auto_table(state):
    expr = Update({Column(column1, table1): elem1})
    statement = compile(expr, state)
    assert statement == 'UPDATE "table 1" SET column1=elem1'
    assert state.parameters == []


def test_compile_update_auto_table_default(state):
    expr = Update({Column(column1): elem1}, default_table=table1)
    statement = compile(expr, state)
    assert statement == 'UPDATE "table 1" SET column1=elem1'
    assert state.parameters == []


def test_compile_update_auto_table_unknown():
    expr = Update({Column(column1): elem1})
    with pytest.raises(CompileError):
        compile(expr)


def test_compile_update_with_strings(state):
    expr = Update({column1: elem1}, "1 = 2", table1)
    statement = compile(expr, state)
    assert statement == 'UPDATE "table 1" SET column1=elem1 WHERE 1 = 2'
    assert state.parameters == []


def test_compile_update_contexts():
    set_left, set_right, where, table = track_contexts(4)
    expr = Update({set_left: set_right}, where, table)
    compile(expr)
    assert set_left.context == COLUMN_NAME
    assert set_right.context == COLUMN_NAME
    assert where.context == EXPR
    assert table.context == TABLE


def test_compile_delete(state):
    expr = Delete(table=table1)
    statement = compile(expr, state)
    assert statement == 'DELETE FROM "table 1"'
    assert state.parameters == []


def test_compile_delete_where(state):
    expr = Delete(Func1(), Func2())
    statement = compile(expr, state)
    assert statement == "DELETE FROM func2() WHERE func1()"
    assert state.parameters == []


def test_compile_delete_with_strings(state):
    expr = Delete("1 = 2", table1)
    statement = compile(expr, state)
    assert statement == 'DELETE FROM "table 1" WHERE 1 = 2'
    assert state.parameters == []


def test_compile_delete_auto_table(state):
    expr = Delete(Column(column1, table1) == 1)
    statement = compile(expr, state)
    assert statement == 'DELETE FROM "table 1" WHERE "table 1".column1 = ?'
    assert_variables_equal(state.parameters, [Variable(1)])


def test_compile_delete_auto_table_default(state):
    expr = Delete(Column(column1) == 1, default_table=table1)
    statement = compile(expr, state)
    assert statement == 'DELETE FROM "table 1" WHERE column1 = ?'
    assert_variables_equal(state.parameters, [Variable(1)])


def test_compile_delete_auto_table_unknown():
    expr = Delete(Column(column1) == 1)
    with pytest.raises(NoTableError):
        compile(expr)


def test_compile_delete_contexts():
    where, table = track_contexts(2)
    expr = Delete(where, table)
    compile(expr)
    assert where.context == EXPR
    assert table.context == TABLE


def test_compile_column(state):
    expr = Column(column1)
    statement = compile(expr, state)
    assert statement == "column1"
    assert state.parameters == []
    assert expr.compile_cache == "column1"


def test_compile_column_table(state):
    column = Column(column1, Func1())
    expr = Select(column)
    statement = compile(expr, state)
    assert statement == "SELECT func1().column1 FROM func1()"
    assert state.parameters == []
    assert column.compile_cache == "column1"


def test_compile_column_contexts():
    table, = track_contexts(1)
    expr = Column(column1, table)
    compile(expr)
    assert table.context == COLUMN_PREFIX


def test_compile_column_with_reserved_words(state):
    expr = Select(Column("name 1", "table 1"))
    statement = compile(expr, state)
    assert statement == 'SELECT "table 1"."name 1" FROM "table 1"'


def test_compile_row():
    expr = Row(column1, column2)
    statement = compile(expr)
    assert statement == "ROW(column1, column2)"


def test_compile_variable(state):
    expr = Variable("value")
    statement = compile(expr, state)
    assert statement == "?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_eq(state):
    expr = Eq(Func1(), Func2())
    statement = compile(expr, state)
    assert statement == "func1() = func2()"
    assert state.parameters == []


def test_compile_eq_param(state):
    expr = Func1() == "value"
    statement = compile(expr, state)
    assert statement == "func1() = ?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_is_in(state):
    expr = Func1().is_in(["Hello", "World"])
    statement = compile(expr, state)
    assert statement == "func1() IN (?, ?)"
    assert_variables_equal(
        state.parameters, [Variable("Hello"), Variable("World")])


def test_compile_is_in_empty(state):
    expr = Func1().is_in([])
    statement = compile(expr, state)
    assert statement == "?"
    assert_variables_equal(state.parameters, [BoolVariable(False)])


def test_compile_is_in_expr(state):
    expr = Func1().is_in(Select(column1))
    statement = compile(expr, state)
    assert statement == "func1() IN (SELECT column1)"
    assert state.parameters == []


def test_compile_eq_none(state):
    expr = Func1() == None

    assert expr.expr2 is None

    statement = compile(expr, state)
    assert statement == "func1() IS NULL"
    assert state.parameters == []


def test_compile_ne(state):
    expr = Ne(Func1(), Func2())
    statement = compile(expr, state)
    assert statement == "func1() != func2()"
    assert state.parameters == []


def test_compile_ne_param(state):
    expr = Func1() != "value"
    statement = compile(expr, state)
    assert statement == "func1() != ?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_ne_none(state):
    expr = Func1() != None
    assert expr.expr2 is None

    statement = compile(expr, state)
    assert statement == "func1() IS NOT NULL"
    assert state.parameters == []


def test_compile_gt(state):
    expr = Gt(Func1(), Func2())
    statement = compile(expr, state)
    assert statement == "func1() > func2()"
    assert state.parameters == []


def test_compile_gt_param(state):
    expr = Func1() > "value"
    statement = compile(expr, state)
    assert statement == "func1() > ?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_ge(state):
    expr = Ge(Func1(), Func2())
    statement = compile(expr, state)
    assert statement == "func1() >= func2()"
    assert state.parameters == []


def test_compile_ge_param(state):
    expr = Func1() >= "value"
    statement = compile(expr, state)
    assert statement == "func1() >= ?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_lt(state):
    expr = Lt(Func1(), Func2())
    statement = compile(expr, state)
    assert statement == "func1() < func2()"
    assert state.parameters == []


def test_compile_lt_param(state):
    expr = Func1() < "value"
    statement = compile(expr, state)
    assert statement == "func1() < ?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_le(state):
    expr = Le(Func1(), Func2())
    statement = compile(expr, state)
    assert statement == "func1() <= func2()"
    assert state.parameters == []


def test_compile_le_param(state):
    expr = Func1() <= "value"
    statement = compile(expr, state)
    assert statement == "func1() <= ?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_lshift(state):
    expr = LShift(Func1(), Func2())
    statement = compile(expr, state)
    assert statement == "func1()<<func2()"
    assert state.parameters == []


def test_compile_lshift_param(state):
    expr = Func1() << "value"
    statement = compile(expr, state)
    assert statement == "func1()<<?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_rshift(state):
    expr = RShift(Func1(), Func2())
    statement = compile(expr, state)
    assert statement == "func1()>>func2()"
    assert state.parameters == []


def test_compile_rshift_param(state):
    expr = Func1() >> "value"
    statement = compile(expr, state)
    assert statement == "func1()>>?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_like(state):
    expr = Like(Func1(), b"value")
    statement = compile(expr, state)
    assert statement == "func1() LIKE ?"
    assert_variables_equal(state.parameters, [RawStrVariable(b"value")])


def test_compile_like_param(state):
    expr = Func1().like("Hello")
    statement = compile(expr, state)
    assert statement == "func1() LIKE ?"
    assert_variables_equal(state.parameters, [Variable("Hello")])


def test_compile_like_escape(state):
    expr = Like(Func1(), b"value", b"!")
    statement = compile(expr, state)
    assert statement == "func1() LIKE ? ESCAPE ?"
    assert_variables_equal(state.parameters,
                      [RawStrVariable(b"value"), RawStrVariable(b"!")])


def test_compile_like_escape_param(state):
    expr = Func1().like("Hello", b"!")
    statement = compile(expr, state)
    assert statement == "func1() LIKE ? ESCAPE ?"
    assert_variables_equal(state.parameters,
                           [Variable("Hello"), RawStrVariable(b"!")])


def test_compile_like_compareable_case():
    expr = Func1().like("Hello")
    assert expr.case_sensitive == None
    expr = Func1().like("Hello", case_sensitive=True)
    assert expr.case_sensitive == True
    expr = Func1().like("Hello", case_sensitive=False)
    assert expr.case_sensitive == False


def test_compile_in(state):
    expr = In(Func1(), b"value")
    statement = compile(expr, state)
    assert statement == "func1() IN (?)"
    assert_variables_equal(state.parameters, [RawStrVariable(b"value")])


def test_compile_in_param(state):
    expr = In(Func1(), elem1)
    statement = compile(expr, state)
    assert statement == "func1() IN (elem1)"
    assert state.parameters == []


def test_compile_and(state):
    expr = And(elem1, elem2, And(elem3, elem4))
    statement = compile(expr, state)
    assert statement == "elem1 AND elem2 AND elem3 AND elem4"
    assert state.parameters == []


def test_compile_and_param(state):
    expr = Func1() & "value"
    statement = compile(expr, state)
    assert statement == "func1() AND ?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_or(state):
    expr = Or(elem1, elem2, Or(elem3, elem4))
    statement = compile(expr, state)
    assert statement == "elem1 OR elem2 OR elem3 OR elem4"
    assert state.parameters == []


def test_compile_or_param(state):
    expr = Func1() | "value"
    statement = compile(expr, state)
    assert statement == "func1() OR ?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_and_with_strings(state):
    expr = And("elem1", "elem2")
    statement = compile(expr, state)
    assert statement == "elem1 AND elem2"
    assert state.parameters == []


def test_compile_or_with_strings(state):
    expr = Or("elem1", "elem2")
    statement = compile(expr, state)
    assert statement == "elem1 OR elem2"
    assert state.parameters == []


def test_compile_add(state):
    expr = Add(elem1, elem2, Add(elem3, elem4))
    statement = compile(expr, state)
    assert statement == "elem1+elem2+elem3+elem4"
    assert state.parameters == []


def test_compile_add_param(state):
    expr = Func1() + "value"
    statement = compile(expr, state)
    assert statement == "func1()+?"
    assert_variables_equal(state.parameters, [Variable("value")])


@pytest.mark.parametrize("expr, sql", [
    (Sub(elem1, Sub(elem2, elem3)), "elem1-(elem2-elem3)"),
    (Sub(Sub(elem1, elem2), elem3), "elem1-elem2-elem3"),
])
def test_compile_sub(state, expr, sql):
    statement = compile(expr, state)
    assert statement == sql
    assert state.parameters == []
    assert_variables_equal(state.parameters, [])

def test_compile_sub_param(state):
    expr = Func1() - "value"
    statement = compile(expr, state)
    assert statement == "func1()-?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_mul(state):
    expr = Mul(elem1, elem2, Mul(elem3, elem4))
    statement = compile(expr, state)
    assert statement == "elem1*elem2*elem3*elem4"
    assert state.parameters == []


def test_compile_mul_param(state):
    expr = Func1() * "value"
    statement = compile(expr, state)
    assert statement == "func1()*?"
    assert_variables_equal(state.parameters, [Variable("value")])


@pytest.mark.parametrize("expr, sql", [
    (Div(elem1, Div(elem2, elem3)), "elem1/(elem2/elem3)"),
    (Div(Div(elem1, elem2), elem3), "elem1/elem2/elem3"),
])
def test_compile_div(state, expr, sql):
    statement = compile(expr, state)
    assert statement == sql
    assert state.parameters == []


def test_compile_div_param(state):
    expr = Func1() / "value"
    statement = compile(expr, state)
    assert statement == "func1()/?"
    assert_variables_equal(state.parameters, [Variable("value")])


@pytest.mark.parametrize("expr, sql", [
    (Mod(elem1, Mod(elem2, elem3)), "elem1%(elem2%elem3)"),
    (Mod(Mod(elem1, elem2), elem3), "elem1%elem2%elem3"),
])
def test_compile_mod(state, expr, sql):
    statement = compile(expr, state)
    assert statement == sql
    assert state.parameters == []


def test_compile_mod_param(state):
    expr = Func1() % "value"
    statement = compile(expr, state)
    assert statement == "func1()%?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_func(state):
    expr = Func("myfunc", elem1, Func1(elem2))
    statement = compile(expr, state)
    assert statement == "myfunc(elem1, func1(elem2))"
    assert state.parameters == []


def test_compile_named_func(state):
    expr = Func1(elem1, Func2(elem2))
    statement = compile(expr, state)
    assert statement == "func1(elem1, func2(elem2))"
    assert state.parameters == []


def test_compile_count(state):
    expr = Count(Func1())
    statement = compile(expr, state)
    assert statement == "COUNT(func1())"
    assert state.parameters == []


def test_compile_count_all(state):
    expr = Count()
    statement = compile(expr, state)
    assert statement == "COUNT(*)"
    assert state.parameters == []


def test_compile_count_distinct(state):
    expr = Count(Func1(), distinct=True)
    statement = compile(expr, state)
    assert statement == "COUNT(DISTINCT func1())"
    assert state.parameters == []


def test_compile_count_distinct_all():
    with pytest.raises(ValueError):
        Count(distinct=True)


def test_compile_cast(state):
    """
    The L{Cast} expression renders a C{CAST} function call with a
    user-defined input value and the type to cast it to.
    """
    expr = Cast(Func1(), "TEXT")
    statement = compile(expr, state)
    assert statement == "CAST(func1() AS TEXT)"
    assert state.parameters == []


def test_compile_max(state):
    expr = Max(Func1())
    statement = compile(expr, state)
    assert statement == "MAX(func1())"
    assert state.parameters == []


def test_compile_min(state):
    expr = Min(Func1())
    statement = compile(expr, state)
    assert statement == "MIN(func1())"
    assert state.parameters == []


def test_compile_avg(state):
    expr = Avg(Func1())
    statement = compile(expr, state)
    assert statement == "AVG(func1())"
    assert state.parameters == []


def test_compile_sum(state):
    expr = Sum(Func1())
    statement = compile(expr, state)
    assert statement == "SUM(func1())"
    assert state.parameters == []


def test_compile_lower(state):
    expr = Lower(Func1())
    statement = compile(expr, state)
    assert statement == "LOWER(func1())"
    assert state.parameters == []


def test_compile_lower_method(state):
    expr = Func1().lower()
    statement = compile(expr, state)
    assert statement == "LOWER(func1())"
    assert state.parameters == []


def test_compile_upper(state):
    expr = Upper(Func1())
    statement = compile(expr, state)
    assert statement == "UPPER(func1())"
    assert state.parameters == []


def test_compile_upper_method(state):
    expr = Func1().upper()
    statement = compile(expr, state)
    assert statement == "UPPER(func1())"
    assert state.parameters == []


def test_compile_coalesce(state):
    expr = Coalesce(Func1())
    statement = compile(expr, state)
    assert statement == "COALESCE(func1())"
    assert state.parameters == []


def test_compile_coalesce_with_many_arguments(state):
    expr = Coalesce(Func1(), Func2(), None)
    statement = compile(expr, state)
    assert statement == "COALESCE(func1(), func2(), NULL)"
    assert state.parameters == []


def test_compile_not(state):
    expr = Not(Func1())
    statement = compile(expr, state)
    assert statement == "NOT func1()"
    assert state.parameters == []


def test_compile_exists(state):
    expr = Exists(Func1())
    statement = compile(expr, state)
    assert statement == "EXISTS func1()"
    assert state.parameters == []


def test_compile_neg(state):
    expr = Neg(Func1())
    statement = compile(expr, state)
    assert statement == "- func1()"
    assert state.parameters == []


def test_compile_neg_operator(state):
    expr = -Func1()
    statement = compile(expr, state)
    assert statement == "- func1()"
    assert state.parameters == []


def test_compile_asc(state):
    expr = Asc(Func1())
    statement = compile(expr, state)
    assert statement == "func1() ASC"
    assert state.parameters == []


def test_compile_desc(state):
    expr = Desc(Func1())
    statement = compile(expr, state)
    assert statement == "func1() DESC"
    assert state.parameters == []


def test_compile_asc_with_string(state):
    expr = Asc("column")
    statement = compile(expr, state)
    assert statement == "column ASC"
    assert state.parameters == []


def test_compile_desc_with_string(state):
    expr = Desc("column")
    statement = compile(expr, state)
    assert statement == "column DESC"
    assert state.parameters == []


def test_compile_sql(state):
    expr = SQL("expression")
    statement = compile(expr, state)
    assert statement == "expression"
    assert state.parameters == []


def test_compile_sql_params(state):
    expr = SQL("expression", ["params"])
    statement = compile(expr, state)
    assert statement == "expression"
    assert state.parameters == ["params"]


def test_compile_sql_invalid_params():
    expr = SQL("expression", "not a list or tuple")
    with pytest.raises(CompileError):
        compile(expr)


def test_compile_sql_tables(state):
    expr = Select([column1, Func1()], SQL("expression", [], Func2()))
    statement = compile(expr, state)
    assert statement == "SELECT column1, func1() FROM func2() WHERE expression"
    assert state.parameters == []

@pytest.mark.parametrize("seq", [
    [Func1(), Func2()],
    (Func1(), Func2()),
])

def test_compile_sql_tables_with_seq(state, seq):
    sql = SQL("expression", [], seq)
    expr = Select(column1, sql)
    statement = compile(expr, state)
    assert statement == "SELECT column1 FROM func1(), func2() WHERE expression"
    assert state.parameters == []


def test_compile_sql_comparison(state):
    expr = SQL("expression1") & SQL("expression2")
    statement = compile(expr, state)
    assert statement == "(expression1) AND (expression2)"
    assert state.parameters == []


def test_compile_table(state):
    expr = Table(table1)
    assert expr.compile_cache is None
    statement = compile(expr, state)
    assert statement == '"table 1"'
    assert state.parameters == []
    assert expr.compile_cache == '"table 1"'


def test_compile_alias(state):
    expr = Alias(Table(table1), "name")
    statement = compile(expr, state)
    assert statement == "name"
    assert state.parameters == []


def test_compile_alias_in_tables(state):
    expr = Select(column1, tables=Alias(Table(table1), "alias 1"))
    statement = compile(expr, state)
    assert statement == 'SELECT column1 FROM "table 1" AS "alias 1"'
    assert state.parameters == []


def test_compile_alias_in_tables_auto_name(state):
    expr = Select(column1, tables=Alias(Table(table1)))
    statement = compile(expr, state)
    assert statement[:statement.rfind("_") + 1] == (
        'SELECT column1 FROM "table 1" AS "_'
    )
    assert state.parameters == []


def test_compile_alias_in_column_prefix(state):
    expr = Select(Column(column1, Alias(Table(table1), "alias 1")))
    statement = compile(expr, state)
    assert statement == 'SELECT "alias 1".column1 FROM "table 1" AS "alias 1"'
    assert state.parameters == []


def test_compile_alias_for_column(state):
    expr = Select(Alias(Column(column1, table1), "alias 1"))
    statement = compile(expr, state)
    assert statement == 'SELECT "table 1".column1 AS "alias 1" FROM "table 1"'
    assert state.parameters == []


def test_compile_alias_union(state):
    union = Union(Select(elem1), Select(elem2))
    expr = Select(elem3, tables=Alias(union, "alias"))
    statement = compile(expr, state)
    assert statement == (
        "SELECT elem3 FROM ((SELECT elem1) UNION (SELECT elem2)) AS alias"
    )
    assert state.parameters == []


def test_compile_distinct(state):
    """L{Distinct} adds a DISTINCT prefix to the given expression."""
    distinct = Distinct(Column(elem1))
    statement = compile(distinct, state)
    assert statement == "DISTINCT elem1"
    assert state.parameters == []


def test_compile_join(state):
    expr = Join(Func1())
    statement = compile(expr, state)
    assert statement == "JOIN func1()"
    assert state.parameters == []


def test_compile_join_on(state):
    expr = Join(Func1(), Func2() == "value")
    statement = compile(expr, state)
    assert statement == "JOIN func1() ON func2() = ?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_join_on_with_string(state):
    expr = Join(Func1(), on="a = b")
    statement = compile(expr, state)
    assert statement == "JOIN func1() ON a = b"
    assert state.parameters == []


def test_compile_join_left_right(state):
    expr = Join(table1, table2)
    statement = compile(expr, state)
    assert statement == '"table 1" JOIN "table 2"'
    assert state.parameters == []


def test_compile_join_nested(state):
    expr = Join(table1, Join(table2, table3))
    statement = compile(expr, state)
    assert statement == '"table 1" JOIN ("table 2" JOIN "table 3")'
    assert state.parameters == []


def test_compile_join_double_nested(state):
    expr = Join(Join(table1, table2), Join(table3, table4))
    statement = compile(expr, state)
    assert statement == (
        '"table 1" JOIN "table 2" JOIN ("table 3" JOIN "table 4")'
    )
    assert state.parameters == []


def test_compile_join_table(state):
    expr = Join(Table(table1), Table(table2))
    statement = compile(expr, state)
    assert statement == '"table 1" JOIN "table 2"'
    assert state.parameters == []


def test_compile_join_contexts():
    table1, table2, on = track_contexts(3)
    expr = Join(table1, table2, on)
    compile(expr)
    assert table1.context == None
    assert table2.context == None
    assert on.context == EXPR


def test_compile_left_join(state):
    expr = LeftJoin(Func1())
    statement = compile(expr, state)
    assert statement == "LEFT JOIN func1()"
    assert state.parameters == []


def test_compile_left_join_on(state):
    expr = LeftJoin(Func1(), Func2() == "value")
    statement = compile(expr, state)
    assert statement == "LEFT JOIN func1() ON func2() = ?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_right_join(state):
    expr = RightJoin(Func1())
    statement = compile(expr, state)
    assert statement == "RIGHT JOIN func1()"
    assert state.parameters == []


def test_compile_right_join_on(state):
    expr = RightJoin(Func1(), Func2() == "value")
    statement = compile(expr, state)
    assert statement == "RIGHT JOIN func1() ON func2() = ?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_natural_join(state):
    expr = NaturalJoin(Func1())
    statement = compile(expr, state)
    assert statement == "NATURAL JOIN func1()"
    assert state.parameters == []


def test_compile_natural_join_on(state):
    expr = NaturalJoin(Func1(), Func2() == "value")
    statement = compile(expr, state)
    assert statement == "NATURAL JOIN func1() ON func2() = ?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_natural_left_join(state):
    expr = NaturalLeftJoin(Func1())
    statement = compile(expr, state)
    assert statement == "NATURAL LEFT JOIN func1()"
    assert state.parameters == []


def test_compile_natural_left_join_on(state):
    expr = NaturalLeftJoin(Func1(), Func2() == "value")
    statement = compile(expr, state)
    assert statement == "NATURAL LEFT JOIN func1() ON func2() = ?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_natural_right_join(state):
    expr = NaturalRightJoin(Func1())
    statement = compile(expr, state)
    assert statement == "NATURAL RIGHT JOIN func1()"
    assert state.parameters == []


def test_compile_natural_right_join_on(state):
    expr = NaturalRightJoin(Func1(), Func2() == "value")
    statement = compile(expr, state)
    assert statement == "NATURAL RIGHT JOIN func1() ON func2() = ?"
    assert_variables_equal(state.parameters, [Variable("value")])


def test_compile_union(state):
    expr = Union(Func1(), elem2, elem3)
    statement = compile(expr, state)
    assert statement == "func1() UNION elem2 UNION elem3"
    assert state.parameters == []


def test_compile_union_all(state):
    expr = Union(Func1(), elem2, elem3, all=True)
    statement = compile(expr, state)
    assert statement == "func1() UNION ALL elem2 UNION ALL elem3"
    assert state.parameters == []


def test_compile_union_order_by_limit_offset(state):
    expr = Union(elem1, elem2, order_by=Func1(), limit=1, offset=2)
    statement = compile(expr, state)
    assert statement == "elem1 UNION elem2 ORDER BY func1() LIMIT 1 OFFSET 2"
    assert state.parameters == []


def test_compile_union_select(state):
    expr = Union(Select(elem1), Select(elem2))
    statement = compile(expr, state)
    assert statement == "(SELECT elem1) UNION (SELECT elem2)"
    assert state.parameters == []


def test_compile_union_select_nested(state):
    expr = Union(Select(elem1), Union(Select(elem2), Select(elem3)))
    statement = compile(expr, state)
    assert statement == (
        "(SELECT elem1) UNION ((SELECT elem2) UNION (SELECT elem3))"
    )
    assert state.parameters == []


def test_compile_union_order_by_and_select(state):
    """
    When ORDER BY is present, databases usually have trouble using
    fully qualified column names.  Because of that, we transform
    pure column names into aliases, and use them in the ORDER BY.
    """
    Alias.auto_counter = 0
    column1 = Column(elem1)
    column2 = Column(elem2)
    expr = Union(Select(column1), Select(column2),
                 order_by=(column1, column2))
    statement = compile(expr, state)
    assert statement == (
        '(SELECT elem1 AS "_1") UNION (SELECT elem2 AS "_2") '
        'ORDER BY "_1", "_2"'
    )
    assert state.parameters == []


def test_compile_union_contexts():
    select1, select2, order_by = track_contexts(3)
    expr = Union(select1, select2, order_by=order_by)
    compile(expr)
    assert select1.context == SELECT
    assert select2.context == SELECT
    assert order_by.context == COLUMN_NAME


def test_compile_except(state):
    expr = Except(Func1(), elem2, elem3)
    statement = compile(expr, state)
    assert statement == "func1() EXCEPT elem2 EXCEPT elem3"
    assert state.parameters == []


def test_compile_except_all(state):
    expr = Except(Func1(), elem2, elem3, all=True)
    statement = compile(expr, state)
    assert statement == "func1() EXCEPT ALL elem2 EXCEPT ALL elem3"
    assert state.parameters == []


def test_compile_except_order_by_limit_offset(state):
    expr = Except(elem1, elem2, order_by=Func1(), limit=1, offset=2)
    statement = compile(expr, state)
    assert statement == "elem1 EXCEPT elem2 ORDER BY func1() LIMIT 1 OFFSET 2"
    assert state.parameters == []


def test_compile_except_select(state):
    expr = Except(Select(elem1), Select(elem2))
    statement = compile(expr, state)
    assert statement == "(SELECT elem1) EXCEPT (SELECT elem2)"
    assert state.parameters == []


def test_compile_except_select_nested(state):
    expr = Except(Select(elem1), Except(Select(elem2), Select(elem3)))
    statement = compile(expr, state)
    assert statement == (
        "(SELECT elem1) EXCEPT ((SELECT elem2) EXCEPT (SELECT elem3))"
    )
    assert state.parameters == []


def test_compile_except_contexts():
    select1, select2, order_by = track_contexts(3)
    expr = Except(select1, select2, order_by=order_by)
    compile(expr)
    assert select1.context == SELECT
    assert select2.context == SELECT
    assert order_by.context == COLUMN_NAME


def test_compile_intersect(state):
    expr = Intersect(Func1(), elem2, elem3)
    statement = compile(expr, state)
    assert statement == "func1() INTERSECT elem2 INTERSECT elem3"
    assert state.parameters == []


def test_compile_intersect_all(state):
    expr = Intersect(Func1(), elem2, elem3, all=True)
    statement = compile(expr, state)
    assert statement == "func1() INTERSECT ALL elem2 INTERSECT ALL elem3"
    assert state.parameters == []


def test_compile_intersect_order_by_limit_offset(state):
    expr = Intersect(elem1, elem2, order_by=Func1(), limit=1, offset=2)
    statement = compile(expr, state)
    assert statement == (
        "elem1 INTERSECT elem2 ORDER BY func1() LIMIT 1 OFFSET 2"
    )
    assert state.parameters == []


def test_compile_intersect_select(state):
    expr = Intersect(Select(elem1), Select(elem2))
    statement = compile(expr, state)
    assert statement == "(SELECT elem1) INTERSECT (SELECT elem2)"
    assert state.parameters == []


def test_compile_intersect_select_nested(state):
    expr = Intersect(
        Select(elem1), Intersect(Select(elem2), Select(elem3)))
    statement = compile(expr, state)
    assert statement == (
        "(SELECT elem1) INTERSECT ((SELECT elem2) INTERSECT (SELECT elem3))"
    )
    assert state.parameters == []


def test_compile_intersect_contexts():
    select1, select2, order_by = track_contexts(3)
    expr = Intersect(select1, select2, order_by=order_by)
    compile(expr)
    assert select1.context == SELECT
    assert select2.context == SELECT
    assert order_by.context == COLUMN_NAME


def test_compile_auto_table(state):
    expr = Select(AutoTables(1, [table1]))
    statement = compile(expr, state)
    assert statement == 'SELECT ? FROM "table 1"'
    assert_variables_equal(state.parameters, [IntVariable(1)])


def test_compile_auto_tables_with_column(state):
    expr = Select(AutoTables(Column(elem1, table1), [table2]))
    statement = compile(expr, state)
    assert statement == 'SELECT "table 1".elem1 FROM "table 1", "table 2"'
    assert state.parameters == []


def test_compile_auto_tables_with_column_and_replace(state):
    expr = Select(AutoTables(Column(elem1, table1), [table2], replace=True))
    statement = compile(expr, state)
    assert statement == 'SELECT "table 1".elem1 FROM "table 2"'
    assert state.parameters == []


def test_compile_auto_tables_with_join(state):
    expr = Select(AutoTables(Column(elem1, table1), [LeftJoin(table2)]))
    statement = compile(expr, state)
    assert statement == (
        'SELECT "table 1".elem1 FROM "table 1" LEFT JOIN "table 2"'
    )
    assert state.parameters == []


def test_compile_auto_tables_with_join_with_left_table(state):
    expr = Select(AutoTables(Column(elem1, table1),
                             [LeftJoin(table1, table2)]))
    statement = compile(expr, state)
    assert statement == (
        'SELECT "table 1".elem1 FROM "table 1" LEFT JOIN "table 2"'
    )
    assert state.parameters == []


def test_compile_auto_tables_duplicated(state):
    expr = Select([AutoTables(Column(elem1, table1), [Join(table2)]),
                   AutoTables(Column(elem2, table2), [Join(table1)]),
                   AutoTables(Column(elem3, table1), [Join(table1)]),
                   AutoTables(Column(elem4, table3), [table1]),
                   AutoTables(Column(elem5, table1),
                              [Join(table4, table5)])])
    statement = compile(expr, state)
    assert statement == (
        'SELECT "table 1".elem1, "table 2".elem2, '
        '"table 1".elem3, "table 3".elem4, "table 1".elem5 '
        'FROM "table 3", "table 4" JOIN "table 5" JOIN '
        '"table 1" JOIN "table 2"'
    )
    assert state.parameters == []


def test_compile_auto_tables_duplicated_nested(state):
    expr = Select(AutoTables(Column(elem1, table1), [Join(table2)]),
                  In(1, Select(AutoTables(Column(elem1, table1),
                                          [Join(table2)]))))
    statement = compile(expr, state)
    assert statement == (
        'SELECT "table 1".elem1 FROM "table 1" JOIN '
        '"table 2" WHERE ? IN (SELECT "table 1".elem1 '
        'FROM "table 1" JOIN "table 2")'
    )
    assert_variables_equal(state.parameters, [IntVariable(1)])


def test_compile_sql_token(state):
    expr = SQLToken("something")
    statement = compile(expr, state)
    assert statement == "something"
    assert state.parameters == []


def test_compile_sql_token_spaces():
    expr = SQLToken("some thing")
    statement = compile(expr)
    assert statement == '"some thing"'


def test_compile_sql_token_quotes():
    expr = SQLToken("some'thing")
    statement = compile(expr)
    assert statement == '"some\'thing"'


def test_compile_sql_token_double_quotes():
    expr = SQLToken('some"thing')
    statement = compile(expr)
    assert statement == '"some""thing"'


def test_compile_sql_token_reserved(state):
    custom_compile = compile.create_child()
    custom_compile.add_reserved_words(["something"])
    expr = SQLToken("something")
    statement = custom_compile(expr, state)
    assert statement == '"something"'
    assert state.parameters == []


def test_compile_sql_token_reserved_from_parent():
    expr = SQLToken("something")
    parent_compile = compile.create_child()
    child_compile = parent_compile.create_child()
    statement = child_compile(expr)
    assert statement == "something"
    parent_compile.add_reserved_words(["something"])
    statement = child_compile(expr)
    assert statement == '"something"'


def test_compile_sql_token_remove_reserved_word_on_child():
    expr = SQLToken("something")
    parent_compile = compile.create_child()
    parent_compile.add_reserved_words(["something"])
    child_compile = parent_compile.create_child()
    statement = child_compile(expr)
    assert statement == '"something"'
    child_compile.remove_reserved_words(["something"])
    statement = child_compile(expr)
    assert statement == "something"


def test_compile_is_reserved_word():
    parent_compile = compile.create_child()
    child_compile = parent_compile.create_child()
    assert child_compile.is_reserved_word("someTHING") == False
    parent_compile.add_reserved_words(["SOMEthing"])
    assert child_compile.is_reserved_word("somETHing") == True
    child_compile.remove_reserved_words(["soMETHing"])
    assert child_compile.is_reserved_word("somethING") == False


def test_compile_sql1992_reserved_words():
    reserved_words = """
        absolute action add all allocate alter and any are as asc assertion
        at authorization avg begin between bit bit_length both by cascade
        cascaded case cast catalog char character char_ length
        character_length check close coalesce collate collation column
        commit connect connection constraint constraints continue convert
        corresponding count create cross current current_date current_time
        current_timestamp current_ user cursor date day deallocate dec
        decimal declare default deferrable deferred delete desc describe
        descriptor diagnostics disconnect distinct domain double drop else
        end end-exec escape except exception exec execute exists external
        extract false fetch first float for foreign found from full get
        global go goto grant group having hour identity immediate in
        indicator initially inner input insensitive insert int integer
        intersect interval into is isolation join key language last leading
        left level like local lower match max min minute module month names
        national natural nchar next no not null nullif numeric octet_length
        of on only open option or order outer output overlaps pad partial
        position precision prepare preserve primary prior privileges
        procedure public read real references relative restrict revoke
        right rollback rows schema scroll second section select session
        session_ user set size smallint some space sql sqlcode sqlerror
        sqlstate substring sum system_user table temporary then time
        timestamp timezone_ hour timezone_minute to trailing transaction
        translate translation trim true union unique unknown update upper
        usage user using value values varchar varying view when whenever
        where with work write year zone
        """.split()
    for word in reserved_words:
        assert compile.is_reserved_word(word) == True


def test_compile_python_precedence():
    expr = And(1, Or(2, 3),
               Add(4, Mul(5, Sub(6, Div(7, Div(8, 9))))))
    py_expr = compile_python(expr)
    assert py_expr == "1 and (2 or 3) and 4+5*(6-7/(8/9))"


def test_compile_python_get_precedence():
    assert compile_python.get_precedence(Or) < compile_python.get_precedence(And)
    assert compile_python.get_precedence(Add) < compile_python.get_precedence(Mul)
    assert compile_python.get_precedence(Sub) < compile_python.get_precedence(Div)


def test_compile_python_compile_sequence(state):
    expr = [elem1, Variable(1), (Variable(2), None)]
    py_expr = compile_python(expr, state)
    assert py_expr == "elem1, _0, _1, None"
    assert state.parameters == [1, 2]


def test_compile_python_compile_invalid():
    with pytest.raises(CompileError):
        compile_python(object())

    with pytest.raises(CompileError):
        compile_python([object()])


def test_compile_python_compile_unsupported():
    with pytest.raises(CompileError):
        compile_python(Expr())

    with pytest.raises(CompileError):
        compile_python(Func1())


def test_compile_python_str():
    py_expr = compile_python("str")
    assert py_expr == "'str'"


def test_compile_python_unicode():
    py_expr = compile_python(u"str")

    if is_python2:
        assert py_expr == "u'str'"
    else:
        assert py_expr == "'str'"


def test_compile_python_int():
    py_expr = compile_python(1)
    assert py_expr == "1"


def test_compile_python_long():
    py_expr = compile_python(long_int(1))

    if is_python2:
        assert py_expr == "1L"
    else:
        assert py_expr == "1"


def test_compile_python_bool(state):
    py_expr = compile_python(True, state)
    assert py_expr == "_0"
    assert state.parameters == [True]


def test_compile_python_float():
    py_expr = compile_python(1.1)
    assert py_expr == repr(1.1)


def test_compile_python_datetime(state):
    dt = datetime(1977, 5, 4, 12, 34)
    py_expr = compile_python(dt, state)
    assert py_expr == "_0"
    assert state.parameters == [dt]


def test_compile_python_date(state):
    d = date(1977, 5, 4)
    py_expr = compile_python(d, state)
    assert py_expr == "_0"
    assert state.parameters == [d]


def test_compile_python_time(state):
    t = time(12, 34)
    py_expr = compile_python(t, state)
    assert py_expr == "_0"
    assert state.parameters == [t]


def test_compile_python_timedelta(state):
    td = timedelta(days=1, seconds=2, microseconds=3)
    py_expr = compile_python(td, state)
    assert py_expr == "_0"
    assert state.parameters == [td]


def test_compile_python_none():
    py_expr = compile_python(None)
    assert py_expr == "None"


def test_compile_python_column(state):
    expr = Column(column1)
    py_expr = compile_python(expr, state)
    assert py_expr == "get_column(_0)"
    assert state.parameters == [expr]


def test_compile_python_column_table(state):
    expr = Column(column1, table1)
    py_expr = compile_python(expr, state)
    assert py_expr == "get_column(_0)"
    assert state.parameters == [expr]


def test_compile_python_variable(state):
    expr = Variable("value")
    py_expr = compile_python(expr, state)
    assert py_expr == "_0"
    assert state.parameters == ["value"]


def test_compile_python_eq(state):
    expr = Eq(Variable(1), Variable(2))
    py_expr = compile_python(expr, state)
    assert py_expr == "_0 == _1"
    assert state.parameters == [1, 2]


def test_compile_python_ne(state):
    expr = Ne(Variable(1), Variable(2))
    py_expr = compile_python(expr, state)
    assert py_expr == "_0 != _1"
    assert state.parameters == [1, 2]


def test_compile_python_gt(state):
    expr = Gt(Variable(1), Variable(2))
    py_expr = compile_python(expr, state)
    assert py_expr == "_0 > _1"
    assert state.parameters == [1, 2]


def test_compile_python_ge(state):
    expr = Ge(Variable(1), Variable(2))
    py_expr = compile_python(expr, state)
    assert py_expr == "_0 >= _1"
    assert state.parameters == [1, 2]


def test_compile_python_lt(state):
    expr = Lt(Variable(1), Variable(2))
    py_expr = compile_python(expr, state)
    assert py_expr == "_0 < _1"
    assert state.parameters == [1, 2]


def test_compile_python_le(state):
    expr = Le(Variable(1), Variable(2))
    py_expr = compile_python(expr, state)
    assert py_expr == "_0 <= _1"
    assert state.parameters == [1, 2]


def test_compile_python_lshift(state):
    expr = LShift(Variable(1), Variable(2))
    py_expr = compile_python(expr, state)
    assert py_expr == "_0<<_1"
    assert state.parameters == [1, 2]


def test_compile_python_rshift(state):
    expr = RShift(Variable(1), Variable(2))
    py_expr = compile_python(expr, state)
    assert py_expr == "_0>>_1"
    assert state.parameters == [1, 2]


def test_compile_python_in(state):
    expr = In(Variable(1), Variable(2))
    py_expr = compile_python(expr, state)
    assert py_expr == "_0 in (_1,)"
    assert state.parameters == [1, 2]


def test_compile_python_and():
    expr = And(elem1, elem2, And(elem3, elem4))
    py_expr = compile_python(expr)
    assert py_expr == "elem1 and elem2 and elem3 and elem4"


def test_compile_python_or():
    expr = Or(elem1, elem2, Or(elem3, elem4))
    py_expr = compile_python(expr)
    assert py_expr == "elem1 or elem2 or elem3 or elem4"


def test_compile_python_add():
    expr = Add(elem1, elem2, Add(elem3, elem4))
    py_expr = compile_python(expr)
    assert py_expr == "elem1+elem2+elem3+elem4"


def test_compile_python_neg():
    expr = Neg(elem1)
    py_expr = compile_python(expr)
    assert py_expr == "-elem1"


def test_compile_python_sub():
    expr = Sub(elem1, Sub(elem2, elem3))
    py_expr = compile_python(expr)
    assert py_expr == "elem1-(elem2-elem3)"

    expr = Sub(Sub(elem1, elem2), elem3)
    py_expr = compile_python(expr)
    assert py_expr == "elem1-elem2-elem3"


def test_compile_python_mul():
    expr = Mul(elem1, elem2, Mul(elem3, elem4))
    py_expr = compile_python(expr)
    assert py_expr == "elem1*elem2*elem3*elem4"


def test_compile_python_div():
    expr = Div(elem1, Div(elem2, elem3))
    py_expr = compile_python(expr)
    assert py_expr == "elem1/(elem2/elem3)"

    expr = Div(Div(elem1, elem2), elem3)
    py_expr = compile_python(expr)
    assert py_expr == "elem1/elem2/elem3"


def test_compile_python_mod():
    expr = Mod(elem1, Mod(elem2, elem3))
    py_expr = compile_python(expr)
    assert py_expr == "elem1%(elem2%elem3)"

    expr = Mod(Mod(elem1, elem2), elem3)
    py_expr = compile_python(expr)
    assert py_expr == "elem1%elem2%elem3"


def test_compile_python_match():
    col1 = Column(column1)
    col2 = Column(column2)

    match = compile_python.get_matcher((col1 > 10) & (col2 < 10))

    assert match({col1: 15, col2: 5}.get)
    assert not match({col1: 5, col2: 15}.get)


def test_compile_python_match_bad_repr():
    """The get_matcher() works for expressions containing values
    whose repr is not valid Python syntax."""
    class BadRepr(object):
        def __repr__(self):
            return "$Not a valid Python expression$"

    value = BadRepr()
    col1 = Column(column1)
    match = compile_python.get_matcher(col1 == Variable(value))
    assert match({col1: value}.get)


def test_lazy_expr_is_lazy_value():
    marker = object()
    expr = SQL("Hullah!")
    variable = Variable()
    variable.set(expr)
    assert variable.get(marker) is marker
