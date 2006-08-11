from tests.helper import TestHelper

from storm.variables import Variable
from storm.expr import *


class ExprTest(TestHelper):

    def test_select_default(self):
        expr = Select(())
        self.assertEquals(expr.columns, ())
        self.assertEquals(expr.where, Undef)
        self.assertEquals(expr.tables, Undef)
        self.assertEquals(expr.default_tables, Undef)
        self.assertEquals(expr.order_by, Undef)
        self.assertEquals(expr.group_by, Undef)
        self.assertEquals(expr.limit, Undef)
        self.assertEquals(expr.offset, Undef)
        self.assertEquals(expr.distinct, False)

    def test_select_constructor(self):
        objects = [object() for i in range(9)]
        expr = Select(*objects)
        self.assertEquals(expr.columns, objects[0])
        self.assertEquals(expr.where, objects[1])
        self.assertEquals(expr.tables, objects[2])
        self.assertEquals(expr.default_tables, objects[3])
        self.assertEquals(expr.order_by, objects[4])
        self.assertEquals(expr.group_by, objects[5])
        self.assertEquals(expr.limit, objects[6])
        self.assertEquals(expr.offset, objects[7])
        self.assertEquals(expr.distinct, objects[8])

    def test_insert_default(self):
        expr = Insert(None, None)
        self.assertEquals(expr.columns, None)
        self.assertEquals(expr.values, None)
        self.assertEquals(expr.table, Undef)
        self.assertEquals(expr.default_table, Undef)

    def test_insert_constructor(self):
        objects = [object() for i in range(4)]
        expr = Insert(*objects)
        self.assertEquals(expr.columns, objects[0])
        self.assertEquals(expr.values, objects[1])
        self.assertEquals(expr.table, objects[2])
        self.assertEquals(expr.default_table, objects[3])

    def test_update_default(self):
        expr = Update(None)
        self.assertEquals(expr.set, None)
        self.assertEquals(expr.where, Undef)
        self.assertEquals(expr.table, Undef)
        self.assertEquals(expr.default_table, Undef)

    def test_update_constructor(self):
        objects = [object() for i in range(4)]
        expr = Update(*objects)
        self.assertEquals(expr.set, objects[0])
        self.assertEquals(expr.where, objects[1])
        self.assertEquals(expr.table, objects[2])
        self.assertEquals(expr.default_table, objects[3])

    def test_delete_default(self):
        expr = Delete()
        self.assertEquals(expr.where, Undef)
        self.assertEquals(expr.table, Undef)

    def test_delete_constructor(self):
        objects = [object() for i in range(3)]
        expr = Delete(*objects)
        self.assertEquals(expr.where, objects[0])
        self.assertEquals(expr.table, objects[1])
        self.assertEquals(expr.default_table, objects[2])

    def test_and(self):
        expr = And("elem1", "elem2", "elem3")
        self.assertEquals(expr.exprs, ("elem1", "elem2", "elem3"))

    def test_or(self):
        expr = Or("elem1", "elem2", "elem3")
        self.assertEquals(expr.exprs, ("elem1", "elem2", "elem3"))

    def test_column_default(self):
        expr = Column()
        self.assertEquals(expr.name, Undef)
        self.assertEquals(expr.table, Undef)
        self.assertEquals(expr.variable_factory, Variable)

    def test_column_constructor(self):
        objects = [object() for i in range(3)]
        expr = Column(*objects)
        self.assertEquals(expr.name, objects[0])
        self.assertEquals(expr.table, objects[1])
        self.assertEquals(expr.variable_factory, objects[2])

    def test_func(self):
        expr = Func("myfunc", "arg1", "arg2")
        self.assertEquals(expr.name, "myfunc")
        self.assertEquals(expr.args, ("arg1", "arg2"))

    def test_named_func(self):
        class MyFunc(NamedFunc):
            name = "myfunc"
        expr = MyFunc("arg1", "arg2")
        self.assertEquals(expr.name, "myfunc")
        self.assertEquals(expr.args, ("arg1", "arg2"))

    def test_like(self):
        expr = Like("arg1", "arg2")
        self.assertEquals(expr.expr1, "arg1")
        self.assertEquals(expr.expr2, "arg2")

    def test_eq(self):
        expr = Eq("arg1", "arg2")
        self.assertEquals(expr.expr1, "arg1")
        self.assertEquals(expr.expr2, "arg2")

    def test_sql_default(self):
        expr = SQL(None)
        self.assertEquals(expr.expr, None)
        self.assertEquals(expr.params, Undef)
        self.assertEquals(expr.tables, Undef)

    def test_sql_constructor(self):
        objects = [object() for i in range(3)]
        expr = SQL(*objects)
        self.assertEquals(expr.expr, objects[0])
        self.assertEquals(expr.params, objects[1])
        self.assertEquals(expr.tables, objects[2])

    def test_join_expr_right(self):
        expr = JoinExpr(None)
        self.assertEquals(expr.right, None)
        self.assertEquals(expr.left, Undef)
        self.assertEquals(expr.on, Undef)

    def test_join_expr_on(self):
        on = Expr()
        expr = JoinExpr(None, on)
        self.assertEquals(expr.right, None)
        self.assertEquals(expr.left, Undef)
        self.assertEquals(expr.on, on)

    def test_join_expr_on_keyword(self):
        on = Expr()
        expr = JoinExpr(None, on=on)
        self.assertEquals(expr.right, None)
        self.assertEquals(expr.left, Undef)
        self.assertEquals(expr.on, on)

    def test_join_expr_on_invalid(self):
        on = Expr()
        self.assertRaises(ExprError, JoinExpr, None, on, None)

    def test_join_expr_right_left(self):
        objects = [object() for i in range(2)]
        expr = JoinExpr(*objects)
        self.assertEquals(expr.left, objects[0])
        self.assertEquals(expr.right, objects[1])
        self.assertEquals(expr.on, Undef)

    def test_join_expr_right_left_on(self):
        objects = [object() for i in range(3)]
        expr = JoinExpr(*objects)
        self.assertEquals(expr.left, objects[0])
        self.assertEquals(expr.right, objects[1])
        self.assertEquals(expr.on, objects[2])

    def test_join_expr_right_join(self):
        join = JoinExpr(None)
        expr = JoinExpr(None, join)
        self.assertEquals(expr.right, join)
        self.assertEquals(expr.left, None)
        self.assertEquals(expr.on, Undef)

    def test_table(self):
        objects = [object() for i in range(1)]
        expr = Table(*objects)
        self.assertEquals(expr.name, objects[0])

    def test_alias_default(self):
        expr = Alias(None)
        self.assertEquals(expr.expr, None)
        self.assertTrue(isinstance(expr.name, str))

    def test_alias_constructor(self):
        objects = [object() for i in range(2)]
        expr = Alias(*objects)
        self.assertEquals(expr.expr, objects[0])
        self.assertEquals(expr.name, objects[1])


class StateTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        self.state = State()

    def test_attrs(self):
        self.assertEquals(self.state.parameters, [])
        self.assertEquals(self.state.auto_tables, [])
        self.assertEquals(self.state.column_prefix, False)

    def test_push_pop(self):
        self.state.parameters.extend([1, 2])
        self.state.push("parameters", [])
        self.assertEquals(self.state.parameters, [])
        self.state.pop()
        self.assertEquals(self.state.parameters, [1, 2])
        self.state.push("parameters")
        self.assertEquals(self.state.parameters, [1, 2])
        self.state.parameters.append(3)
        self.assertEquals(self.state.parameters, [1, 2, 3])
        self.state.pop()
        self.assertEquals(self.state.parameters, [1, 2])

    def test_push_pop_unexistent(self):
        self.state.push("nonexistent")
        self.assertEquals(self.state.nonexistent, None)
        self.state.nonexistent = "something"
        self.state.pop()
        self.assertEquals(self.state.nonexistent, None)


class Func1(NamedFunc):
    name = "func1"

class Func2(NamedFunc):
    name = "func2"


class CompileTest(TestHelper):

    def test_customize(self):
        custom_compile = compile.fork()
        @custom_compile.when(type(None))
        def compile_none(compile, state, expr):
            return "None"
        statement, parameters = custom_compile(Func1(None))
        self.assertEquals(statement, "func1(None)")

    def test_customize_inheritance(self):
        class C(object): pass
        compile_parent = Compile()
        compile_child = compile_parent.fork()

        @compile_parent.when(C)
        def compile_in_parent(compile, state, expr):
            return "parent"
        statement, parameters = compile_child(C())
        self.assertEquals(statement, "parent")

        @compile_child.when(C)
        def compile_in_child(compile, state, expr):
            return "child"
        statement, parameters = compile_child(C())
        self.assertEquals(statement, "child")

    def test_precedence(self):
        expr = And("1", Or("2", "3"),
                   Add("4", Mul("5", Sub("6", Div("7", Div("8", "9"))))))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "1 AND (2 OR 3) AND 4+5*(6-7/(8/9))")

        expr = Func1(Select(Count()), [Select(Count())])
        statement, parameters = compile(expr)
        self.assertEquals(statement,
                          "func1((SELECT COUNT(*)), (SELECT COUNT(*)))")

    def test_get_precedence(self):
        self.assertTrue(compile.get_precedence(Or) <
                        compile.get_precedence(And))
        self.assertTrue(compile.get_precedence(Add) <
                        compile.get_precedence(Mul))
        self.assertTrue(compile.get_precedence(Sub) <
                        compile.get_precedence(Div))

    def test_customize_precedence(self):
        expr = And("and1", Or("or1", "or2"))
        custom_compile = compile.fork()
        custom_compile.set_precedence(10, And)

        custom_compile.set_precedence(11, Or)
        statement, parameters = custom_compile(expr)
        self.assertEquals(statement, "and1 AND or1 OR or2")

        custom_compile.set_precedence(10, Or)
        statement, parameters = custom_compile(expr)
        self.assertEquals(statement, "and1 AND or1 OR or2")

        custom_compile.set_precedence(9, Or)
        statement, parameters = custom_compile(expr)
        self.assertEquals(statement, "and1 AND (or1 OR or2)")

    def test_customize_precedence_inheritance(self):
        compile_parent = compile.fork()
        compile_child = compile_parent.fork()

        expr = And("and1", Or("or1", "or2"))

        compile_parent.set_precedence(10, And)

        compile_parent.set_precedence(11, Or)
        self.assertEquals(compile_child.get_precedence(Or), 11)
        self.assertEquals(compile_parent.get_precedence(Or), 11)
        statement, parameters = compile_child(expr)
        self.assertEquals(statement, "and1 AND or1 OR or2")

        compile_parent.set_precedence(10, Or)
        self.assertEquals(compile_child.get_precedence(Or), 10)
        self.assertEquals(compile_parent.get_precedence(Or), 10)
        statement, parameters = compile_child(expr)
        self.assertEquals(statement, "and1 AND or1 OR or2")

        compile_child.set_precedence(9, Or)
        self.assertEquals(compile_child.get_precedence(Or), 9)
        self.assertEquals(compile_parent.get_precedence(Or), 10)
        statement, parameters = compile_child(expr)
        self.assertEquals(statement, "and1 AND (or1 OR or2)")


    def test_compile_sequence(self):
        expr = ["str", Func1(), (Func2(), None)]
        statement, parameters = compile(expr)
        self.assertEquals(statement, "str, func1(), func2(), NULL")

    def test_compile_invalid(self):
        self.assertRaises(CompileError, compile, object())
        self.assertRaises(CompileError, compile, [object()])

    def test_str(self):
        statement, parameters = compile("str")
        self.assertEquals(statement, "str")
        self.assertEquals(parameters, [])

    def test_none(self):
        statement, parameters = compile(None)
        self.assertEquals(statement, "NULL")
        self.assertEquals(parameters, [])

    def test_select(self):
        expr = Select(["column1", "column2"])
        statement, parameters = compile(expr)
        self.assertEquals(statement, "SELECT column1, column2")
        self.assertEquals(parameters, [])

    def test_select_distinct(self):
        expr = Select(["column1", "column2"], Undef, ["table"], distinct=True)
        statement, parameters = compile(expr)
        self.assertEquals(statement,
                          "SELECT DISTINCT column1, column2 FROM table")
        self.assertEquals(parameters, [])

    def test_select_where(self):
        expr = Select(["column1", Func1()],
                      Func1(),
                      ["table1", Func1()],
                      order_by=["column2", Func1()],
                      group_by=["column3", Func1()],
                      limit=3, offset=4)
        statement, parameters = compile(expr)
        self.assertEquals(statement, "SELECT column1, func1() "
                                     "FROM table1, func1() "
                                     "WHERE func1() "
                                     "ORDER BY column2, func1() "
                                     "GROUP BY column3, func1() "
                                     "LIMIT 3 OFFSET 4")
        self.assertEquals(parameters, [])

    def test_select_auto_table(self):
        expr = Select(Column("column1", "table1"),
                      Column("column2", "table2") == 1),
        statement, parameters = compile(expr)
        self.assertEquals(statement, "SELECT table1.column1 "
                                     "FROM table1, table2 "
                                     "WHERE table2.column2 = ?")
        self.assertEquals(parameters, [Variable(1)])

    def test_select_auto_table_default(self):
        expr = Select(Column("column1"),
                      Column("column2") == 1,
                      default_tables="table"),
        statement, parameters = compile(expr)
        self.assertEquals(statement, "SELECT column1 "
                                     "FROM table "
                                     "WHERE column2 = ?")
        self.assertEquals(parameters, [Variable(1)])

    def test_select_auto_table_unknown(self):
        statement, parameters = compile(Select("1"))
        self.assertEquals(statement, "SELECT 1")

    def test_select_auto_table_sub(self):
        column1 = Column("column1", "table1")
        column2 = Column("column2", "table2")
        expr = Select(column1, In("1", Select(column2, column1 == column2,
                                              column2.table)))
        statement, parameters = compile(expr)
        self.assertEquals(statement,
                          "SELECT table1.column1 FROM table1 WHERE "
                          "1 IN (SELECT table2.column2 FROM table2 WHERE "
                          "table1.column1 = table2.column2)")

    def test_select_join(self):
        expr = Select(["column1", Func1()], Func1(),
                      ["table1", Join("table2"), Join("table3")])
        statement, parameters = compile(expr)
        self.assertEquals(statement, "SELECT column1, func1() "
                                     "FROM table1 JOIN table2 JOIN table3 "
                                     "WHERE func1()")
        self.assertEquals(parameters, [])

    def test_select_join_right_left(self):
        expr = Select(["column1", Func1()], Func1(),
                      ["table1", Join("table2", "table3")])
        statement, parameters = compile(expr)
        self.assertEquals(statement, "SELECT column1, func1() "
                                     "FROM table1, table2 JOIN table3 "
                                     "WHERE func1()")
        self.assertEquals(parameters, [])

    def test_insert(self):
        expr = Insert(["column1", Func1()], ["value1", Func1()], Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "INSERT INTO func1() (column1, func1()) "
                                     "VALUES (value1, func1())")
        self.assertEquals(parameters, [])

    def test_insert_with_columns(self):
        expr = Insert([Column("a", "table"), Column("b", "table")],
                      ["1", "2"], "table")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "INSERT INTO table (a, b) VALUES (1, 2)")
        self.assertEquals(parameters, [])

    def test_insert_auto_table(self):
        expr = Insert(Column("column", "table"), "value")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "INSERT INTO table (column) "
                                     "VALUES (value)")
        self.assertEquals(parameters, [])

    def test_insert_auto_table_default(self):
        expr = Insert(Column("column"), "value", default_table="table")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "INSERT INTO table (column) "
                                     "VALUES (value)")
        self.assertEquals(parameters, [])

    def test_insert_auto_table_unknown(self):
        expr = Insert(Column("column"), "value")
        self.assertRaises(NoTableError, compile, expr)

    def test_update(self):
        expr = Update({"column1": "value1", Func1(): Func2()}, table=Func1())
        statement, parameters = compile(expr)
        self.assertTrue(statement in
                        ["UPDATE func1() SET column1=value1, func1()=func2()",
                         "UPDATE func1() SET func1()=func2(), column1=value1"])
        self.assertEquals(parameters, [])

    def test_update_with_columns(self):
        expr = Update({Column("column", "table"): "value"}, table="table")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "UPDATE table SET column=value")
        self.assertEquals(parameters, [])

    def test_update_where(self):
        expr = Update({"column": "value"}, Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement,
                          "UPDATE func2() SET column=value WHERE func1()")
        self.assertEquals(parameters, [])

    def test_update_auto_table(self):
        expr = Update({Column("column", "table"): "value"})
        statement, parameters = compile(expr)
        self.assertEquals(statement, "UPDATE table SET column=value")
        self.assertEquals(parameters, [])

    def test_update_auto_table_default(self):
        expr = Update({Column("column"): "value"}, default_table="table")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "UPDATE table SET column=value")
        self.assertEquals(parameters, [])

    def test_update_auto_table_unknown(self):
        expr = Update({Column("column"): "value"})
        self.assertRaises(CompileError, compile, expr)

    def test_delete(self):
        expr = Delete(table=Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "DELETE FROM func1()")
        self.assertEquals(parameters, [])

    def test_delete_where(self):
        expr = Delete(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "DELETE FROM func2() WHERE func1()")
        self.assertEquals(parameters, [])

    def test_delete_auto_table(self):
        expr = Delete(Column("column", "table") == 1)
        statement, parameters = compile(expr)
        self.assertEquals(statement,
                          "DELETE FROM table WHERE table.column = ?")
        self.assertEquals(parameters, [Variable(1)])

    def test_delete_auto_table_default(self):
        expr = Delete(Column("column") == 1, default_table="table")
        statement, parameters = compile(expr)
        self.assertEquals(statement,
                          "DELETE FROM table WHERE column = ?")
        self.assertEquals(parameters, [Variable(1)])

    def test_delete_auto_table_unknown(self):
        expr = Delete(Column("column") == 1)
        self.assertRaises(NoTableError, compile, expr)

    def test_column(self):
        expr = Column("name")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "name")
        self.assertEquals(parameters, [])

    def test_column_table(self):
        expr = Select(Column("name", Func1()))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "SELECT func1().name FROM func1()")
        self.assertEquals(parameters, [])

    def test_variable(self):
        expr = Variable("value")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "?")
        self.assertEquals(parameters, [Variable("value")])

    def test_eq(self):
        expr = Eq(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() = func2()")
        self.assertEquals(parameters, [])

        expr = Func1() == "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() = ?")
        self.assertEquals(parameters, [Variable("value")])

    def test_is_in(self):
        expr = Func1().is_in(["Hello", "World"])
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() IN (?, ?)")
        self.assertEquals(parameters, [Variable("Hello"), Variable("World")])

    def test_eq_none(self):
        expr = Func1() == None

        self.assertTrue(expr.expr2 is None)

        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() IS NULL")
        self.assertEquals(parameters, [])

    def test_ne(self):
        expr = Ne(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() != func2()")
        self.assertEquals(parameters, [])

        expr = Func1() != "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() != ?")
        self.assertEquals(parameters, [Variable("value")])

    def test_ne_none(self):
        expr = Func1() != None

        self.assertTrue(expr.expr2 is None)

        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() IS NOT NULL")
        self.assertEquals(parameters, [])

    def test_gt(self):
        expr = Gt(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() > func2()")
        self.assertEquals(parameters, [])

        expr = Func1() > "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() > ?")
        self.assertEquals(parameters, [Variable("value")])

    def test_ge(self):
        expr = Ge(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() >= func2()")
        self.assertEquals(parameters, [])

        expr = Func1() >= "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() >= ?")
        self.assertEquals(parameters, [Variable("value")])

    def test_lt(self):
        expr = Lt(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() < func2()")
        self.assertEquals(parameters, [])

        expr = Func1() < "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() < ?")
        self.assertEquals(parameters, [Variable("value")])

    def test_le(self):
        expr = Le(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() <= func2()")
        self.assertEquals(parameters, [])

        expr = Func1() <= "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() <= ?")
        self.assertEquals(parameters, [Variable("value")])

    def test_lshift(self):
        expr = LShift(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1()<<func2()")
        self.assertEquals(parameters, [])

        expr = Func1() << "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1()<<?")
        self.assertEquals(parameters, [Variable("value")])

    def test_rshift(self):
        expr = RShift(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1()>>func2()")
        self.assertEquals(parameters, [])

        expr = Func1() >> "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1()>>?")
        self.assertEquals(parameters, [Variable("value")])

    def test_like(self):
        expr = Like(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() LIKE func2()")
        self.assertEquals(parameters, [])

    def test_in(self):
        expr = In(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() IN (func2())")
        self.assertEquals(parameters, [])

        expr = In(Func1(), "1")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() IN (1)")
        self.assertEquals(parameters, [])

    def test_and(self):
        expr = And("elem1", "elem2", And("elem3", "elem4"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "elem1 AND elem2 AND elem3 AND elem4")
        self.assertEquals(parameters, [])

        expr = Func1() & "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() AND ?")
        self.assertEquals(parameters, [Variable("value")])

    def test_or(self):
        expr = Or("elem1", "elem2", Or("elem3", "elem4"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "elem1 OR elem2 OR elem3 OR elem4")
        self.assertEquals(parameters, [])

        expr = Func1() | "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() OR ?")
        self.assertEquals(parameters, [Variable("value")])

    def test_add(self):
        expr = Add("elem1", "elem2", Add("elem3", "elem4"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "elem1+elem2+elem3+elem4")
        self.assertEquals(parameters, [])

        expr = Func1() + "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1()+?")
        self.assertEquals(parameters, [Variable("value")])

    def test_sub(self):
        expr = Sub("elem1", Sub("elem2", "elem3"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "elem1-(elem2-elem3)")
        self.assertEquals(parameters, [])

        expr = Sub(Sub("elem1", "elem2"), "elem3")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "elem1-elem2-elem3")
        self.assertEquals(parameters, [])

        expr = Func1() - "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1()-?")
        self.assertEquals(parameters, [Variable("value")])

    def test_mul(self):
        expr = Mul("elem1", "elem2", Mul("elem3", "elem4"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "elem1*elem2*elem3*elem4")
        self.assertEquals(parameters, [])

        expr = Func1() * "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1()*?")
        self.assertEquals(parameters, [Variable("value")])

    def test_div(self):
        expr = Div("elem1", Div("elem2", "elem3"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "elem1/(elem2/elem3)")
        self.assertEquals(parameters, [])

        expr = Div(Div("elem1", "elem2"), "elem3")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "elem1/elem2/elem3")
        self.assertEquals(parameters, [])

        expr = Func1() / "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1()/?")
        self.assertEquals(parameters, [Variable("value")])

    def test_mod(self):
        expr = Mod("elem1", Mod("elem2", "elem3"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "elem1%(elem2%elem3)")
        self.assertEquals(parameters, [])

        expr = Mod(Mod("elem1", "elem2"), "elem3")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "elem1%elem2%elem3")
        self.assertEquals(parameters, [])

        expr = Func1() % "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1()%?")
        self.assertEquals(parameters, [Variable("value")])

    def test_func(self):
        expr = Func("myfunc", "arg1", Func1("arg2"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "myfunc(arg1, func1(arg2))")
        self.assertEquals(parameters, [])

    def test_named_func(self):
        expr = Func1("arg1", Func2("arg2"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1(arg1, func2(arg2))")
        self.assertEquals(parameters, [])

    def test_count(self):
        expr = Count(Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "COUNT(func1())")
        self.assertEquals(parameters, [])

    def test_count_all(self):
        expr = Count()
        statement, parameters = compile(expr)
        self.assertEquals(statement, "COUNT(*)")
        self.assertEquals(parameters, [])

    def test_max(self):
        expr = Max(Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "MAX(func1())")
        self.assertEquals(parameters, [])

    def test_min(self):
        expr = Min(Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "MIN(func1())")
        self.assertEquals(parameters, [])

    def test_avg(self):
        expr = Avg(Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "AVG(func1())")
        self.assertEquals(parameters, [])

    def test_sum(self):
        expr = Sum(Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "SUM(func1())")
        self.assertEquals(parameters, [])

    def test_asc(self):
        expr = Asc(Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() ASC")
        self.assertEquals(parameters, [])

    def test_desc(self):
        expr = Desc(Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1() DESC")
        self.assertEquals(parameters, [])

    def test_sql(self):
        expr = SQL("expression")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "expression")
        self.assertEquals(parameters, [])

    def test_sql_params(self):
        expr = SQL("expression", ["params"])
        statement, parameters = compile(expr)
        self.assertEquals(statement, "expression")
        self.assertEquals(parameters, ["params"])

    def test_sql_invalid_params(self):
        expr = SQL("expression", "not a list or tuple")
        self.assertRaises(CompileError, compile, expr)

    def test_sql_tables(self):
        expr = Select(["column", Func1()], SQL("expression", [], Func2()))
        statement, parameters = compile(expr)
        self.assertEquals(statement,
                          "SELECT column, func1() FROM func2() "
                          "WHERE expression")
        self.assertEquals(parameters, [])

    def test_sql_tables_with_list_or_tuple(self):
        sql = SQL("expression", [], [Func1(), Func2()])
        expr = Select("column", sql)
        statement, parameters = compile(expr)
        self.assertEquals(statement,
                          "SELECT column FROM func1(), func2() "
                          "WHERE expression")
        self.assertEquals(parameters, [])

        sql = SQL("expression", [], (Func1(), Func2()))
        expr = Select("column", sql)
        statement, parameters = compile(expr)
        self.assertEquals(statement,
                          "SELECT column FROM func1(), func2() "
                          "WHERE expression")
        self.assertEquals(parameters, [])

    def test_sql_comparison(self):
        expr = SQL("expression1") & SQL("expression2")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(expression1) AND (expression2)")
        self.assertEquals(parameters, [])

    def test_table(self):
        expr = Table("table")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "table")
        self.assertEquals(parameters, [])

    def test_alias(self):
        expr = Alias(Table("table"), "name")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "table AS name")
        self.assertEquals(parameters, [])

    def test_alias_auto_name(self):
        expr = Alias(Table("table"))
        statement, parameters = compile(expr)
        self.assertTrue(statement[:10], "table AS _")
        self.assertEquals(parameters, [])

    def test_alias_in_column(self):
        expr = Select(Column("column", Alias(Table("table"), "alias")))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "SELECT alias.column FROM table AS alias")
        self.assertEquals(parameters, [])

    def test_join(self):
        expr = Join(Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "JOIN func1()")
        self.assertEquals(parameters, [])

    def test_join_on(self):
        expr = Join(Func1(), Func2() == "value")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "JOIN func1() ON func2() = ?")
        self.assertEquals(parameters, [Variable("value")])

    def test_join_left_right(self):
        expr = Join("table1", "table2")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "table1 JOIN table2")
        self.assertEquals(parameters, [])

    def test_join_nested(self):
        expr = Join("table1", Join("table2", "table3"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "table1 JOIN (table2 JOIN table3)")
        self.assertEquals(parameters, [])

    def test_join_double_nested(self):
        expr = Join(Join("table1", "table2"), Join("table3", "table4"))
        statement, parameters = compile(expr)
        self.assertEquals(statement,
                          "(table1 JOIN table2) JOIN (table3 JOIN table4)")
        self.assertEquals(parameters, [])

    def test_join_table(self):
        expr = Join(Table("table1"), Table("table2"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "table1 JOIN table2")
        self.assertEquals(parameters, [])

    def test_left_join(self):
        expr = LeftJoin(Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "LEFT JOIN func1()")
        self.assertEquals(parameters, [])

    def test_left_join_on(self):
        expr = LeftJoin(Func1(), Func2() == "value")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "LEFT JOIN func1() ON func2() = ?")
        self.assertEquals(parameters, [Variable("value")])

    def test_right_join(self):
        expr = RightJoin(Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "RIGHT JOIN func1()")
        self.assertEquals(parameters, [])

    def test_right_join_on(self):
        expr = RightJoin(Func1(), Func2() == "value")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "RIGHT JOIN func1() ON func2() = ?")
        self.assertEquals(parameters, [Variable("value")])

    def test_natural_join(self):
        expr = NaturalJoin(Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "NATURAL JOIN func1()")
        self.assertEquals(parameters, [])

    def test_natural_join_on(self):
        expr = NaturalJoin(Func1(), Func2() == "value")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "NATURAL JOIN func1() ON func2() = ?")
        self.assertEquals(parameters, [Variable("value")])

    def test_natural_left_join(self):
        expr = NaturalLeftJoin(Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "NATURAL LEFT JOIN func1()")
        self.assertEquals(parameters, [])

    def test_natural_left_join_on(self):
        expr = NaturalLeftJoin(Func1(), Func2() == "value")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "NATURAL LEFT JOIN func1() "
                                     "ON func2() = ?")
        self.assertEquals(parameters, [Variable("value")])

    def test_natural_right_join(self):
        expr = NaturalRightJoin(Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "NATURAL RIGHT JOIN func1()")
        self.assertEquals(parameters, [])

    def test_natural_right_join_on(self):
        expr = NaturalRightJoin(Func1(), Func2() == "value")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "NATURAL RIGHT JOIN func1() "
                                     "ON func2() = ?")
        self.assertEquals(parameters, [Variable("value")])


class CompilePythonTest(TestHelper):

    def test_precedence(self):
        expr = And("1", Or("2", "3"),
                   Add("4", Mul("5", Sub("6", Div("7", Div("8", "9"))))))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "1 and (2 or 3) and 4+5*(6-7/(8/9))")

    def test_get_precedence(self):
        self.assertTrue(compile_python.get_precedence(Or) <
                        compile_python.get_precedence(And))
        self.assertTrue(compile_python.get_precedence(Add) <
                        compile_python.get_precedence(Mul))
        self.assertTrue(compile_python.get_precedence(Sub) <
                        compile_python.get_precedence(Div))

    def test_compile_sequence(self):
        expr = ["str", Variable(1), (Variable(2), None)]
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "str, 1, 2, None")

    def test_compile_invalid(self):
        self.assertRaises(CompileError, compile_python, object())
        self.assertRaises(CompileError, compile_python, [object()])

    def test_compile_unsupported(self):
        self.assertRaises(CompileError, compile_python, Expr())
        self.assertRaises(CompileError, compile_python, Func1())

    def test_str(self):
        py_expr = compile_python.get_expr("str")
        self.assertEquals(py_expr, "str")

    def test_none(self):
        py_expr = compile_python.get_expr(None)
        self.assertEquals(py_expr, "None")

    def test_column(self):
        expr = Column("name")
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "get_column('name')")

    def test_column_table(self):
        expr = Column("name", "table")
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "get_column('name')")

    def test_variable(self):
        expr = Variable("value")
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "'value'")

    def test_eq(self):
        expr = Eq(Variable(1), Variable(2))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "1 == 2")

    def test_ne(self):
        expr = Ne(Variable(1), Variable(2))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "1 != 2")

    def test_gt(self):
        expr = Gt(Variable(1), Variable(2))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "1 > 2")

    def test_ge(self):
        expr = Ge(Variable(1), Variable(2))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "1 >= 2")

    def test_lt(self):
        expr = Lt(Variable(1), Variable(2))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "1 < 2")

    def test_le(self):
        expr = Le(Variable(1), Variable(2))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "1 <= 2")

    def test_lshift(self):
        expr = LShift(Variable(1), Variable(2))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "1<<2")

    def test_rshift(self):
        expr = RShift(Variable(1), Variable(2))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "1>>2")

    def test_in(self):
        expr = In(Variable(1), Variable(2))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "1 in (2,)")

    def test_and(self):
        expr = And("elem1", "elem2", And("elem3", "elem4"))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "elem1 and elem2 and elem3 and elem4")

    def test_or(self):
        expr = Or("elem1", "elem2", Or("elem3", "elem4"))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "elem1 or elem2 or elem3 or elem4")

    def test_add(self):
        expr = Add("elem1", "elem2", Add("elem3", "elem4"))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "elem1+elem2+elem3+elem4")

    def test_sub(self):
        expr = Sub("elem1", Sub("elem2", "elem3"))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "elem1-(elem2-elem3)")

        expr = Sub(Sub("elem1", "elem2"), "elem3")
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "elem1-elem2-elem3")

    def test_mul(self):
        expr = Mul("elem1", "elem2", Mul("elem3", "elem4"))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "elem1*elem2*elem3*elem4")

    def test_div(self):
        expr = Div("elem1", Div("elem2", "elem3"))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "elem1/(elem2/elem3)")

        expr = Div(Div("elem1", "elem2"), "elem3")
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "elem1/elem2/elem3")

    def test_mod(self):
        expr = Mod("elem1", Mod("elem2", "elem3"))
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "elem1%(elem2%elem3)")

        expr = Mod(Mod("elem1", "elem2"), "elem3")
        py_expr = compile_python.get_expr(expr)
        self.assertEquals(py_expr, "elem1%elem2%elem3")

    def test_match(self):
        col1 = Column("name1")
        col2 = Column("name2")

        match = compile_python((col1 > 10) & (col2 < 10))

        self.assertTrue(match({"name1": 15, "name2": 5}.get))
        self.assertFalse(match({"name1": 5, "name2": 15}.get))
