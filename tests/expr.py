from tests.helper import TestHelper

from storm.expr import *


class ExprTest(TestHelper):

    def test_select_default(self):
        expr = Select()
        self.assertEquals(expr.columns, ())
        self.assertEquals(expr.tables, ())
        self.assertEquals(expr.where, None)
        self.assertEquals(expr.order_by, ())
        self.assertEquals(expr.group_by, ())
        self.assertEquals(expr.limit, None)
        self.assertEquals(expr.offset, None)

    def test_select_constructor(self):
        objects = [object() for i in range(11)]
        select = Select(objects[0:2], objects[2:4], objects[4], objects[5:7],
                        objects[7:9], objects[9], objects[10])
        objects = tuple(objects)
        self.assertEquals(select.columns, objects[0:2])
        self.assertEquals(select.tables, objects[2:4])
        self.assertEquals(select.where, objects[4])
        self.assertEquals(select.order_by, objects[5:7])
        self.assertEquals(select.group_by, objects[7:9])
        self.assertEquals(select.limit, objects[9])
        self.assertEquals(select.offset, objects[10])

    def test_insert_default(self):
        expr = Insert()
        self.assertEquals(expr.table, None)
        self.assertEquals(expr.columns, ())
        self.assertEquals(expr.values, ())

    def test_insert_constructor(self):
        objects = [object() for i in range(5)]
        expr = Insert(objects[0], objects[1:3], objects[3:5])
        objects = tuple(objects)
        self.assertEquals(expr.table, objects[0])
        self.assertEquals(expr.columns, objects[1:3])
        self.assertEquals(expr.values, objects[3:5])

    def test_update_default(self):
        expr = Update()
        self.assertEquals(expr.table, None)
        self.assertEquals(expr.columns, ())
        self.assertEquals(expr.values, ())
        self.assertEquals(expr.where, None)

    def test_update_constructor(self):
        objects = [object() for i in range(6)]
        expr = Update(objects[0], objects[1:3], objects[3:5], objects[5])
        objects = tuple(objects)
        self.assertEquals(expr.table, objects[0])
        self.assertEquals(expr.columns, objects[1:3])
        self.assertEquals(expr.values, objects[3:5])
        self.assertEquals(expr.where, objects[5])

    def test_delete_default(self):
        expr = Delete()
        self.assertEquals(expr.table, None)
        self.assertEquals(expr.where, None)

    def test_delete_constructor(self):
        objects = [object() for i in range(2)]
        expr = Delete(objects[0], objects[1])
        self.assertEquals(expr.table, objects[0])
        self.assertEquals(expr.where, objects[1])

    def test_and(self):
        expr = And("elem1", "elem2", "elem3")
        self.assertEquals(expr.exprs, ("elem1", "elem2", "elem3"))

    def test_or(self):
        expr = Or("elem1", "elem2", "elem3")
        self.assertEquals(expr.exprs, ("elem1", "elem2", "elem3"))

    def test_column_default(self):
        expr = Column()
        self.assertEquals(expr.name, None)
        self.assertEquals(expr.table, None)

    def test_column_constructor(self):
        expr = Column("name", "table")
        self.assertEquals(expr.name, "name")
        self.assertEquals(expr.table, "table")

    def test_param(self):
        expr = Param("value")
        self.assertEquals(expr.value, "value")

    def test_func(self):
        class MyFunc(Func):
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


class StateTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        self.state = State()

    def test_attrs(self):
        self.assertEquals(self.state.parameters, [])
        self.assertEquals(self.state.tables, [])
        self.assertEquals(self.state.omit_column_tables, False)

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


class Func1(Func):
    name = "func1"

class Func2(Func):
    name = "func2"


class CompileTest(TestHelper):

    def test_customize(self):
        custom_compile = compile.copy()
        @custom_compile.when(type(None))
        def compile_none(compile, state, expr):
            return "None"
        statement, parameters = custom_compile(Func1(None))
        self.assertEquals(statement, "func1(None)")

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

    def test_select_where(self):
        expr = Select(["column1", Func1()],
                      ["table1", Func1()],
                      where=Func1(),
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

    def test_insert(self):
        expr = Insert(Func1(), ["column1", Func1()], ["value1", Func1()])
        statement, parameters = compile(expr)
        self.assertEquals(statement, "INSERT INTO func1() (column1, func1()) "
                                     "VALUES (value1, func1())")
        self.assertEquals(parameters, [])

    def test_insert_with_columns(self):
        expr = Insert("table", [Column("a", "table"), Column("b", "table")],
                      ["1", "2"])
        statement, parameters = compile(expr)
        self.assertEquals(statement, "INSERT INTO table (a, b) VALUES (1, 2)")
        self.assertEquals(parameters, [])

    def test_update(self):
        expr = Update(Func1(), ["column1", Func1()], ["value1", Func2()])
        statement, parameters = compile(expr)
        self.assertEquals(statement, "UPDATE func1() SET column1=value1, "
                                     "func1()=func2()")
        self.assertEquals(parameters, [])

    def test_update_with_columns(self):
        expr = Update("table", [Column("a", "table"), Column("b", "table")],
                      ["1", "2"])
        statement, parameters = compile(expr)
        self.assertEquals(statement, "UPDATE table SET a=1, b=2")
        self.assertEquals(parameters, [])

    def test_update_where(self):
        expr = Update(Func1(), ["column1", Func1()], ["value1", Func2()],
                      Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "UPDATE func1() SET column1=value1, "
                                     "func1()=func2() WHERE func1()")
        self.assertEquals(parameters, [])

    def test_delete(self):
        expr = Delete(Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "DELETE FROM func1()")
        self.assertEquals(parameters, [])

    def test_delete_where(self):
        expr = Delete(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "DELETE FROM func1() WHERE func2()")
        self.assertEquals(parameters, [])

    def test_column(self):
        expr = Column("name")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "name")
        self.assertEquals(parameters, [])

    def test_column_table(self):
        expr = Column("name", Func1())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "func1().name")
        self.assertEquals(parameters, [])

    def test_param(self):
        expr = Param("value")
        statement, parameters = compile(expr)
        self.assertEquals(statement, "?")
        self.assertEquals(parameters, ["value"])

    def test_eq(self):
        expr = Eq(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() = func2())")
        self.assertEquals(parameters, [])

        expr = Func1() == "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() = ?)")
        self.assertEquals(parameters, ["value"])

    def test_eq_none(self):
        expr = Func1() == None

        self.assertTrue(expr.expr2 is None)

        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() IS NULL)")
        self.assertEquals(parameters, [])

    def test_ne(self):
        expr = Ne(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() != func2())")
        self.assertEquals(parameters, [])

        expr = Func1() != "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() != ?)")
        self.assertEquals(parameters, ["value"])

    def test_ne_none(self):
        expr = Func1() != None

        self.assertTrue(expr.expr2 is None)

        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() IS NOT NULL)")
        self.assertEquals(parameters, [])

    def test_gt(self):
        expr = Gt(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() > func2())")
        self.assertEquals(parameters, [])

        expr = Func1() > "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() > ?)")
        self.assertEquals(parameters, ["value"])

    def test_ge(self):
        expr = Ge(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() >= func2())")
        self.assertEquals(parameters, [])

        expr = Func1() >= "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() >= ?)")
        self.assertEquals(parameters, ["value"])

    def test_lt(self):
        expr = Lt(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() < func2())")
        self.assertEquals(parameters, [])

        expr = Func1() < "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() < ?)")
        self.assertEquals(parameters, ["value"])

    def test_le(self):
        expr = Le(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() <= func2())")
        self.assertEquals(parameters, [])

        expr = Func1() <= "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() <= ?)")
        self.assertEquals(parameters, ["value"])

    def test_lshift(self):
        expr = LShift(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1()<<func2())")
        self.assertEquals(parameters, [])

        expr = Func1() << "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1()<<?)")
        self.assertEquals(parameters, ["value"])

    def test_rshift(self):
        expr = RShift(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1()>>func2())")
        self.assertEquals(parameters, [])

        expr = Func1() >> "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1()>>?)")
        self.assertEquals(parameters, ["value"])

    def test_like(self):
        expr = Like(Func1(), Func2())
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() LIKE func2())")
        self.assertEquals(parameters, [])

    def test_and(self):
        expr = And("elem1", "elem2", And("elem3", "elem4"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(elem1 AND elem2 AND (elem3 AND elem4))")
        self.assertEquals(parameters, [])

        expr = Func1() & "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() AND ?)")
        self.assertEquals(parameters, ["value"])

    def test_or(self):
        expr = Or("elem1", "elem2", Or("elem3", "elem4"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(elem1 OR elem2 OR (elem3 OR elem4))")
        self.assertEquals(parameters, [])

        expr = Func1() | "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1() OR ?)")
        self.assertEquals(parameters, ["value"])

    def test_add(self):
        expr = Add("elem1", "elem2", Add("elem3", "elem4"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(elem1+elem2+(elem3+elem4))")
        self.assertEquals(parameters, [])

        expr = Func1() + "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1()+?)")
        self.assertEquals(parameters, ["value"])

    def test_sub(self):
        expr = Sub("elem1", "elem2", Sub("elem3", "elem4"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(elem1-elem2-(elem3-elem4))")
        self.assertEquals(parameters, [])

        expr = Func1() - "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1()-?)")
        self.assertEquals(parameters, ["value"])

    def test_mul(self):
        expr = Mul("elem1", "elem2", Mul("elem3", "elem4"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(elem1*elem2*(elem3*elem4))")
        self.assertEquals(parameters, [])

        expr = Func1() * "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1()*?)")
        self.assertEquals(parameters, ["value"])

    def test_div(self):
        expr = Div("elem1", "elem2", Div("elem3", "elem4"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(elem1/elem2/(elem3/elem4))")
        self.assertEquals(parameters, [])

        expr = Func1() / "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1()/?)")
        self.assertEquals(parameters, ["value"])

    def test_mod(self):
        expr = Mod("elem1", "elem2", Mod("elem3", "elem4"))
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(elem1%elem2%(elem3%elem4))")
        self.assertEquals(parameters, [])

        expr = Func1() % "value"
        statement, parameters = compile(expr)
        self.assertEquals(statement, "(func1()%?)")
        self.assertEquals(parameters, ["value"])

    def test_func(self):
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
