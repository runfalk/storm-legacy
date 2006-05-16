from tests.helper import TestHelper

from storm.expr import *


class ExprTest(TestHelper):

    def test_select_default(self):
        expr = Select()
        self.assertEquals(expr.columns, ())
        self.assertEquals(expr.tables, ())
        self.assertEquals(expr.where, None)
        self.assertEquals(expr.limit, None)
        self.assertEquals(expr.offset, None)
        self.assertEquals(expr.order_by, ())
        self.assertEquals(expr.group_by, ())

    def test_select_constructor(self):
        objects = [object() for i in range(11)]
        select = Select(objects[0:2], objects[2:4], objects[4], objects[5],
                        objects[6], objects[7:9], objects[9:11])
        objects = tuple(objects)
        self.assertEquals(select.columns, objects[0:2])
        self.assertEquals(select.tables, objects[2:4])
        self.assertEquals(select.where, objects[4])
        self.assertEquals(select.limit, objects[5])
        self.assertEquals(select.offset, objects[6])
        self.assertEquals(select.order_by, objects[7:9])
        self.assertEquals(select.group_by, objects[9:11])

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
        self.assertEquals(expr.sets, ())
        self.assertEquals(expr.where, None)

    def test_update_constructor(self):
        objects = [object() for i in range(4)]
        expr = Update(objects[0], objects[1:3], objects[3])
        objects = tuple(objects)
        self.assertEquals(expr.table, objects[0])
        self.assertEquals(expr.sets, objects[1:3])
        self.assertEquals(expr.where, objects[3])

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
        like = Like("arg1", "arg2")
        self.assertEquals(like.expr1, "arg1")
        self.assertEquals(like.expr2, "arg2")

    def test_equals(self):
        equals = Eq("arg1", "arg2")
        self.assertEquals(equals.expr1, "arg1")
        self.assertEquals(equals.expr2, "arg2")


class Func1(Func):
    name = "func1"

class Func2(Func):
    name = "func2"


class CompilerTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        self.compiler = Compiler()

    def test_literal(self):
        statement, parameters = self.compiler.compile("literal")
        self.assertEquals(statement, "literal")
        self.assertEquals(parameters, [])

    def test_select(self):
        expr = Select(["column1", "column2"])
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "SELECT column1, column2")
        self.assertEquals(parameters, [])

    def test_select_where(self):
        expr = Select(["column1", Func1()],
                      ["table1", Func1()],
                      where=Func1(), limit=3, offset=4,
                      order_by=["column2", Func1()],
                      group_by=["column3", Func1()])
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "SELECT column1, func1() "
                                     "FROM table1, func1() "
                                     "WHERE func1() LIMIT 3 OFFSET 4 "
                                     "ORDER BY column2, func1() "
                                     "GROUP BY column3, func1()")
        self.assertEquals(parameters, [])

    def test_insert(self):
        expr = Insert(Func1(), ["column1", Func1()], ["value1", Func1()])
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "INSERT INTO func1() (column1, func1()) "
                                     "VALUES (value1, func1())")
        self.assertEquals(parameters, [])

    def test_update(self):
        expr = Update(Func1(), [("column1", Func1()), ("column2", Func2())])
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "UPDATE func1() SET column1=func1(), "
                                     "column2=func2()")
        self.assertEquals(parameters, [])

    def test_update_where(self):
        expr = Update(Func1(), [("column1", Func1()), ("column2", Func2())],
                      Func1())
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "UPDATE func1() SET column1=func1(), "
                                     "column2=func2() WHERE func1()")
        self.assertEquals(parameters, [])

    def test_delete(self):
        expr = Delete(Func1())
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "DELETE FROM func1()")
        self.assertEquals(parameters, [])

    def test_delete_where(self):
        expr = Delete(Func1(), Func2())
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "DELETE FROM func1() WHERE func2()")
        self.assertEquals(parameters, [])

    def test_column(self):
        expr = Column("name")
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "name")
        self.assertEquals(parameters, [])

    def test_column_table(self):
        expr = Column("name", Func1())
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "func1().name")
        self.assertEquals(parameters, [])

    def test_param(self):
        expr = Param("value")
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "?")
        self.assertEquals(parameters, ["value"])

    def test_func(self):
        expr = Func1("arg1", Func2("arg2"))
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "func1(arg1, func2(arg2))")
        self.assertEquals(parameters, [])

    def test_none(self):
        expr = None
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "NULL")
        self.assertEquals(parameters, [])

    def test_like(self):
        expr = Like(Func1(), Func2())
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() LIKE func2())")
        self.assertEquals(parameters, [])

    def test_eq(self):
        expr = Eq(Func1(), Func2())
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() = func2())")
        self.assertEquals(parameters, [])

        expr = Func1() == "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() = ?)")
        self.assertEquals(parameters, ["value"])

    def test_eq_none(self):
        expr = Func1() == None

        self.assertTrue(expr.expr2 is None)

        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() IS NULL)")
        self.assertEquals(parameters, [])

    def test_ne(self):
        expr = Ne(Func1(), Func2())
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() != func2())")
        self.assertEquals(parameters, [])

        expr = Func1() != "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() != ?)")
        self.assertEquals(parameters, ["value"])

    def test_ne_none(self):
        expr = Func1() != None

        self.assertTrue(expr.expr2 is None)

        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() IS NOT NULL)")
        self.assertEquals(parameters, [])

    def test_gt(self):
        expr = Gt(Func1(), Func2())
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() > func2())")
        self.assertEquals(parameters, [])

        expr = Func1() > "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() > ?)")
        self.assertEquals(parameters, ["value"])

    def test_ge(self):
        expr = Ge(Func1(), Func2())
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() >= func2())")
        self.assertEquals(parameters, [])

        expr = Func1() >= "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() >= ?)")
        self.assertEquals(parameters, ["value"])

    def test_lt(self):
        expr = Lt(Func1(), Func2())
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() < func2())")
        self.assertEquals(parameters, [])

        expr = Func1() < "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() < ?)")
        self.assertEquals(parameters, ["value"])

    def test_le(self):
        expr = Le(Func1(), Func2())
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() <= func2())")
        self.assertEquals(parameters, [])

        expr = Func1() <= "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() <= ?)")
        self.assertEquals(parameters, ["value"])

    def test_lshift(self):
        expr = LShift(Func1(), Func2())
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1()<<func2())")
        self.assertEquals(parameters, [])

        expr = Func1() << "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1()<<?)")
        self.assertEquals(parameters, ["value"])

    def test_rshift(self):
        expr = RShift(Func1(), Func2())
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1()>>func2())")
        self.assertEquals(parameters, [])

        expr = Func1() >> "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1()>>?)")
        self.assertEquals(parameters, ["value"])

    def test_and(self):
        expr = And("elem1", "elem2", And("elem3", "elem4"))
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(elem1 AND elem2 AND (elem3 AND elem4))")
        self.assertEquals(parameters, [])

        expr = Func1() & "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() AND ?)")
        self.assertEquals(parameters, ["value"])

    def test_or(self):
        expr = Or("elem1", "elem2", Or("elem3", "elem4"))
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(elem1 OR elem2 OR (elem3 OR elem4))")
        self.assertEquals(parameters, [])

        expr = Func1() | "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1() OR ?)")
        self.assertEquals(parameters, ["value"])

    def test_add(self):
        expr = Add("elem1", "elem2", Add("elem3", "elem4"))
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(elem1+elem2+(elem3+elem4))")
        self.assertEquals(parameters, [])

        expr = Func1() + "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1()+?)")
        self.assertEquals(parameters, ["value"])

    def test_sub(self):
        expr = Sub("elem1", "elem2", Sub("elem3", "elem4"))
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(elem1-elem2-(elem3-elem4))")
        self.assertEquals(parameters, [])

        expr = Func1() - "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1()-?)")
        self.assertEquals(parameters, ["value"])

    def test_mul(self):
        expr = Mul("elem1", "elem2", Mul("elem3", "elem4"))
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(elem1*elem2*(elem3*elem4))")
        self.assertEquals(parameters, [])

        expr = Func1() * "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1()*?)")
        self.assertEquals(parameters, ["value"])

    def test_div(self):
        expr = Div("elem1", "elem2", Div("elem3", "elem4"))
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(elem1/elem2/(elem3/elem4))")
        self.assertEquals(parameters, [])

        expr = Func1() / "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1()/?)")
        self.assertEquals(parameters, ["value"])

    def test_mod(self):
        expr = Mod("elem1", "elem2", Mod("elem3", "elem4"))
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(elem1%elem2%(elem3%elem4))")
        self.assertEquals(parameters, [])

        expr = Func1() % "value"
        statement, parameters = self.compiler.compile(expr)
        self.assertEquals(statement, "(func1()%?)")
        self.assertEquals(parameters, ["value"])