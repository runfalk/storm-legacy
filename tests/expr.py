from tests.helper import TestHelper

from storm.expr import *


class ExprTest(TestHelper):

    def test_select_default(self):
        select = Select()
        self.assertEquals(select.columns, [])
        self.assertEquals(select.tables, [])
        self.assertEquals(select.where, None)
        self.assertEquals(select.limit, None)
        self.assertEquals(select.offset, None)
        self.assertEquals(select.order_by, [])
        self.assertEquals(select.group_by, [])

    def test_select_constructor(self):
        objects = tuple([object() for i in range(11)])
        select = Select(objects[0:2], objects[2:4], objects[4], objects[5],
                        objects[6], objects[7:9], objects[9:11])
        objects = list(objects)
        self.assertEquals(select.columns, objects[0:2])
        self.assertEquals(select.tables, objects[2:4])
        self.assertEquals(select.where, objects[4])
        self.assertEquals(select.limit, objects[5])
        self.assertEquals(select.offset, objects[6])
        self.assertEquals(select.order_by, objects[7:9])
        self.assertEquals(select.group_by, objects[9:11])


class CompilerTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        self.compiler = Compiler()

    def test_literal(self):
        result = self.compiler.compile("literal")
        self.assertEquals(result, "literal")

    def test_select_basic(self):
        select = Select(["column1", "column2"], ["table1", "table2"])
        result = self.compiler.compile(select)
        self.assertEquals(result,
                          "SELECT column1, column2 FROM table1, table2")

    def test_select_fancy(self):
        select = Select(["column1", "column2"], ["table1", "table2"],
                        where="where", limit=3, offset=4,
                        order_by=["column3", "column4"],
                        group_by=["column5", "column6"])
        result = self.compiler.compile(select)
        self.assertEquals(result,
                          "SELECT column1, column2 FROM table1, table2 "
                          "WHERE where LIMIT 3 OFFSET 4 "
                          "ORDER BY column3, column4 "
                          "GROUP BY column5, column6")

# Column with "as" (do we want it?)
# Select arguments (column == 'asd')
