import sys


class Select(object):

    def __init__(self, columns=(), tables=(), where=None,
                 limit=None, offset=None, order_by=(), group_by=()):
        self.columns = list(columns)
        self.tables = list(tables)
        self.where = where
        self.limit = limit
        self.offset = offset
        self.order_by = list(order_by)
        self.group_by = list(group_by)


def used_for(*types):
    """Convenience decorator to fill the dispatch table."""
    dispatch_table = sys._getframe(1).f_locals["_dispatch_table"]
    def decorator(method):
        for type in types:
            dispatch_table[type] = method.func_name
        return method
    return decorator


class Compiler(object):

    _dispatch_table = {}

    @used_for(str)
    def compile_literal(self, literal):
        return literal


    @used_for(Select)
    def compile_select(self, select):
        # XXX Must compile columns and tables. Test with func(column).
        statement = ["SELECT %s FROM %s" % (", ".join(select.columns),
                                            ", ".join(select.tables))]
        if select.where:
            statement.append(" WHERE ")
            statement.append(self.compile(select.where))
        if select.limit:
            statement.append(" LIMIT %d" % select.limit)
        if select.offset:
            statement.append(" OFFSET %d" % select.offset)
        if select.order_by:
            statement.append(" ORDER BY ")
            statement.append(", ".join(select.order_by))
        if select.group_by:
            statement.append(" GROUP BY ")
            statement.append(", ".join(select.group_by))
        return "".join(statement)

    def compile(self, obj):
        handler_name = self._dispatch_table[type(obj)]
        handler = getattr(self, handler_name)
        return handler(obj)


del used_for

