import sys


class Select(object):

    def __init__(self, columns=None, tables=None, where=None, limit=None,
                 offset=None, order_by=None, group_by=None):
        self.columns = list(columns or ())
        self.tables = list(tables or ())
        self.where = where
        self.limit = limit
        self.offset = offset
        self.order_by = list(order_by or ())
        self.group_by = list(group_by or ())


class And(object):

    def __init__(self, *elements):
        self.elements = list(elements)


class Or(object):

    def __init__(self, *elements):
        self.elements = list(elements)


class Parameter(object):

    def __init__(self, value):
        self.value = value


def used_for(*types):
    """Convenience decorator to fill the dispatch table.

    Subclasses of Compiler don't need it.
    """
    dispatch_table = sys._getframe(1).f_locals["_dispatch_table"]
    def decorator(method):
        for type in types:
            dispatch_table[type] = method.func_name
        return method
    return decorator


class Compiler(object):

    _dispatch_table = {}

    @used_for(str)
    def compile_literal(self, literal, parameters):
        return literal

    @used_for(Parameter)
    def compile_parameter(self, param, parameters):
        parameters.append(param.value)
        return "?"

    @used_for(And)
    def compile_and(self, and_, parameters):
        # TODO: Compile elements rather than considering them as strings.
        return "(%s)" % " AND ".join(and_.elements)

    @used_for(Or)
    def compile_or(self, or_, parameters):
        # TODO: Compile elements rather than considering them as strings.
        return "(%s)" % " OR ".join(or_.elements)

    @used_for(Select)
    def compile_select(self, select, parameters):
        # TODO: Compile elements rather than considering them as strings.
        tokens = ["SELECT %s" % (", ".join(select.columns))]
        if select.tables:
            tokens.append(" FROM ")
            tokens.append(", ".join(select.tables))
        if select.where:
            tokens.append(" WHERE ")
            tokens.append(self._compile(select.where, parameters))
        if select.limit:
            tokens.append(" LIMIT %d" % select.limit)
        if select.offset:
            tokens.append(" OFFSET %d" % select.offset)
        if select.order_by:
            tokens.append(" ORDER BY ")
            tokens.append(", ".join(select.order_by))
        if select.group_by:
            tokens.append(" GROUP BY ")
            tokens.append(", ".join(select.group_by))
        return "".join(tokens)

    def _compile(self, expr, parameters):
        handler_name = self._dispatch_table[type(expr)]
        handler = getattr(self, handler_name)
        return handler(expr, parameters)

    def compile(self, expr):
        parameters = []
        return self._compile(expr, parameters), parameters

del used_for
