from storm.expr import Select, Count, Max, Min, Avg, Sum
from storm.properties import ClassInfo, ObjectInfo


class InvalidKey(Exception):
    pass


class Store(object):

    def __init__(self, database):
        self._database = database
        self._connection = database.connect()

    @staticmethod
    def of(obj):
        return ObjectInfo(obj).__store

    def get(self, cls, key):
        if type(key) != tuple:
            key = (key,)

        cls_info = ClassInfo(cls)

        where = None
        for i, prop in enumerate(cls_info.primary_key):
            if where is None:
                where = (prop == key[i])
            else:
                where &= (prop == key[i])

        select = Select(cls_info.prop_insts, (cls_info.table,), where, limit=1)

        prop_values = self._connection.execute_expr(select).fetch_one()
        if prop_values is None:
            return None

        return self._build_object(cls_info, prop_values)

    def find(self, cls, *args, **kwargs):
        cls_info = ClassInfo(cls)
        return DynamicSelect(self._connection.execute_expr,
                             lambda vals: self._build_object(cls_info, vals),
                             columns=cls_info.prop_insts,
                             tables=(cls_info.table,),
                             where=self._build_where(cls_info, args, kwargs))

    def _build_object(self, cls_info, prop_values):
        obj = object.__new__(cls_info.cls)
        ObjectInfo(obj).__store = self
        for name, value in zip(cls_info.prop_names, prop_values):
            setattr(obj, name, value)
        #load = getattr(obj, "__load__", None)
        #if load is not None:
        #    load()
        return obj

    def _build_where(self, cls_info, args, kwargs):
        where = None
        if args:
            for arg in args:
                if where is None:
                    where = arg
                else:
                    where &= arg
        if kwargs:
            for key in kwargs:
                if where is None:
                    where = cls_info.prop_dict[key] == kwargs[key]
                else:
                    where &= cls_info.prop_dict[key] == kwargs[key]
        return where
        

class DynamicSelect(object):

    def __init__(self, result_factory, object_factory, **kwargs):
        self._result_factory = result_factory
        self._object_factory = object_factory
        self._kwargs = kwargs

    def __iter__(self):
        for values in self._result_factory(Select(**self._kwargs)):
            yield self._object_factory(values)

    def _aggregate(self, column):
        return self._result_factory(Select((column,),
                                           self._kwargs.get("tables"),
                                           self._kwargs.get("where"))
                                   ).fetch_one()[0]

    def one(self):
        kwargs = self._kwargs.copy()
        kwargs["limit"] = 1
        values = self._result_factory(Select(**kwargs)).fetch_one()
        return self._object_factory(values)

    def order_by(self, *args):
        kwargs = self._kwargs.copy()
        kwargs["order_by"] = args
        return self.__class__(self._result_factory, self._object_factory,
                              **kwargs)

    def count(self):
        return self._aggregate(Count())

    def max(self, prop):
        return self._aggregate(Max(prop))

    def min(self, prop):
        return self._aggregate(Min(prop))

    def avg(self, prop):
        return self._aggregate(Avg(prop))

    def sum(self, prop):
        return self._aggregate(Sum(prop))
