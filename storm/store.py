from storm.expr import Select, Insert, Delete
from storm.expr import Param, Count, Max, Min, Avg, Sum
from storm.properties import ClassInfo, ObjectInfo


#class InvalidKeyError(Exception):
#    pass
#
#class WrongStoreError(Exception):
#    pass
#
#class AlreadyInStoreError(Exception):
#    pass


class Store(object):

    def __init__(self, database):
        self._database = database
        self._connection = database.connect()

    @staticmethod
    def of(obj):
        try:
            return ObjectInfo(obj).__store
        except AttributeError:
            return None

    def get_connection(self):
        return self._connection

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

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

        select = Select(cls_info.prop_insts, cls_info.tables, where, limit=1)

        prop_values = self._connection.execute(select).fetch_one()
        if prop_values is None:
            return None

        return self._build_object(cls_info, prop_values)

    def find(self, cls, *args, **kwargs):
        cls_info = ClassInfo(cls)

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

        return ResultSet(self._connection.execute,
                         lambda vals: self._build_object(cls_info, vals),
                         columns=cls_info.prop_insts,
                         tables=cls_info.tables, where=where)

    def add(self, obj):
        # TODO: Check if object is in some store already. force=True could
        #       make it accept, unless the store is already self.
        cls_info = ClassInfo(obj)
        insert = Insert(cls_info.table, cls_info.columns,
                        [Param(prop.__get__(obj))
                         for prop in cls_info.prop_insts])
        self._connection.execute(insert)

    def remove(self, obj):
        cls_info = ClassInfo(obj)
        where = None
        for prop in cls_info.primary_key:
            if where is None:
                where = (prop == prop.__get__(obj))
            else:
                where &= (prop == prop.__get__(obj))
        delete = Delete(cls_info.table, where)
        self._connection.execute(delete)
        ObjectInfo(obj).__store = None

    def _build_object(self, cls_info, prop_values):
        obj = object.__new__(cls_info.cls)
        ObjectInfo(obj).__store = self
        for name, value in zip(cls_info.attr_names, prop_values):
            setattr(obj, name, value)
        return obj
        

class ResultSet(object):

    def __init__(self, execute, object_factory, **kwargs):
        self._execute = execute
        self._object_factory = object_factory
        self._kwargs = kwargs

    def __iter__(self):
        for values in self._execute(Select(**self._kwargs)):
            yield self._object_factory(values)

    def _aggregate(self, column):
        return self._execute(Select((column,), self._kwargs.get("tables"),
                                    self._kwargs.get("where"))).fetch_one()[0]

    def one(self):
        kwargs = self._kwargs.copy()
        kwargs["limit"] = 1
        values = self._execute(Select(**kwargs)).fetch_one()
        return self._object_factory(values)

    def order_by(self, *args):
        kwargs = self._kwargs.copy()
        kwargs["order_by"] = args
        return self.__class__(self._execute, self._object_factory, **kwargs)

    def remove(self):
        delete = Delete(self._kwargs.get("tables")[0],
                        self._kwargs.get("where"))
        self._execute(delete)

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
