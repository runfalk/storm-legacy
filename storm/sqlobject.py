import re

from storm.properties import Unicode, Str, Int, Bool, DateTime, Date
from storm.references import Reference, ReferenceSet
from storm.store import Store
from storm.base import Storm
from storm.expr import SQL, Desc, And, Or, Not, In, Like
from storm.tz import tzutc
from storm import Undef


__all__ = ["SQLObjectBase", "StringCol", "IntCol", "BoolCol", "DateCol",
           "UtcDateTimeCol", "ForeignKey", "SQLMultipleJoin",
           "SQLRelatedJoin", "DESC", "AND", "OR", "NOT", "IN", "LIKE",
           "SQLConstant"]


DESC, AND, OR, NOT, IN, LIKE, SQLConstant = Desc, And, Or, Not, In, Like, SQL


class SQLObjectStyle(object):

    longID = False

    def idForTable(self, table_name):
        if self.longID:
            return self.tableReference(table_name)
        else:
            return 'id'

    def pythonClassToAttr(self, class_name):
        return self._lowerword(class_name)

    def instanceAttrToIDAttr(self, attr_name):
        return attr_name + "ID"

    def pythonAttrToDBColumn(self, attr_name):
        return self._mixed_to_under(attr_name)

    def dbColumnToPythonAttr(self, column_name):
        return self._under_to_mixed(column_name)

    def pythonClassToDBTable(self, class_name):
        return class_name[0].lower()+self._mixed_to_under(class_name[1:])

    def dbTableToPythonClass(self, table_name):
        return table[0].upper()+self._under_to_mixed(table_name[1:])

    def pythonClassToDBTableReference(self, class_name):
        return self.tableReference(self.pythonClassToDBTable(class_name))

    def tableReference(self, table_name):
        return table_name+"_id"

    def _mixed_to_under(self, name, _re=re.compile(r'[A-Z]+')):
        if name.endswith('ID'):
            return self._mixed_to_under(name[:-2]+"_id")
        name = _re.sub(self._mixed_to_under_sub, name)
        if name.startswith('_'):
            return name[1:]
        return name
    
    def _mixed_to_under_sub(self, match):
        m = match.group(0).lower()
        if len(m) > 1:
            return '_%s_%s' % (m[:-1], m[-1])
        else:
            return '_%s' % m

    def _under_to_mixed(self, name, _re=re.compile('_.')):
        if name.endswith('_id'):
            return self._under_to_mixed(name[:-3] + "ID")
        return _re.sub(self._under_to_mixed_sub, name)

    def _under_to_mixed_sub(self, match):
        return match.group(0)[1].upper()

    @staticmethod
    def _capword(s):
        return s[0].upper() + s[1:]
    
    @staticmethod
    def _lowerword(s):
        return s[0].lower() + s[1:]


class SQLObjectMeta(type(Storm)):

    @staticmethod
    def _get_attr(attr, bases, dict):
        value = dict.get(attr)
        if value is None:
            for base in bases:
                value = getattr(base, attr, None)
                if value is not None:
                    break
        return value

    def __new__(cls, name, bases, dict):
        if dict.get("__metaclass__") is SQLObjectMeta:
            # Do not parse SQLObjectBase itself.
            return type.__new__(cls, name, bases, dict)

        style = cls._get_attr("_style", bases, dict)
        if style is None:
            dict["_style"] = style = SQLObjectStyle()

        table_name = cls._get_attr("_table", bases, dict)
        if table_name is None:
            table_name = style.pythonClassToDBTable(name)

        id_name = cls._get_attr("_idName", bases, dict)
        if id_name is None:
            id_name = style.idForTable(table_name)

        default_order = cls._get_attr("_defaultOrder", bases, dict)
        if default_order is not None:
            if isinstance(default_order, list):
                # Storm requires __order__ to be a tuple for sequences.
                default_order = tuple(default_order)
            dict["__order__"] = default_order

        dict["__table__"] = table_name, id_name

        for attr, prop in dict.items():
            if isinstance(prop, ForeignKey):
                db_name = prop.kwargs.get("dbName", attr)
                local_prop_name = style.instanceAttrToIDAttr(attr)
                dict[local_prop_name] = local_prop = Int(db_name)
                dict[attr] = Reference(local_prop,
                                       "%s.<primary key>" % prop.foreignKey)
            elif isinstance(prop, PropertyAdapter):
                db_name = prop.dbName or attr
                method_name = prop.alternateMethodName
                if method_name is None and prop.alternateID:
                    method_name = "by" + db_name[0].upper() + db_name[1:]
                if method_name is not None:
                    def func(cls, key):
                        store = cls._get_store()
                        return store.find(cls, getattr(cls, attr) == key).one()
                    func.func_name = method_name
                    dict[method_name] = classmethod(func)


        dict[id_name] = {int: Int(),
                         str: Str(),
                         unicode: Unicode()}[dict.get("_idType", int)]

        obj = super(SQLObjectMeta, cls).__new__(cls, name, bases, dict)

        obj._storm_property_registry.add_property(obj, getattr(obj, id_name),
                                                  "<primary key>")
        return obj


class DotQ(object):
    """A descriptor that mimics the SQLObject 'Table.q' syntax"""

    def __get__(self, obj, cls=None):
        return BoundDotQ(cls)


class BoundDotQ(object):

    def __init__(self, cls):
        self._cls = cls

    def __getattr__(self, attr):
        if attr.startswith('__'):
            raise AttributeError(attr)
        elif attr == 'id':
            return getattr(self._cls, self._cls.__table__[1])
        else:
            return getattr(self._cls, attr)


class SQLObjectBase(Storm):
    __metaclass__ = SQLObjectMeta

    q = DotQ()

    def __init__(self, **kwargs):
        for attr, value in kwargs.iteritems():
            setattr(self, attr, value)
        self._get_store().add(self)

    @staticmethod
    def _get_store():
        raise NotImplementedError("SQLObjectBase._get_store() "
                                  "must be implemented")

    @classmethod
    def get(cls, id):
        store = cls._get_store()
        return store.get(cls, id)

    @classmethod
    def select(cls, expr=None, orderBy=None):
        store = cls._get_store()
        if expr is None:
            args = ()
        else:
            if isinstance(expr, basestring):
                expr = SQL(expr)
            args = (expr,)
        result = store.find(cls, *args)
        if orderBy is not None:
            result.order_by(*tuple(cls._parse_orderBy(orderBy)))
        return SQLObjectResultSet(result, cls)

    @classmethod
    def selectBy(cls, **kwargs):
        store = cls._get_store()
        return SQLObjectResultSet(store.find(cls, **kwargs), cls)

    @classmethod
    def selectOne(cls, expr):
        store = cls._get_store()
        if expr is None:
            args = ()
        else:
            if isinstance(expr, basestring):
                expr = SQL(expr)
            args = (expr,)
        return store.find(cls, *args).one()

    @classmethod
    def selectOneBy(cls, **kwargs):
        store = cls._get_store()
        return store.find(cls, **kwargs).one()

    @classmethod
    def selectFirst(cls, expr, orderBy=None):
        store = cls._get_store()
        if expr is None:
            args = ()
        else:
            if isinstance(expr, basestring):
                expr = SQL(expr)
            args = (expr,)
        result = store.find(cls, *args)
        if orderBy is not None:
            result.order_by(*cls._parse_orderBy(orderBy))
        return result.first()

    @classmethod
    def selectFirstBy(cls, orderBy=None, **kwargs):
        store = cls._get_store()
        result = store.find(cls, **kwargs)
        if orderBy is not None:
            result.order_by(*cls._parse_orderBy(orderBy))
        return result.first()

    @classmethod
    def _parse_orderBy(cls, orderBy):
        result = []
        if not isinstance(orderBy, (tuple, list)):
            orderBy = (orderBy,)
        for item in orderBy:
            if isinstance(item, basestring):
                if item.startswith("-"):
                    result.append(Desc(getattr(cls, item[1:])))
                else:
                    result.append(getattr(cls, item))
            else:
                result.append(item)
        return tuple(result)

    # Dummy methods.
    def sync(self): pass
    def syncUpdate(self): pass


class SQLObjectResultSet(object):

    def __init__(self, result_set, cls):
        self._result_set = result_set
        self._cls = cls

    def count(self):
        return self._result_set.count()

    def __getitem__(self, index):
        return self._result_set[index]

    def orderBy(self, orderBy):
        result_set = self._result_set.copy()
        result_set.order_by(*self._cls._parse_orderBy(orderBy))
        return SQLObjectResultSet(result_set, self._cls)


class PropertyAdapter(object):

    _kwargs = {}

    def __init__(self, dbName=None, notNull=False, default=Undef,
                 alternateID=None, unique=None, name=None,
                 alternateMethodName=None, length=None, immutable=None):

        self.dbName = dbName
        self.alternateID = alternateID
        self.alternateMethodName = alternateMethodName

        # XXX Implement handler for:
        #
        #   - immutable (causes setting the attribute to fail)
        #
        # XXX Implement tests for ignored parameters:
        #
        #   - unique (for tablebuilder)
        #   - length (for tablebuilder for StringCol)
        #   - name (for _columns stuff)

        if callable(default):
            default_factory = default
            default = Undef
        else:
            default_factory = Undef
        super(PropertyAdapter, self).__init__(dbName, allow_none=not notNull,
                                              default_factory=default_factory,
                                              default=default, **self._kwargs)


class StringCol(PropertyAdapter, Unicode):
    pass

class IntCol(PropertyAdapter, Int):
    pass

class BoolCol(PropertyAdapter, Bool):
    pass

class UtcDateTimeCol(PropertyAdapter, DateTime):
    _kwargs = {"tzinfo": tzutc()}

class DateCol(PropertyAdapter, Date):
    pass


class ForeignKey(object):

    def __init__(self, foreignKey, **kwargs):
        self.foreignKey = foreignKey
        self.kwargs = kwargs


class SQLMultipleJoin(ReferenceSet):

    def __init__(self, otherClass=None, joinColumn=None,
                 intermediateTable=None, otherColumn=None, orderBy=None):
        if intermediateTable:
            args = ("<primary key>",
                    "%s.%s" % (intermediateTable, joinColumn),
                    "%s.%s" % (intermediateTable, otherColumn),
                    "%s.<primary key>" % otherClass)
        else:
            args = ("<primary key>", "%s.%s" % (otherClass, joinColumn))
        ReferenceSet.__init__(self, *args)
        self._orderBy = orderBy

    def __get__(self, obj, cls=None):
        if obj is None:
            return self
        bound_reference_set = ReferenceSet.__get__(self, obj)
        target_cls = bound_reference_set._target_cls
        result_set = bound_reference_set.find()
        if self._orderBy:
            result_set.order_by(*target_cls._parse_orderBy(self._orderBy))
        return SQLObjectResultSet(result_set, target_cls)

SQLRelatedJoin = SQLMultipleJoin
