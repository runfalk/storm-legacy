import re

from storm.properties import Unicode, Str, Int, Bool, DateTime
from storm.store import Store
from storm.base import Storm
from storm.expr import SQL, Desc
from storm.tz import tzutc
from storm import Undef


__all__ = ["SQLObjectBase", "StringCol", "IntCol", "BoolCol",
           "UtcDateTimeCol"]


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
        return attr + "ID"

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
            dict["__order__"] = default_order

        dict["__table__"] = table_name, id_name

        dict[id_name] = {int: Int(),
                         str: Str(),
                         unicode: Unicode()}[dict.get("_idType", int)]

        return super(SQLObjectMeta, cls).__new__(cls, name, bases, dict)


class SQLObjectBase(Storm):
    __metaclass__ = SQLObjectMeta

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
            args = (SQL(expr),)
        result = store.find(cls, *args)
        if orderBy is not None:
            result.order_by(tuple(cls._parse_orderBy(orderBy)))
        return SQLObjectResultSet(result)

    @classmethod
    def selectBy(cls, **kwargs):
        store = cls._get_store()
        return SQLObjectResultSet(store.find(cls, **kwargs))

    @classmethod
    def selectOne(cls, expr):
        store = cls._get_store()
        return store.find(cls, SQL(expr)).one()

    @classmethod
    def selectOneBy(cls, **kwargs):
        store = cls._get_store()
        return store.find(cls, **kwargs).one()

    @classmethod
    def selectFirst(cls, expr, orderBy=None):
        store = cls._get_store()
        result = store.find(cls, SQL(expr))
        if orderBy is not None:
            result.order_by(tuple(cls._parse_orderBy(orderBy)))
        return result.first()

    @classmethod
    def selectFirstBy(cls, orderBy=None, **kwargs):
        store = cls._get_store()
        result = store.find(cls, **kwargs)
        if orderBy is not None:
            result.order_by(tuple(cls._parse_orderBy(orderBy)))
        return result.first()

    @classmethod
    def _parse_orderBy(cls, orderBy):
        if not isinstance(orderBy, (tuple, list)):
            orderBy = (orderBy,)
        for item in orderBy:
            if isinstance(item, basestring):
                if item.startswith("-"):
                    yield Desc(getattr(cls, item[1:]))
                else:
                    yield getattr(cls, item)
            else:
                yield item

    # Dummy methods.
    def sync(self): pass
    def syncUpdate(self): pass


class SQLObjectResultSet(object):

    def __init__(self, result_set):
        self._result_set = result_set

    def count(self):
        return self._result_set.count()

    def __getitem__(self, index):
        return self._result_set[index]


class PropertyAdapter(object):

    _kwargs = {}

    def __init__(self, dbName=None, notNull=False, default=Undef):
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
