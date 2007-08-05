has_zope = True
try:
    from zope.interface import classImplements
    from zope.security.checker import (
        NoProxy, BasicTypes, _available_by_default)
except ImportError:
    has_zope = False

from storm.info import ObjectInfo
from storm.zope.interfaces import ISQLObjectResultSet
from storm import sqlobject as storm_sqlobject

# The following is required for storm.info.get_obj_info() to have
# access to a proxied object which is already in the store (IOW, has
# the object info set already).  With this, Storm is able to
# gracefully handle situations when a proxied object is passed to a
# Store.
if has_zope:
    _available_by_default.append("__object_info")
    BasicTypes[ObjectInfo] = NoProxy
    classImplements(storm_sqlobject.SQLObjectResultSet, ISQLObjectResultSet)
