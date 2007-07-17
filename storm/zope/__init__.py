from zope.security.checker import NoProxy, BasicTypes, _available_by_default

from storm.info import ObjectInfo


# The followig is required for storm.info.get_obj_info() to have
# access to a proxied object which is already in the store (IOW, has
# the object info set already).  With this, Storm is able to
# gracefully handle situations when a proxied object is passed to a
# Store.
_available_by_default.append("__object_info")
BasicTypes[ObjectInfo] = NoProxy
