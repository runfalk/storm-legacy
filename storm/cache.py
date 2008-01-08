

class Cache(object):
    """Prevents recently used objects from being deallocated.

    This prevents recently used objects from being deallocated by Python
    even if the user isn't holding any strong references to it.  It does
    that by holding strong references to the objects referenced by the
    last C{N} C{obj_info}s added to it (where C{N} is the cache size).
    """

    def __init__(self, size=100):
        self._size = size
        self._cache = {} # {obj_info: obj, ...}
        self._order = [] # [obj_info, ...]

    def clear(self):
        """Clear the entire cache at once."""
        self._cache.clear()
        del self._order[:]

    def add(self, obj_info):
        """Add C{obj_info} as the most recent entry in the cache.

        If the C{obj_info} is already in the cache, it remains in the
        cache and has its order changed to become the most recent entry
        (IOW, will be the last to leave).
        """
        if self._size != 0:
            if obj_info in self._cache:
                self._order.remove(obj_info)
            else:
                self._cache[obj_info] = obj_info.get_obj()
            self._order.insert(0, obj_info)
            if len(self._cache) > self._size:
                del self._cache[self._order.pop()]

    def remove(self, obj_info):
        """Remove C{obj_info} from the cache, if present.

        @return: True if C{obj_info} was cached, False otherwise.
        """
        if obj_info in self._cache:
            self._order.remove(obj_info)
            del self._cache[obj_info]
            return True
        return False

    def set_size(self, size):
        """Set the maximum number of objects that may be held in this cache.

        If the size is reduced, older C{obj_info}s may be dropped from
        the cache to respect the new size.
        """
        if size == 0:
            self.clear()
        else:
            # Remove all entries above the new size.
            while len(self._cache) > size:
                del self._cache[self._order.pop()]
        self._size = size

    def get_cached(self):
        """Return an ordered list of the currently cached C{obj_info}s.

        The most recently added objects come first in the list.
        """
        return list(self._order)
