

class Cache(object):

    """A cache that holds strong references to objects referenced by
    object infos"""

    def __init__(self, max_size=100):
        self.max_size = max_size
        self.clear()

    def add(self, obj_info):
        """adds @obj_info to the cache"""
        if self.max_size==0:
            return
        self._objs[obj_info] = obj_info.get_obj()
        if obj_info in self._infos:
            self._infos.remove(obj_info)
        self._infos.insert(0, obj_info)
        if len(self._infos)>self.max_size:
            del self._objs[self._infos.pop()]

    def remove(self, obj_info):
        """removes the entry for @obj_info from the cache if
        cached. Returns True if obj_info was cached, otherwise False"""
        if self.max_size==0:
            return False
        if obj_info in self._objs:
            self._infos.remove(obj_info)
            del self._objs[obj_info]
            return True
        return False

    def clear(self):
        """clears the entire cache"""
        self._objs = {}
        self._infos = []

    def set_size(self, size):
        """sets the maximum number of objects that can be held in this
        cache"""
        old_size = self.max_size
        self.max_size = size
        if old_size<=size:
            # size is bigger so nothing to delete
            return
        if self.max_size==0:
            # shortcut for zero entries
            self.clear()
            return
        # remove all entries above max_size
        while len(self._infos)>self.max_size:
            del self._objs[self._infos.pop()]

    def get_stats(self):
        """returns a tuple of (<number of cached objects>, max_size)"""
        return len(self._infos), self.max_size
