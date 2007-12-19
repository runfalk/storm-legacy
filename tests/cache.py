from storm.cache import Cache

from tests.helper import TestHelper


class StubObjectInfo(object):

    def __init__(self, id):
        self.id = id
        self.hashed = False

    def get_obj(self):
        return str(self.id)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.id)

    def __hash__(self):
        self.hashed = True
        return self.id


class CacheTest(TestHelper):

    def setUp(self):
        super(CacheTest, self).setUp()
        self.obj_infos = [StubObjectInfo(i) for i in range(10)]

    def clear_hashed(self):
        for obj_info in self.obj_infos:
            obj_info.hashed = False

    def test_add(self):
        cache = Cache(5)
        for id in [0, 1, 2]:
            cache.add(self.obj_infos[id])
        self.assertEquals([obj_info.id for obj_info in cache.get_cached()],
                          [2, 1, 0])

    def test_remove(self):
        cache = Cache(5)
        for id in [0, 1, 2]:
            cache.add(self.obj_infos[id])
        cache.remove(self.obj_infos[1])
        self.assertEquals([obj_info.id for obj_info in cache.get_cached()],
                          [2, 0])

    def test_add_existing(self):
        cache = Cache(5)
        for id in [0, 1, 2, 1]:
            cache.add(self.obj_infos[id])
        self.assertEquals([obj_info.id for obj_info in cache.get_cached()],
                          [1, 2, 0])

    def test_size_and_fifo_behaviour(self):
        cache = Cache(5)
        for obj_info in self.obj_infos:
            cache.add(obj_info)
        self.assertEquals([obj_info.id for obj_info in cache.get_cached()],
                          [9, 8, 7, 6, 5])

    def test_add_with_size_zero(self):
        """Cache is disabled entirely on add() if size is 0."""
        cache = Cache(0)
        obj_info = self.obj_infos[0]
        cache.add(obj_info)
        # Ensure that we don't even check if obj_info is in the
        # cache, by testing if it was hashed.  Hopefully, that means
        # we got a faster path.
        self.assertEquals(obj_info.hashed, False)

    def test_remove_with_size_zero(self):
        """Cache is disabled entirely on remove() if size is 0."""
        cache = Cache(0)
        obj_info = self.obj_infos[0]
        cache.remove(obj_info)

    def test_reduce_max_size_to_zero(self):
        """When setting the size to zero, there's an optimization."""
        cache = Cache(5)
        obj_info = self.obj_infos[0]
        cache.add(obj_info)
        obj_info.hashed = False
        cache.set_size(0)
        self.assertEquals(cache.get_cached(), [])
        # Ensure that we don't even check if obj_info is in the
        # cache, by testing if it was hashed.  Hopefully, that means
        # we got a faster path.
        self.assertEquals(obj_info.hashed, False)

    def test_reduce_max_size(self):
        cache = Cache(5)
        for obj_info in self.obj_infos:
            cache.add(obj_info)
        cache.set_size(3)
        self.assertEquals([obj_info.id for obj_info in cache.get_cached()],
                          [9, 8, 7])

        # Adding items past the new maximum size should drop older ones.
        for obj_info in self.obj_infos[:2]:
            cache.add(obj_info)
        self.assertEquals([obj_info.id for obj_info in cache.get_cached()],
                          [1, 0, 9])

    def test_increase_max_size(self):
        cache = Cache(5)
        for obj_info in self.obj_infos:
            cache.add(obj_info)
        cache.set_size(10)
        self.assertEquals([obj_info.id for obj_info in cache.get_cached()],
                          [9, 8, 7, 6, 5])

        # Adding items past the new maximum size should drop older ones.
        for obj_info in self.obj_infos[:6]:
            cache.add(obj_info)
        self.assertEquals([obj_info.id for obj_info in cache.get_cached()],
                          [5, 4, 3, 2, 1, 0, 9, 8, 7, 6])

    def test_clear(self):
        """The clear method empties the cache."""
        cache = Cache(5)
        for obj_info in self.obj_infos:
            cache.add(obj_info)
        cache.clear()
        self.assertEquals(cache.get_cached(), [])

        # We have two structures. Ensure that both were cleared.
        for obj_info in self.obj_infos:
            self.assertEquals(cache.remove(obj_info), False)
