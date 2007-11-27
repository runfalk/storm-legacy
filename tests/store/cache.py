from storm.cache import Cache

from tests.helper import TestHelper

class DummyObjectInfo(object):

    def __init__(self, id):
        self.id = id

    def get_obj(self):
        return str(self)

class CacheTest(TestHelper):

    def test_cache(self):
        # the cache should only hold max_size items
        c = Cache(5)
        dummies = [DummyObjectInfo(i) for i in range(10)]
        for dummy in dummies:
            c.add(dummy)
        self.assertEqual(c.get_stats(), (5,5))
        # so the first 5 items are no more cached
        for i in range(5):
            self.assertEqual(c.remove(dummies[i]), False)
        # but the last 5
        for i in range(5,10):
            self.assertEqual(c.remove(dummies[i]), True)
        # now the cache is empty
        self.assertEqual(c.get_stats(), (0,5))

        # let us fill the cache again
        for dummy in dummies:
            c.add(dummy)

        # and reduce the size
        c.set_size(3)
        # so the first 7 items are no more cached
        for i in range(7):
            self.assertEqual(c.remove(dummies[i]), False)
        # but the last 3
        for i in range(7,10):
            self.assertEqual(c.remove(dummies[i]), True)
        self.assertEqual(c.get_stats(), (0,3))

        # the clear method emties the cache
        for dummy in dummies:
            c.add(dummy)
        self.assertEqual(c.get_stats(), (3, 3))
        c.clear()
        self.assertEqual(c.get_stats(), (0, 3))
