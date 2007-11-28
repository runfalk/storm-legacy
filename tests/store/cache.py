from storm.cache import Cache

from tests.helper import TestHelper

class DummyObjectInfo(object):

    def __init__(self, id):
        self.id = id
        self._hc = False

    def get_obj(self):
        return str(self)

    def __hash__(self):
        self._hc = True
        return self.id

class CacheTest(TestHelper):

    def setUp(self):
        super(CacheTest, self).setUp()
        self.dummies = [DummyObjectInfo(i) for i in range(10)]

    def clear_hash_calls(self):
        for d in self.dummies:
            d._hc = False

    def test_fifo_behaviour(self):
        c = Cache(5)
        for dummy in self.dummies:
            c.add(dummy)
        # number fife would fall out if not readded
        c.add(self.dummies[5])
        # add a new
        c.add(self.dummies[1])
        self.assertEqual(True, [c.remove(d) for d in self.dummies][5])

    def test_max_items_overflow(self):
        # the cache should only hold max_size items
        c = Cache(5)
        for dummy in self.dummies:
            c.add(dummy)
        self.assertEqual(c.get_stats(), (5,5))
        # so the first 5 items are no more cached
        for i in range(5):
            self.assertEqual(c.remove(self.dummies[i]), False)
        # but the last 5
        for i in range(5,10):
            self.assertEqual(c.remove(self.dummies[i]), True)

    def test_item_remove_zero_size(self):
        # cache is disabled if size is 0
        # in order to test this we have to touch private attributes
        c = Cache(0)
        c1 = Cache(1)
        d = self.dummies[0]
        c.add(d)
        c1.add(d)
        c._objs = c1._objs
        c._infos = c1._infos
        self.clear_hash_calls()
        c.remove(d)
        self.assertEqual(d._hc, False)

    def test_item_add_zero_size(self):
        # cache is disabled if size is 0
        c = Cache(0)
        d = self.dummies[0]
        c.add(d)
        self.assertEqual(d._hc, False)

    def test_item_removed(self):
        c = Cache(5)
        c.add(self.dummies[0])
        self.assertEqual(c.get_stats(), (1, 5))
        c.remove(self.dummies[0])
        self.assertEqual(c.get_stats(), (0, 5))
        self.assertEqual(len(c._objs), 0)

    def test_reduce_max_size_empty(self):
        c = Cache(5)
        c.set_size(3)
        self.assertEqual(c.get_stats(), (0,3))

    def test_reduce_max_size_to_zero(self):
        c = Cache(5)
        c.add(self.dummies[0])
        self.clear_hash_calls()
        c.set_size(0)
        self.assertEqual(c.get_stats(), (0,0))
        # the infos are not touched, because clear is called
        for dummy in self.dummies:
            self.assertEqual(dummy._hc, False)

    def test_increase_max_size_full(self):
        c = Cache(5)
        for dummy in self.dummies:
            c.add(dummy)
        self.clear_hash_calls()
        c.set_size(10)
        # the objects are still in cache
        self.assertEqual(c.get_stats(), (5, 10))

    def test_reduce_max_size_full(self):
        c = Cache(5)
        for dummy in self.dummies:
            c.add(dummy)
        self.assertEqual(c.get_stats(), (5,5))
        # max size to 3
        c.set_size(3)
        # so the first 7 items are no more cached
        for i in range(7):
            self.assertEqual(c.remove(self.dummies[i]), False)
        # but the last 3
        for i in range(7,10):
            self.assertEqual(c.remove(self.dummies[i]), True)

    def test_clear(self):
        # the clear method emties the cache
        c = Cache(3)
        for dummy in self.dummies:
            c.add(dummy)
        self.assertEqual(c.get_stats(), (3, 3))
        c.clear()
        self.assertEqual(c.get_stats(), (0, 3))
