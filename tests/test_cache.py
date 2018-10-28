import pytest

from storm.compat import iter_range, ustr
from storm.properties import Int
from storm.info import get_obj_info
from storm.cache import Cache, GenerationalCache


class StubObjectInfo(object):

    def __init__(self, id):
        self.id = id
        self.hashed = False

    def get_obj(self):
        return ustr(self.id)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.id)

    def __hash__(self):
        self.hashed = True
        return self.id

    def __lt__(self, other):
        return self.id < other.id


class StubClass(object):

    __storm_table__ = "stub_class"

    id = Int(primary=True)


@pytest.fixture
def obj_infos():
    return [StubObjectInfo(i) for i in iter_range(10)]

@pytest.fixture
def obj1(obj_infos):
    return obj_infos[0]


@pytest.fixture
def obj2(obj_infos):
    return obj_infos[1]


@pytest.fixture
def obj3(obj_infos):
    return obj_infos[2]


multi_cache_test = pytest.mark.parametrize("Cache", [Cache, GenerationalCache])


@multi_cache_test
def test_initially_empty(Cache):
    cache = Cache()
    assert cache.get_cached() == []


@multi_cache_test
def test_add(Cache, obj1, obj2, obj3):
    cache = Cache(5)
    cache.add(obj1)
    cache.add(obj2)
    cache.add(obj3)
    assert sorted(cache.get_cached()) == [obj1, obj2, obj3]


@multi_cache_test
def test_adding_similar_obj_infos(Cache):
    """If __eq__ is broken, this fails."""
    obj_info1 = get_obj_info(StubClass())
    obj_info2 = get_obj_info(StubClass())
    cache = Cache(5)
    cache.add(obj_info1)
    cache.add(obj_info2)
    cache.add(obj_info2)
    cache.add(obj_info1)

    cached = [hash(obj_info) for obj_info in cache.get_cached()]
    expected = [hash(obj_info1), hash(obj_info2)]
    assert sorted(cached) == sorted(expected)


@multi_cache_test
def test_remove(Cache, obj1, obj2, obj3):
    cache = Cache(5)
    cache.add(obj1)
    cache.add(obj2)
    cache.add(obj3)
    cache.remove(obj2)
    assert sorted(cache.get_cached()) == [obj1, obj3]


@multi_cache_test
def test_add_existing(Cache, obj1, obj2, obj3):
    cache = Cache(5)
    cache.add(obj1)
    cache.add(obj2)
    cache.add(obj3)
    cache.add(obj2)
    assert sorted(cache.get_cached()) == [obj1, obj2, obj3]


@multi_cache_test
def test_add_with_size_zero(Cache, obj1):
    """Cache is disabled entirely on add() if size is 0."""
    cache = Cache(0)
    cache.add(obj1)
    # Ensure that we don't even check if obj_info is in the
    # cache, by testing if it was hashed.  Hopefully, that means
    # we got a faster path.
    assert not obj1.hashed


@multi_cache_test
def test_remove_with_size_zero(Cache, obj1):
    """Cache is disabled entirely on remove() if size is 0."""
    cache = Cache(0)
    cache.remove(obj1)


@multi_cache_test
def test_clear(Cache, obj_infos):
    """The clear method empties the cache."""
    cache = Cache(5)
    for obj_info in obj_infos:
        cache.add(obj_info)
    cache.clear()
    assert cache.get_cached() == []

    # Just an additional check ensuring that any additional structures
    # which may be used were cleaned properly as well.
    for obj_info in obj_infos:
        assert not cache.remove(obj_info)


@multi_cache_test
def test_set_zero_size(Cache, obj1, obj2):
    """
    Setting a cache's size to zero clears the cache.
    """
    cache = Cache()
    cache.add(obj1)
    cache.add(obj2)
    cache.set_size(0)
    assert cache.get_cached() == []


@multi_cache_test
def test_fit_size(Cache):
    """
    A cache of size n can hold at least n objects.
    """
    size = 10
    cache = Cache(size)
    for i in iter_range(size):
        cache.add(StubObjectInfo(i))
    assert len(cache.get_cached()) == size


def test_cache_size_and_fifo_behaviour(obj_infos):
    cache = Cache(5)
    for obj_info in obj_infos:
        cache.add(obj_info)
    assert [obj_info.id for obj_info in cache.get_cached()] == [9, 8, 7, 6, 5]


def test_cache_reduce_max_size_to_zero(obj1):
    """When setting the size to zero, there's an optimization."""
    cache = Cache(5)
    cache.add(obj1)
    obj1.hashed = False
    cache.set_size(0)
    assert cache.get_cached() == []
    # Ensure that we don't even check if obj1 is in the cache, by
    # testing if it was hashed.  Hopefully, that means we got a
    # faster path.
    assert not obj1.hashed


def test_cache_reduce_max_size(obj_infos):
    cache = Cache(5)
    for obj_info in obj_infos:
        cache.add(obj_info)
    cache.set_size(3)
    assert [obj_info.id for obj_info in cache.get_cached()] == [9, 8, 7]

    # Adding items past the new maximum size should drop older ones.
    for obj_info in obj_infos[:2]:
        cache.add(obj_info)
    assert [obj_info.id for obj_info in cache.get_cached()] == [1, 0, 9]


def test_cache_increase_max_size(obj_infos):
    cache = Cache(5)
    for obj_info in obj_infos:
        cache.add(obj_info)
    cache.set_size(10)
    assert [obj_info.id for obj_info in cache.get_cached()] == [9, 8, 7, 6, 5]

    # Adding items past the new maximum size should drop older ones.
    for obj_info in obj_infos[:6]:
        cache.add(obj_info)
    cached_ids = [obj_info.id for obj_info in cache.get_cached()]
    assert cached_ids == [5, 4, 3, 2, 1, 0, 9, 8, 7, 6]


def test_cache_one_object(obj1):
    cache = GenerationalCache()
    cache.add(obj1)
    assert cache.get_cached() == [obj1]


def test_cache_multiple_objects(obj1, obj2):
    cache = GenerationalCache()
    cache.add(obj1)
    cache.add(obj2)
    assert sorted(cache.get_cached()) == [obj1, obj2]


def test_generational_cache_clear_cache(obj1):
    cache = GenerationalCache()
    cache.add(obj1)
    cache.clear()
    assert cache.get_cached() == []


def test_generational_cache_clear_cache_clears_the_second_generation(obj1, obj2):
    cache = GenerationalCache(1)
    cache.add(obj1)
    cache.add(obj2)
    cache.clear()
    assert cache.get_cached() == []


def test_generational_cache_remove_object(obj1, obj2, obj3):
    cache = GenerationalCache()
    cache.add(obj1)
    cache.add(obj2)
    cache.add(obj3)

    present = cache.remove(obj2)
    assert present
    assert sorted(cache.get_cached()) == [obj1, obj3]


def test_generational_cache_remove_nothing(obj1, obj2):
    cache = GenerationalCache()
    cache.add(obj1)

    present = cache.remove(obj2)
    assert not present
    assert cache.get_cached() == [obj1]


def test_generational_cache_size_limit():
    """
    A cache will never hold more than twice its size in objects.  The
    generational system is what prevents it from holding exactly the
    requested number of objects.
    """
    size = 10
    cache = GenerationalCache(size)
    for value in iter_range(5 * size):
        cache.add(StubObjectInfo(value))
    assert len(cache.get_cached()) == size * 2


def test_generational_cache_set_size_smaller_than_current_size():
    """
    Setting the size to a smaller size than the number of objects
    currently cached will drop some of the extra content.  Note that
    because of the generation system, it can actually hold two times
    the size requested in edge cases.
    """
    cache = GenerationalCache(150)
    for i in iter_range(250):
        cache.add(StubObjectInfo(i))
    cache.set_size(100)
    cached = cache.get_cached()
    assert len(cached) == 100
    for obj_info in cache.get_cached():
        assert obj_info.id >= 100


def test_generational_cache_set_size_larger_than_current_size(obj1, obj2, obj3):
    """
    Setting the cache size to something more than the number of
    objects in the cache does not affect its current contents,
    and will merge any elements from the second generation into
    the first one.
    """
    cache = GenerationalCache(1)
    cache.add(obj1)     # new=[1]    old=[]
    cache.add(obj2)     # new=[2]    old=[1]
    cache.set_size(2)   # new=[1, 2] old=[]
    cache.add(obj3)     # new=[3]    old=[1, 2]
    assert sorted(cache.get_cached()) == [obj1, obj2, obj3]


def test_generational_cache_set_size_limit():
    """
    Setting the size limits the cache's size just like passing an
    initial size would.
    """
    size = 10
    cache = GenerationalCache(size * 100)
    cache.set_size(size)
    for value in iter_range(size * 10):
        cache.add(StubObjectInfo(value))
    assert len(cache.get_cached()) == size * 2


def test_generational_cache_two_generations(obj1, obj2):
    """
    Inserting more objects than the cache's size causes the cache
    to contain two generations, each holding up to <size> objects.
    """
    cache = GenerationalCache(1)
    cache.add(obj1)
    cache.add(obj2)

    assert sorted(cache.get_cached()) == [obj1, obj2]


def test_generational_cache_three_generations(obj1, obj2, obj3):
    """
    If more than 2*<size> objects come along, only 2*<size>
    objects are retained.
    """
    cache = GenerationalCache(1)
    cache.add(obj1)
    cache.add(obj2)
    cache.add(obj3)

    assert sorted(cache.get_cached()) == [obj2, obj3]


def test_generational_cache_generational_overlap(obj1, obj2, obj3):
    """
    An object that is both in the primary and the secondary
    generation is listed only once in the cache's contents.
    """
    cache = GenerationalCache(2)
    cache.add(obj1)  # new=[1]    old=[]
    cache.add(obj2)  # new=[1, 2] old=[]
    cache.add(obj3)  # new=[3]    old=[1, 2]
    cache.add(obj1)  # new=[3, 1] old=[1, 2]

    assert sorted(cache.get_cached()) == [obj1, obj2, obj3]


def test_generational_cache_remove_from_overlap(obj1, obj2, obj3):
    """
    Removing an object from the cache removes it from both its
    primary and secondary generations.
    """
    cache = GenerationalCache(2)
    cache.add(obj1)  # new=[1]    old=[]
    cache.add(obj2)  # new=[1, 2] old=[]
    cache.add(obj3)  # new=[3]    old=[1, 2]
    cache.add(obj1)  # new=[3, 1] old=[1, 2]

    present = cache.remove(obj1)
    assert present
    assert sorted(cache.get_cached()) == [obj2, obj3]


def test_generational_cache_evict_oldest(obj1, obj2, obj3):
    """The "oldest" object is the first to be evicted."""
    cache = GenerationalCache(1)
    cache.add(obj1)
    cache.add(obj2)
    cache.add(obj3)

    assert sorted(cache.get_cached()) == [obj2, obj3]


def test_generational_cache_evict_lru(obj1, obj2, obj3):
    """
    Actually, it's not the oldest but the LRU object that is first
    to be evicted.  Re-adding the oldest object makes it not be
    the LRU.
    """
    cache = GenerationalCache(1)
    cache.add(obj1)
    cache.add(obj2)

    # This "refreshes" the oldest object in the cache.
    cache.add(obj1)

    cache.add(obj3)

    assert sorted(cache.get_cached()) == [obj1, obj3]
