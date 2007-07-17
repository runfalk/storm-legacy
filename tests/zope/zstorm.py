import thread

from tests.helper import TestHelper

has_zope = True
try:
    from zope import component

    import transaction

    from storm.exceptions import OperationalError
    from storm.locals import Store, Int
    from storm.zope.interfaces import IZStorm, ZStormError
    from storm.zope.zstorm import ZStorm
except ImportError:
    has_zope = False


class ZStormTest(TestHelper):

    def is_supported(self):
        return has_zope

    def setUp(self):
        self.zstorm = ZStorm()

    def tearDown(self):
        # Free the transaction to avoid having errors that cross
        # test cases.
        transaction.manager.free(transaction.get())

    def test_utility(self):
        component.provideUtility(ZStorm())
        self.assertTrue(isinstance(component.getUtility(IZStorm), ZStorm))

    def test_create(self):
        store = self.zstorm.create(None, "sqlite:")
        self.assertTrue(isinstance(store, Store))

    def test_create_twice_unnamed(self):
        store = self.zstorm.create(None, "sqlite:")
        store.execute("CREATE TABLE test (id INTEGER)")
        store.commit()

        store = self.zstorm.create(None, "sqlite:")
        self.assertRaises(OperationalError,
                          store.execute, "SELECT * FROM test")

    def test_create_twice_same_name(self):
        store = self.zstorm.create("name", "sqlite:")
        self.assertRaises(ZStormError, self.zstorm.create, "name", "sqlite:")

    def test_create_and_get_named(self):
        store = self.zstorm.create("name", "sqlite:")
        self.assertTrue(self.zstorm.get("name") is store)

    def test_create_and_get_named_another_thread(self):
        store = self.zstorm.create("name", "sqlite:")

        raised = []

        lock = thread.allocate_lock()
        lock.acquire()
        def f():
            try:
                try:
                    self.zstorm.get("name")
                except ZStormError:
                    raised.append(True)
            finally:
                lock.release()
        thread.start_new_thread(f, ())
        lock.acquire()

        self.assertTrue(raised)

    def test_get_unexistent(self):
        self.assertRaises(ZStormError, self.zstorm.get, "name")

    def test_get_with_uri(self):
        store = self.zstorm.get("name", "sqlite:")
        self.assertTrue(isinstance(store, Store))
        self.assertTrue(self.zstorm.get("name") is store)
        self.assertTrue(self.zstorm.get("name", "sqlite:") is store)

    def test_set_default_uri(self):
        self.zstorm.set_default_uri("name", "sqlite:")
        store = self.zstorm.get("name")
        self.assertTrue(isinstance(store, Store))

    def test_create_default(self):
        self.zstorm.set_default_uri("name", "sqlite:")
        store = self.zstorm.create("name")
        self.assertTrue(isinstance(store, Store))

    def test_create_default_twice(self):
        self.zstorm.set_default_uri("name", "sqlite:")
        self.zstorm.create("name")
        self.assertRaises(ZStormError, self.zstorm.create, "name")

    def test_iterstores(self):
        store1 = self.zstorm.create(None, "sqlite:")
        store2 = self.zstorm.create(None, "sqlite:")
        store3 = self.zstorm.create("name", "sqlite:")
        stores = []
        for name, store in self.zstorm.iterstores():
            stores.append((name, store))
        self.assertEquals(len(stores), 3)
        self.assertEquals(set(stores),
                          set([(None, store1), (None, store2),
                               ("name", store3)]))

    def test_default_databases(self):
        self.zstorm.set_default_uri("name1", "sqlite:1")
        self.zstorm.set_default_uri("name2", "sqlite:2")
        self.zstorm.set_default_uri("name3", "sqlite:3")
        default_uris = self.zstorm.get_default_uris()
        self.assertEquals(default_uris, {"name1": "sqlite:1",
                                         "name2": "sqlite:2",
                                         "name3": "sqlite:3"})

    def test_remove(self):
        removed_store = self.zstorm.get("name", "sqlite:")
        self.zstorm.remove(removed_store)
        for name, store in self.zstorm.iterstores():
            self.assertNotEquals(store, removed_store)
        self.assertRaises(ZStormError, self.zstorm.get, "name")

        # Abort the transaction so that the currently registered
        # resource detaches.
        transaction.abort()

        # Let's try to make storm blow up when committing, so that
        # we can be sure that the removed store isn't linked to the
        # transaction system by a synchronizer anymore.
        class BadTable(object):
            __storm_table__ = "bad_table"
            id = Int(primary=True)
        removed_store.add(BadTable())

        # If this fails, the store is still linked to the transaction
        # system.
        transaction.commit()

    def test_double_abort(self):
        """
        Surprisingly, transaction.abort() won't detach the internal
        transaction from the manager.  This means that when
        transaction.begin() runs, it will detect that there's still
        a Transaction instance around, and will call abort on it once
        more before instantiating a new Transaction.  The Transaction's
        abort() method will then call beforeCompletion() on our
        synchronizer, which then tries to join() on the current
        transaction, which is still the old one and blows up saying
        that a previous transaction has failed (we know it, since we
        *aborted* it).
        """
        class BadTable(object):
            __storm_table__ = "bad_table"
            id = Int(primary=True, default=1)
        store = self.zstorm.get("name", "sqlite:")
        store.add(BadTable())
        self.assertRaises(OperationalError, transaction.commit)
        transaction.abort()
        transaction.abort()
