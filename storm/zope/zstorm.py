"""ZStorm integrates Storm with Zope 3.

@var global_zstorm: A global L{ZStorm} instance.  It used the
    L{IZStorm} utility registered in C{configure.zcml}.
"""

#
# Copyright (c) 2006, 2007 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# Storm is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 2.1 of
# the License, or (at your option) any later version.
#
# Storm is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
import threading
import weakref

from zope.testing.cleanup import addCleanUp
from zope.interface import implements

import transaction
from transaction.interfaces import IDataManager, ISynchronizer
from transaction._transaction import TransactionFailedError

from storm.zope.interfaces import IZStorm, ZStormError
from storm.database import create_database
from storm.store import Store


class ZStorm(object):
    """A utility which integrates Storm with Zope.

    Typically, applications will register stores using ZCML similar
    to::

      <store name='main' uri='sqlite:' />

    Application code can then acquire the store by name using code
    similar to::

      from zope.component import getUtility
      from storm.zope.interfaces import IZStorm

      store = getUtility(IZStorm).get('main')
    """

    implements(IZStorm)

    _databases = {}

    def __init__(self):
        self._local = threading.local()
        self._default_databases = {}
        self._default_uris = {}

    def _reset(self):
        for name, store in self.iterstores():
            store.close()
        self._local = threading.local()
        self._databases.clear()
        self._default_databases.clear()
        self._default_uris.clear()

    @property
    def _stores(self):
        try:
            return self._local.stores
        except AttributeError:
            stores = weakref.WeakValueDictionary()
            return self._local.__dict__.setdefault("stores", stores)

    @property
    def _named(self):
        try:
            return self._local.named
        except AttributeError:
            return self._local.__dict__.setdefault(
                "named", weakref.WeakValueDictionary())

    @property
    def _name_index(self):
        try:
            return self._local.name_index
        except AttributeError:
            return self._local.__dict__.setdefault("name_index", {})

    def _get_database(self, uri):
        database = self._databases.get(uri)
        if database is None:
            return self._databases.setdefault(uri, create_database(uri))
        return database

    def set_default_uri(self, name, default_uri):
        """Set C{default_uri} as the default URI for stores called C{name}."""
        self._default_databases[name] = self._get_database(default_uri)
        self._default_uris[name] = default_uri

    def create(self, name, uri=None):
        """Create a new store called C{name}.

        @param uri: Optionally, the URI to use.
        @raises ZStormError: Raised if C{uri} is None and no default
            URI exists for C{name}.  Also raised if a store with
            C{name} already exists.
        """
        if uri is None:
            database = self._default_databases.get(name)
            if database is None:
                raise ZStormError("Store named '%s' not found" % name)
        else:
            database = self._get_database(uri)
        store = Store(database)
        store.__synchronizer = StoreSynchronizer(store)

        self._stores[id(store)] = store

        if name is not None:
            old_store = self._named.setdefault(name, store)
            if old_store is not store:
                raise ZStormError("Store named '%s' already exists" % name)
        self._name_index[store] = name
        return store

    def get(self, name, default_uri=None):
        """Get the store called C{name} or None if one isn't available.

        @param default_uri: Optionally, the URI to use to create a
           store called C{name} when one doesn't already exist.
        """
        store = self._named.get(name)
        if not store:
            return self.create(name, default_uri)
        return store

    def remove(self, store):
        """Remove the given store from ZStorm.

        This removes any management of the store from ZStorm.

        Notice that if the store was used inside the current
        transaction, it's probably joined the transaction system as
        a resource already, and thus it will commit/rollback when
        the transaction system requests so.

        This method will unlink the *synchronizer* from the transaction
        system, so that once the current transaction is over it won't
        link back to it in future transactions.
        """
        del self._stores[id(store)]
        name = self._name_index[store]
        del self._name_index[store]
        del self._named[name]
        transaction.manager.unregisterSynch(store.__synchronizer)

    def iterstores(self):
        """Iterate C{name, store} 2-tuples."""
        for store, name in self._name_index.iteritems():
            yield name, store

    def get_name(self, store):
        """Returns the name for C{store} or None if one isn't available."""
        return self._name_index.get(store)

    def get_default_uris(self):
        """
        Return a list of name, uri tuples that are named as the default
        databases for those names.
        """
        return self._default_uris.copy()


class StoreSynchronizer(object):
    """This class takes cares of plugging the store in new transactions.

    Garbage collection should work fine, because the transaction manager
    stores synchronizers as weak references. Even then, we give a hand
    to the garbage collector by avoiding strong circular references
    on the store.
    """

    implements(ISynchronizer)

    def __init__(self, store):
        data_manager = StoreDataManager(store)

        self._store_ref = weakref.ref(store)
        self._data_manager_ref = weakref.ref(data_manager)

        # Join now ...
        data_manager.join(transaction.get())

        # ... and in the future.
        transaction.manager.registerSynch(self)

    def _join(self, trans):
        # If the store is still alive and the transaction is in this thread.
        store = self._store_ref()
        if store and trans is transaction.get():
            data_manager = self._data_manager_ref()
            if data_manager is None:
                data_manager = StoreDataManager(store)
                self._data_manager_ref = weakref.ref(data_manager)
            try:
                data_manager.join(trans)
            except TransactionFailedError:
                # It means that an *already failed* transaction is trying
                # to join us to notify that it is indeed failed.  We don't
                # care about these (see the double_abort test case).
                pass

    def beforeCompletion(self, trans):
        self._join(trans)

    def afterCompletion(self, trans):
        pass

    def newTransaction(self, trans):
        self._join(trans)


class StoreDataManager(object):
    """An L{IDataManager} implementation for C{ZStorm}."""

    implements(IDataManager)

    transaction_manager = transaction.manager

    def __init__(self, store):
        self._store = store
        self._trans = None

    def join(self, trans):
        if trans is not self._trans:
            self._trans = trans
            trans.join(self)

    def abort(self, txn):
        self._trans = None
        self._store.rollback()

    def tpc_begin(self, txn):
        # Zope's transaction system will call tpc_begin() on all
        # managers before calling commit, so flushing here may help
        # in cases where there are two stores with changes, and one
        # of them will fail.  In such cases, flushing earlier will
        # ensure that both transactions will be rolled back, instead
        # of one committed and one rolled back.
        self._store.flush()

    def commit(self, txn):
        self._trans = None
        self._store.commit()

    def tpc_vote(self, txn):
        pass

    def tpc_finish(self, txn):
        pass

    def tpc_abort(self, txn):
        pass

    def sortKey(self):
        return "store_%d" % id(self)


global_zstorm = ZStorm()

addCleanUp(global_zstorm._reset)
