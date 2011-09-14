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
import os
import sys

from tests.helper import TestHelper
from tests.zope import has_transaction, has_zope_component, has_testresources

from storm.locals import create_database, Store, Unicode, Int
from storm.exceptions import IntegrityError

if has_transaction and has_zope_component and has_testresources:
    from zope.component import provideUtility, getUtility
    from storm.zope.zstorm import ZStorm
    from storm.zope.interfaces import IZStorm
    from storm.zope.schema import ZSchema
    from storm.zope.testing import ZStormResourceManager


PATCH = """
def apply(store):
    store.execute('ALTER TABLE test ADD COLUMN bar INT')
"""


class ZStormResourceManagerTest(TestHelper):

    def is_supported(self):
        return has_transaction and has_zope_component and has_testresources

    def setUp(self):
        super(ZStormResourceManagerTest, self).setUp()
        self._package_dir = self.makeDir()
        sys.path.append(self._package_dir)
        patch_dir = os.path.join(self._package_dir, "patch_package")
        os.mkdir(patch_dir)
        self.makeFile(path=os.path.join(patch_dir, "__init__.py"), content="")
        self.makeFile(path=os.path.join(patch_dir, "patch_1.py"),
                      content=PATCH)
        import patch_package
        create = ["CREATE TABLE test (foo TEXT UNIQUE, bar INT)"]
        drop = ["DROP TABLE test"]
        delete = ["DELETE FROM test"]
        uri = "sqlite:///%s" % self.makeFile()
        schema = ZSchema(create, drop, delete, patch_package)
        self.databases = [{"name": "test", "uri": uri, "schema": schema}]
        self.resource = ZStormResourceManager(self.databases)
        self.store = Store(create_database(uri))

    def tearDown(self):
        del sys.modules["patch_package"]
        sys.path.remove(self._package_dir)
        if "patch_1" in sys.modules:
            del sys.modules["patch_1"]
        super(ZStormResourceManagerTest, self).tearDown()

    def test_make(self):
        """
        L{ZStormResourceManager.make} returns a L{ZStorm} resource that can be
        used to get the registered L{Store}s.
        """
        zstorm = self.resource.make([])
        store = zstorm.get("test")
        self.assertEqual([], list(store.execute("SELECT foo, bar FROM test")))

    def test_make_upgrade(self):
        """
        L{ZStormResourceManager.make} upgrades the schema if needed.
        """
        self.store.execute("CREATE TABLE patch "
                           "(version INTEGER NOT NULL PRIMARY KEY)")
        self.store.execute("CREATE TABLE test (foo TEXT)")
        self.store.commit()
        zstorm = self.resource.make([])
        store = zstorm.get("test")
        self.assertEqual([], list(store.execute("SELECT bar FROM test")))

    def test_make_delete(self):
        """
        L{ZStormResourceManager.make} deletes the data from all tables to make
        sure that tests run against a clean database.
        """
        self.store.execute("CREATE TABLE patch "
                           "(version INTEGER NOT NULL PRIMARY KEY)")
        self.store.execute("CREATE TABLE test (foo TEXT)")
        self.store.execute("INSERT INTO test (foo) VALUES ('data')")
        self.store.commit()
        zstorm = self.resource.make([])
        store = zstorm.get("test")
        self.assertEqual([], list(store.execute("SELECT foo FROM test")))

    def test_make_zstorm_overwritten(self):
        """
        L{ZStormResourceManager.make} registers its own ZStorm again if a test
        has registered a new ZStorm utility overwriting the resource one.
        """
        zstorm = self.resource.make([])
        provideUtility(ZStorm())
        self.resource.make([])
        self.assertIs(zstorm, getUtility(IZStorm))

    def test_clean_flush(self):
        """
        L{ZStormResourceManager.clean} tries to flush the stores to make sure
        that they are all in a consistent state.
        """
        class Test(object):
            __storm_table__ = "test"
            foo = Unicode()
            bar = Int(primary=True)

            def __init__(self, foo, bar):
                self.foo = foo
                self.bar = bar

        zstorm = self.resource.make([])
        store = zstorm.get("test")
        store.add(Test(u"data", 1))
        store.add(Test(u"data", 2))
        self.assertRaises(IntegrityError, self.resource.clean, zstorm)

    def test_clean_delete(self):
        """
        L{ZStormResourceManager.clean} cleans the database tables from the data
        created by the tests.
        """
        zstorm = self.resource.make([])
        store = zstorm.get("test")
        store.execute("INSERT INTO test (foo, bar) VALUES ('data', 123)")
        store.commit()
        self.resource.clean(zstorm)
        self.assertEqual([], list(self.store.execute("SELECT * FROM test")))

    def test_clean_with_force_delete(self):
        """
        If L{ZStormResourceManager.force_delete} is C{True}, L{Schema.delete}
        is always invoked upon test cleanup.
        """
        zstorm = self.resource.make([])
        self.store.execute("INSERT INTO test (foo, bar) VALUES ('data', 123)")
        self.store.commit()
        self.resource.force_delete = True
        self.resource.clean(zstorm)
        self.assertEqual([], list(self.store.execute("SELECT * FROM test")))

    def test_wb_clean_clears_alive_cache_before_abort(self):
        """
        L{ZStormResourceManager.clean} clears the alive cache before
        aborting the transaction.
        """
        class Test(object):
            __storm_table__ = "test"
            bar = Int(primary=True)

            def __init__(self, bar):
                self.bar = bar

        zstorm = self.resource.make([])
        store = zstorm.get("test")
        store.add(Test(1))
        store.add(Test(2))
        real_invalidate = store.invalidate

        def invalidate_proxy():
            self.assertEqual(0, len(store._alive.values()))
            real_invalidate()
        store.invalidate = invalidate_proxy

        self.resource.clean(zstorm)

    def test_schema_uri(self):
        """
        It's possible to specify an alternate URI for applying the schema
        and cleaning up tables after a test.
        """
        schema_uri = "sqlite:///%s" % self.makeFile()
        self.databases[0]["schema-uri"] = schema_uri
        zstorm = self.resource.make([])
        store = zstorm.get("test")
        schema_store = Store(create_database(schema_uri))

        # The schema was applied using the alternate schema URI
        statement = "SELECT name FROM sqlite_master WHERE name='patch'"
        self.assertEqual([], list(store.execute(statement)))
        self.assertEqual([("patch",)], list(schema_store.execute(statement)))

        # The cleanup is performed with the alternate schema URI
        store.commit()
        schema_store.execute("INSERT INTO test (foo) VALUES ('data')")
        schema_store.commit()
        self.resource.clean(zstorm)
        self.assertEqual([], list(schema_store.execute("SELECT * FROM test")))

    def test_deprecated_database_format(self):
        """
        The old deprecated format of the 'database' constructor parameter is
        still supported.
        """
        import patch_package
        uri = "sqlite:///%s" % self.makeFile()
        schema = ZSchema([], [], [], patch_package)
        resource = ZStormResourceManager({"test": (uri, schema)})
        zstorm = resource.make([])
        store = zstorm.get("test")
        self.assertIsNot(None, store)
