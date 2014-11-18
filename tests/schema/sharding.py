#
# Copyright (c) 2006, 2014 Canonical
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
from tests.mocker import MockerTestCase

from storm.schema.schema import SchemaMissingError, UnappliedPatchesError
from storm.schema.sharding import (
    Sharding, PatchLevelMismatchError, VERTICAL_PATCHING)


class FakeSchema(object):

    patches = 2

    def __init__(self):
        self.applied = []

    def check(self, store):
        if store.pristine:
            raise SchemaMissingError()
        if store.patch < self.patches:
            unapplied_versions = range(store.patch + 1, self.patches + 1)
            raise UnappliedPatchesError(unapplied_versions)

    def create(self, store):
        store.pristine = False

    def upgrade(self, store):
        for i in range(2):
            store.patch += 1
            self.applied.append((store, store.patch))

    def advance(self, store, version):
        store.patch = version
        self.applied.append((store, store.patch))


class FakeStore(object):

    pristine = True  # If no schema was ever applied
    patch = 0  # Current patch level of the store


class ShardingTest(MockerTestCase):

    def setUp(self):
        super(ShardingTest, self).setUp()
        self.store = FakeStore()
        self.schema = FakeSchema()
        self.sharding = Sharding()

    def test_upgrade_pristine_store(self):
        """
        Pristine L{Store}s get their L{Schema} created from scratch.
        """
        self.sharding.add(self.store, self.schema)
        self.sharding.upgrade()
        self.assertFalse(self.store.pristine)

    def test_upgrade_apply_patches(self):
        """
        If a L{Store}s is not at the latest patch level, all pending
        patches get applied.
        """
        self.store.pristine = False
        self.sharding.add(self.store, self.schema)
        self.sharding.upgrade()
        self.assertEqual(2, self.store.patch)

    def test_upgrade_multi_store(self):
        """
        If a L{Store}s is not at the latest patch level, all pending
        patches get applied, one level at a time.
        """
        self.store.pristine = False
        self.sharding.add(self.store, self.schema)

        store2 = FakeStore()
        store2.pristine = False
        self.sharding.add(store2, self.schema)

        self.sharding.upgrade()
        self.assertEqual(2, self.store.patch)
        self.assertEqual(2, store2.patch)
        self.assertEqual(
            [(self.store, 1), (store2, 1), (self.store, 2), (store2, 2)],
            self.schema.applied)

    def test_upgrade_multi_store_vertically(self):
        """
        With vertical sharding, all pending patches for a L{Store} are
        applied sequentially.
        """
        self.store.pristine = False
        self.sharding.add(self.store, self.schema)

        store2 = FakeStore()
        store2.pristine = False
        self.sharding.add(store2, self.schema)

        self.sharding.upgrade(mode=VERTICAL_PATCHING)
        self.assertEqual(2, self.store.patch)
        self.assertEqual(2, store2.patch)
        self.assertEqual(
            [(self.store, 1), (self.store, 2), (store2, 1), (store2, 2)],
            self.schema.applied)

    def test_upgrade_patch_level_mismatch(self):
        """
        If not all L{Store}s are at the same patch level, an exception
        is raised.
        """
        self.store.pristine = False
        self.sharding.add(self.store, self.schema)

        store2 = FakeStore()
        store2.pristine = False
        store2.patch = 1
        self.sharding.add(store2, self.schema)

        self.assertRaises(PatchLevelMismatchError, self.sharding.upgrade)
