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
"""Manage L{Schema}s across a set of L{Store} shards.

The L{Sharding} class can be used to perform schema operations (create,
upgrade, delete) against a set of L{Store}s. For example, let's say
we have two L{Schema}s and two L{Store}s we want to apply them to. We can
setup our L{Sharding} instance like this:

>>> schema1 = Schema(...)
>>> schema1 = Schema(...)
>>> store1 = Store(...)
>>> store2 = Store(...)
>>> sharding = Sharding()
>>> sharding.add(store1, schema1)
>>> sharding.add(store2, schema2)

And then perform schema maintenance operations across all shards:

>>> sharding.upgrade()

Patches will be applied "horizontally", meaning that the stores will always
be at the same patch level. See L{storm.schema.patch.PatchSet}.
"""

from storm.schema.schema import SchemaMissingError, UnappliedPatchesError


class PatchLevelMismatchError(Exception):
    """Raised when stores don't have all the same patch level."""


class Sharding(object):
    """Manage L{Shema}s over a collection of L{Store}s."""

    def __init__(self):
        self._stores = []  # Sequence of Stores with their Schemas

    def add(self, store, schema):
        """Add a new L{Store} shard.

        @param store: The L{Store} to add.
        @param schema: The L{Schema} the L{Store} is meant to have.
        """
        self._stores.append((store, schema))

    def create(self):
        """Create all schemas from scratch across all L{Store} shards."""
        for store, schema in self._stores:
            schema.create(store)

    def drop(self):
        """Drop all tables across all L{Store} shards."""
        for store, schema in self._stores:
            schema.drop(store)

    def delete(self):
        """Delete all table rows across all L{Store} shards."""
        for store, schema in self._stores:
            schema.delete(store)

    def upgrade(self):
        """Perform a schema upgrade.

        Pristine L{Store}s without any schema applied yet, will be initialized
        using L{Schema.create}.

        All other L{Store}s will be upgraded to the latest version of their
        L{Schema}s by applying all pending patches.

        The patching strategy is "horizontal", meaning that patch numbering
        must be the same across all L{Schema}s, and all L{Store}s must be at
        the same patch number. For example if the common patch number is N,
        the upgrade will start by applying patch number N + 1 to all
        non-pristine stores, following the order in which the stores were
        added to the L{Sharding}. Then the upgrade will apply all patches
        with number N + 2, etc.
        """
        stores_to_upgrade = []
        unapplied_versions = []
        for store, schema in self._stores:
            try:
                schema.check(store)
            except SchemaMissingError:
                schema.create(store)
            except UnappliedPatchesError as error:
                if not unapplied_versions:
                    unapplied_versions = error.unapplied_versions
                elif unapplied_versions != error.unapplied_versions:
                    raise PatchLevelMismatchError(
                        "Some stores have different patch levels")
                stores_to_upgrade.append((store, schema))

        for version in unapplied_versions:
            for store, schema in stores_to_upgrade:
                schema.advance(store, version)
