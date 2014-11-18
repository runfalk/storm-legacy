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
"""Manage L{Schema}s across a set of L{Store} shards."""

from storm.schema.schema import SchemaMissingError, UnappliedPatchesError


HORIZONTAL_PATCHING = 1
VERTICAL_PATCHING = 2


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
        for store, schema in self._stores:
            schema.create(store)

    def drop(self):
        for store, schema in self._stores:
            schema.drop(store)

    def delete(self):
        for store, schema in self._stores:
            schema.delete(store)

    def upgrade(self, mode=HORIZONTAL_PATCHING):
        """Perform a schema upgrade.

        Pristine L{Store}s without any schema applied yet, will be initialized
        using L{Schema.create}.

        All other L{Store}s will be upgraded to the latest version of their
        L{Schema}s by applying all pending patches.

        There are two patching modes that can be used: horizontal or vertical.

        With horizontal upgrades the patch numbering must be the same across
        all L{Schema}s, and all L{Store}s must be at the same patch number. For
        example if the common patch number is N, the upgrade will start by
        applying patch number N + 1 to all non-pristine stores, following the
        order in which the stores were added to the L{Sharding}. Then the
        upgrade will apply all patches with number N + 2, etc.

        With vertical upgrades all pending patches for the first store get
        applied sequentially one after the other, then the same happens for
        all pending patches in the second stored, etc.
        """
        if mode == HORIZONTAL_PATCHING:
            self._horizontal_upgrade()
        elif mode == VERTICAL_PATCHING:
            self._vertical_upgrade()
        else:
            raise RuntimeError("Unknown patch strategy")

    def _horizontal_upgrade(self):
        """Apply patches horizzontally."""
        stores_to_upgrade = []
        unapplied_versions = []
        for store, schema in self._stores:
            try:
                schema.check(store)
            except SchemaMissingError:
                schema.create(store)
            except UnappliedPatchesError, error:
                if not unapplied_versions:
                    unapplied_versions = error.unapplied_versions
                elif unapplied_versions != error.unapplied_versions:
                    raise PatchLevelMismatchError(
                        "Some stores have different patch levels")
                stores_to_upgrade.append((store, schema))

        for version in unapplied_versions:
            for store, schema in stores_to_upgrade:
                schema.advance(store, version)

    def _vertical_upgrade(self):
        """Apply patches vertically."""
        for store, schema in self._stores:
            schema.upgrade(store)
