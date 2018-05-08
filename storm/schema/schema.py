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
"""Manage database shemas.

The L{Schema} class can be used to create, drop, clean and upgrade database
schemas.

A database L{Schema} is defined by the series of SQL statements that should be
used to create, drop and clear the schema, respectively and by a patch module
used to upgrade it (see also L{PatchApplier}).

For example:

>>> store =  Store(create_database('sqlite:'))
>>> creates = ['CREATE TABLE person (id INTEGER, name TEXT)']
>>> drops = ['DROP TABLE person']
>>> deletes = ['DELETE FROM person']
>>> import patch_module
>>> patch_set = PatchSet(patch_module)
>>> schema = Schema(creates, drops, deletes, patch_set)
>>> schema.create(store)


where patch_module is a Python module containing database patches used to
upgrade the schema over time.
"""
from __future__ import print_function

import types

from storm.locals import StormError
from storm.schema.patch import PatchApplier, PatchSet


class SchemaMissingError(Exception):
    """Raised when a L{Store} has no schema at all."""


class UnappliedPatchesError(Exception):
    """Raised when a L{Store} has unapplied schema patches.

    @ivar unapplied_versions: A list containing all unapplied patch versions.
    """

    def __init__(self, unapplied_versions):
        self.unapplied_versions = unapplied_versions


class Schema(object):
    """Create, drop, clean and patch table schemas.

    @param creates: A list of C{CREATE TABLE} statements.
    @param drops: A list of C{DROP TABLE} statements.
    @param deletes: A list of C{DELETE FROM} statements.
    @param patch_set: The L{PatchSet} containing patch modules to apply.
    @param committer: Optionally a committer to pass to the L{PatchApplier}.

    @see: L{PatchApplier}.
    """
    _create_patch = "CREATE TABLE patch (version INTEGER NOT NULL PRIMARY KEY)"
    _drop_patch = "DROP TABLE IF EXISTS patch"
    _autocommit = True

    def __init__(self, creates, drops, deletes, patch_set, committer=None):
        self._creates = creates
        self._drops = drops
        self._deletes = deletes
        if isinstance(patch_set, types.ModuleType):
            # Up to version 0.20.0 the fourth positional parameter used to
            # be a raw module containing the patches. We wrap it with PatchSet
            # for keeping backward-compatibility.
            patch_set = PatchSet(patch_set)
        self._patch_set = patch_set
        self._committer = committer

    def _execute_statements(self, store, statements):
        """Execute the given statements in the given store."""
        for statement in statements:
            try:
                store.execute(statement)
            except Exception:
                print("Error running %s" % statement)
                raise
        if self._autocommit:
            store.commit()

    def autocommit(self, flag):
        """Control whether to automatically commit/rollback schema changes.

        The default is C{True}, if set to C{False} it's up to the calling code
        to handle commits and rollbacks.

        @note: In case of rollback the exception will just be propagated, and
            no rollback on the store will be performed.
        """
        self._autocommit = flag

    def check(self, store):
        """Check that the given L{Store} is compliant with this L{Schema}.

        @param store: The L{Store} to check.

        @raises SchemaMissingError: If there is no schema at all.
        @raises UnappliedPatchesError: If there are unapplied schema patches.
        @raises UnknownPatchError: If the store has patches the schema doesn't.
        """
        # Let's create a savepoint here: the select statement below is just
        # used to test if the patch exists and we don't want to rollback
        # the whole transaction in case it fails.
        store.execute("SAVEPOINT schema")
        try:
            store.execute("SELECT * FROM patch WHERE version=0")
        except StormError:
            # No schema at all. Create it from the ground.
            store.execute("ROLLBACK TO SAVEPOINT schema")
            raise SchemaMissingError()
        else:
            store.execute("RELEASE SAVEPOINT schema")

        patch_applier = self._build_patch_applier(store)
        patch_applier.check_unknown()
        unapplied_versions = list(patch_applier.get_unapplied_versions())
        if unapplied_versions:
            raise UnappliedPatchesError(unapplied_versions)

    def create(self, store):
        """Run C{CREATE TABLE} SQL statements with C{store}.

        @raises SchemaAlreadyCreatedError: If the schema for this store was
            already created.
        """
        self._execute_statements(store, [self._create_patch])
        self._execute_statements(store, self._creates)
        patch_applier = self._build_patch_applier(store)
        patch_applier.mark_applied_all()

    def drop(self, store):
        """Run C{DROP TABLE} SQL statements with C{store}."""
        self._execute_statements(store, self._drops)
        self._execute_statements(store, [self._drop_patch])

    def delete(self, store):
        """Run C{DELETE FROM} SQL statements with C{store}."""
        self._execute_statements(store, self._deletes)

    def upgrade(self, store):
        """Upgrade C{store} to have the latest schema.

        If a schema isn't present a new one will be created.  Unapplied
        patches will be applied to an existing schema.
        """
        patch_applier = self._build_patch_applier(store)
        try:
            self.check(store)
        except SchemaMissingError:
            # No schema at all. Create it from the ground.
            self.create(store)
        except UnappliedPatchesError as error:
            patch_applier.check_unknown()
            for version in error.unapplied_versions:
                self.advance(store, version)

    def advance(self, store, version):
        """Advance the schema of C{store} by applying the next unapplied patch.

        @return: The version of patch that has been applied or C{None} if
            no patch was applied (i.e. the schema is fully upgraded).
        """
        patch_applier = self._build_patch_applier(store)
        patch_applier.apply(version)

    def _build_patch_applier(self, store):
        """Build a L{PatchApplier} to use for the given C{store}."""
        committer = self._committer
        if not self._autocommit:
            committer = _NoopCommitter()
        return PatchApplier(store, self._patch_set, committer)


class _NoopCommitter(object):
    """Dummy committer that does nothing."""

    def commit(self):
        pass

    def rollback(self):
        pass
