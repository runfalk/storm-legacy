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
import transaction

from storm.locals import StormError
from storm.patch import PatchApplier


__all__ = ["Schema"]


class Schema(object):
    """Create, drop, clean and patch table schemas.

    @param creates: A list of C{CREATE TABLE} statements.
    @param drops: A list of C{DROP TABLE} statements.
    @param deletes: A list of C{DELETE FROM} statements.
    @param patch_package: The Python package containing patch modules to apply,
        see also L{PatchApplier}.
    """

    def __init__(self, creates, drops, deletes, patch_package):
        """
        """
        self._creates = creates
        self._drops = drops
        self._deletes = deletes
        self._patch_package = patch_package

    def _execute_statements(self, store, statements):
        for statement in statements:
            try:
                store.execute(statement)
            except Exception:
                print "Error running %s" % statement
                raise
        store.commit()

    def create(self, store):
        """Run C{CREATE TABLE} SQL statements with C{store}."""
        self._execute_statements(store, self._creates)

    def drop(self, store):
        """Run C{DROP TABLE} SQL statements with C{store}."""
        self._execute_statements(store, self._drops)

    def delete(self, store):
        """Run C{DELETE FROM} SQL statements with C{store}."""
        self._execute_statements(store, self._deletes)

    def upgrade(self, store):
        """Upgrade C{store} to have the latest schema.

        If a schema isn't present a new one will be created.  Unapplied
        patches will be applied to an existing schema.
        """
        patch_applier = PatchApplier(store, self._patch_package)
        try:
            store.execute("SELECT * FROM patch WHERE 1=2")
        except StormError:
            # No schema at all. Create it from the ground.
            transaction.abort()
            self.create(store)
            patch_applier.mark_applied_all()
            transaction.commit()
        else:
            patch_applier.apply_all()
