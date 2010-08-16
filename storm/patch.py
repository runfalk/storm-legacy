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
import sys
import os
import re

import transaction

from storm.locals import StormError, Int


class UnknownPatchError(Exception):
    """
    Raised if a patch is found in the database that doesn't exist
    in the local patch directory.
    """

    def __init__(self, store, patches):
        self._store = store
        self._patches = patches

    def __str__(self):
        from zope.component import getUtility
        from storm.zope.interfaces import IZStorm
        zstorm = getUtility(IZStorm)
        name = zstorm.get_name(self._store)
        name = "foo"
        return "%s has patches the code doesn't know about: %s" % (
               name, ", ".join([str(version) for version in self._patches]))


class BadPatchError(Exception):
    """Raised when a patch failing with a random exception is found."""


class Patch(object):
    """Database object representing an applied patch.

    @version: The version of the patch associated with this object.
    """

    __storm_table__ = "patch"

    version = Int(primary=True, allow_none=False)

    def __init__(self, version):
        self.version = version


class PatchApplier(object):
    """Apply to a L{Store} the database patches from a given Python package.

    @param store: The L{Storm} to apply the patches to.
    @param package: The Python package containing the patches. Each patch is
        represented by a file inside the module, whose filename must match
        the format 'patch_N.py', where N is an integer number.
    """

    def __init__(self, store, package):
        self._store = store
        self._package = package

    def _module(self, version):
        module_name = "patch_%d" % (version,)
        return __import__(self._package.__name__ + "." + module_name,
                          None, None, [''])

    def apply(self, version):
        patch = Patch(version)
        self._store.add(patch)
        module = None
        try:
            module = self._module(version)
            module.apply(self._store)
        except StormError:
            transaction.abort()
            raise
        except:
            type, value, traceback = sys.exc_info()
            patch_repr = getattr(module, "__file__", version)
            raise BadPatchError, \
                  "Patch %s failed: %s: %s" % \
                      (patch_repr, type.__name__, str(value)), \
                      traceback
        transaction.commit()

    def apply_all(self):
        unknown_patches = self.get_unknown_patch_versions()
        if unknown_patches:
            raise UnknownPatchError(self._store, unknown_patches)
        for version in self._get_unapplied_versions():
            self.apply(version)

    def mark_applied(self, version):
        self._store.add(Patch(version))
        transaction.commit()

    def mark_applied_all(self):
        for version in self._get_unapplied_versions():
            self.mark_applied(version)

    def has_pending_patches(self):
        for version in self._get_unapplied_versions():
            return True
        return False

    def get_unknown_patch_versions(self):
        """
        Return the list of Patch versions that have been applied to the
        database, but don't appear in the schema's patches module.
        """
        applied = self._get_applied_patches()
        known_patches = self._get_patch_versions()
        unknown_patches = set()

        for patch in applied:
            if not patch in known_patches:
                unknown_patches.add(patch)
        return unknown_patches

    def _get_unapplied_versions(self):
        applied = self._get_applied_patches()
        for version in self._get_patch_versions():
            if version not in applied:
                yield version

    def _get_applied_patches(self):
        applied = set()
        for patch in self._store.find(Patch):
            applied.add(patch.version)
        return applied

    def _get_patch_versions(self):
        format = re.compile(r"^patch_(\d+).py$")

        filenames = os.listdir(os.path.dirname(self._package.__file__))
        matches = [(format.match(fn), fn) for fn in filenames]
        matches = sorted(filter(lambda x: x[0], matches),
                         key=lambda x: int(x[1][6:-3]))
        return [int(match.group(1)) for match, filename in matches]
