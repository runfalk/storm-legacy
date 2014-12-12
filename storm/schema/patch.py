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
"""Apply database patches.

The L{PatchApplier} class can be used to apply and keep track of a series
of database patches.

To create a patch series all is needed is to add Python files under a module
of choice, an name them as 'patch_N.py' where 'N' is the version of the patch
in the series. Each patch file must define an C{apply} callable taking a
L{Store} instance has its only argument. This function will be called when the
patch gets applied.

The L{PatchApplier} can be then used to apply to a L{Store} all the available
patches. After a patch has been applied, its version is recorded in a special
'patch' table in the given L{Store}, and it won't be applied again.
"""

import sys
import os
import re
import types

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
        return "store has patches the code doesn't know about: %s" % (
            ", ".join([str(version) for version in self._patches]))


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

    @param store: The L{Store} to apply the patches to.
    @param patch_set: The L{PatchSet} containing the patches to apply.
    @param committer: Optionally an object implementing 'commit()' and
        'rollback()' methods, to be used to commit or rollback the changes
        after applying a patch. If C{None} is given, the C{store} itself is
        used.
    """

    def __init__(self, store, patch_set, committer=None):
        self._store = store
        if isinstance(patch_set, types.ModuleType):
            # Up to version 0.20.0 the second positional parameter used to
            # be a raw module containing the patches. We wrap it with PatchSet
            # so to keep backward-compatibility.
            patch_set = PatchSet(patch_set)
        self._patch_set = patch_set
        if committer is None:
            committer = store
        self._committer = committer

    def apply(self, version):
        """Execute the patch with the given version.

        This will call the 'apply' function defined in the patch file with
        the given version, passing it our L{Store}.

        @param version: The version of the patch to execute.
        """
        patch = Patch(version)
        self._store.add(patch)
        module = None
        try:
            module = self._patch_set.get_patch_module(version)
            module.apply(self._store)
        except StormError:
            self._committer.rollback()
            raise
        except:
            type, value, traceback = sys.exc_info()
            patch_repr = getattr(module, "__file__", version)
            raise BadPatchError, \
                  "Patch %s failed: %s: %s" % \
                      (patch_repr, type.__name__, str(value)), \
                      traceback
        self._committer.commit()

    def apply_all(self):
        """Execute all unapplied patches.

        @raises UnknownPatchError: If the patch table has versions for which
            no patch file actually exists.
        """
        self.check_unknown()
        for version in self.get_unapplied_versions():
            self.apply(version)

    def mark_applied(self, version):
        """Mark the patch with the given version as applied."""
        self._store.add(Patch(version))
        self._committer.commit()

    def mark_applied_all(self):
        """Mark all unapplied patches as applied."""
        for version in self.get_unapplied_versions():
            self.mark_applied(version)

    def has_pending_patches(self):
        """Return C{True} if there are unapplied patches, C{False} if not."""
        for version in self.get_unapplied_versions():
            return True
        return False

    def get_unknown_patch_versions(self):
        """
        Return the list of Patch versions that have been applied to the
        database, but don't appear in the schema's patches module.
        """
        applied = self._get_applied_patches()
        known_patches = self._patch_set.get_patch_versions()
        unknown_patches = set()

        for patch in applied:
            if not patch in known_patches:
                unknown_patches.add(patch)
        return unknown_patches

    def check_unknown(self):
        """Look for patches that we don't know about.

        @raises UnknownPatchError: If the store has applied patch versions
            this schema doesn't know about.
        """
        unknown_patches = self.get_unknown_patch_versions()
        if unknown_patches:
            raise UnknownPatchError(self._store, unknown_patches)

    def get_unapplied_versions(self):
        """Return the versions of all unapplied patches."""
        applied = self._get_applied_patches()
        for version in self._patch_set.get_patch_versions():
            if version not in applied:
                yield version

    def _get_applied_patches(self):
        """Return the versions of all applied patches."""
        applied = set()
        for patch in self._store.find(Patch):
            applied.add(patch.version)
        return applied


class PatchSet(object):
    """A collection of patch modules.

    Each patch module lives in a regular Python module file, contained in a
    sub-directory named against the patch version. For example, given
    a directory tree like:

      mypackage/
          __init__.py
          patch_1/
              __init__.py
              foo.py

    the following code will return a patch module object for foo.py:

      >>> import mypackage
      >>> patch_set = PackagePackage(mypackage, sub_level="foo")
      >>> patch_module = patch_set.get_patch_module(1)
      >>> print patch_module.__name__
      'mypackage.patch_1.foo'

    Different sub-levels can be used to apply different patches to different
    stores (see L{Sharding}).

    Alternatively if no sub-level is provided, the structure will be flat:

      mypackage/
          __init__.py
          patch_1.py

      >>> import mypackage
      >>> patch_set = PackagePackage(mypackage)
      >>> patch_module = patch_set.get_patch_module(1)
      >>> print patch_module.__name__
      'mypackage.patch_1'

    This simpler structure can be used if you have just one store to patch
    or you don't care to co-ordinate the patches across your stores.
    """

    def __init__(self, package, sub_level=None):
        self._package = package
        self._sub_level = sub_level

    def get_patch_versions(self):
        """Return the versions of all available patches."""
        pattern = r"^patch_(\d+)"
        if not self._sub_level:
            pattern += ".py"
        pattern += "$"
        format = re.compile(pattern)

        patch_directory = self._get_patch_directory()
        filenames = os.listdir(patch_directory)
        matches = [(format.match(fn), fn) for fn in filenames]
        matches = sorted(filter(lambda x: x[0], matches),
                         key=lambda x: int(x[0].group(1)))
        return [int(match.group(1)) for match, filename in matches]

    def get_patch_module(self, version):
        """Import the Python module of the patch file with the given version.

        @param: The version of the module patch to import.
        @return: The imported module.
        """
        name = "patch_%d" % version
        levels = [self._package.__name__, name]
        if self._sub_level:
            directory = self._get_patch_directory()
            path = os.path.join(directory, name, self._sub_level + ".py")
            if not os.path.exists(path):
                return _EmptyPatchModule()
            levels.append(self._sub_level)
        return __import__(".".join(levels), None, None, [''])

    def _get_patch_directory(self):
        """Get the path to the directory of the patch package."""
        return os.path.dirname(self._package.__file__)


class _EmptyPatchModule(object):
    """Fake module object with a no-op C{apply} function."""

    def apply(self, store):
        pass
