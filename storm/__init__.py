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


class UndefType(object):

    def __repr__(self):
        return "Undef"

    def __reduce__(self):
        return "Undef"


Undef = UndefType()


# This is here for libraries that have specific Storm version requirements, or
# enforces a minimum version. Since a lot of features have been removed it is
# slightly misleading to call this 0.21.0 since there is no such release, but
# it's probably the best compromise to make.
#
# This will never reflect the storm-legacy version.
version = "0.21.0"
version_info = tuple([int(x) for x in version.split(".")])


# C extensions are enabled by default.  They are not used if the
# STORM_CEXTENSIONS environment variable is set to '0'.  If they can't be
# imported Storm will automatically use Python versions of the optimized code
# in the C extension.
has_cextensions = False
if os.environ.get("STORM_CEXTENSIONS") != "0":
    try:
        from storm import cextensions
        has_cextensions = True
    except ImportError as e:
        if "cextensions" not in str(e):
           raise
