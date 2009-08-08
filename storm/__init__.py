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


version = "0.15"
version_info = tuple([int(x) for x in version.split(".")])


class UndefType(object):

    def __repr__(self):
        return "Undef"

    def __reduce__(self):
        return "Undef"


Undef = UndefType()


# XXX The default is 1 for now.  In the future we'll invert this logic so
#     that it's enabled by default.
has_cextensions = False
if os.environ.get("STORM_CEXTENSIONS") == "1":
    try:
        from storm import cextensions
        has_cextensions = True
    except ImportError, e:
        # XXX Once the logic is inverted and cextensions are enabled by
        #     default, use the following version so that people may opt
        #     to use and distribute the pure Python version.
        #if "cextensions" not in str(e):
        #    raise
        raise
