#
# Copyright (c) 2011 Canonical
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

import json
import sys


__all__ = [
    "bstr",
    "is_python2",
    "long_int",
    "json",
    "ustr",
    "version",
]


version = sys.version_info[:2]


if version >= (3, 0):
    is_python2 = False

    bstr = bytes
    ustr = str
    iter_range = range
    long_int = int
else:
    is_python2 = True

    bstr = str
    ustr = unicode
    iter_range = xrange
    long_int = long
