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

import sys


__all__ = [
    "add_metaclass",
    "buffer",
    "bstr",
    "is_python2",
    "iter_range",
    "iter_items",
    "iter_keys",
    "iter_values",
    "iter_zip",
    "long_int",
    "pickle",
    "string_types",
    "StringIO",
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

    def iter_items(obj):
        return obj.items()

    def iter_keys(obj):
        return obj.keys()

    def iter_values(obj):
        return obj.values()
else:
    is_python2 = True

    bstr = str
    ustr = unicode
    iter_range = xrange
    long_int = long

    def iter_items(obj):
        return obj.iteritems()

    def iter_keys(obj):
        return obj.iterkeys()

    def iter_values(obj):
        return obj.itervalues()


string_types = (bstr, ustr)


try:
    from itertools import izip as iter_zip
except ImportError:
    iter_zip = zip


try:
    # We need to import cPickle first as pickle exists in Python 2 as well, but
    # is not implemented in C
    import cPickle as pickle
except ImportError:
    import pickle


try:
    # The io module is available in Python 2 but it works differently than
    # cStringIO
    from cStringIO import StringIO
except ImportError:
    from io import StringIO


try:
    import socketserver
except ImportError:
    import SocketServer as socketserver


try:
    from __builtin__ import buffer
except ImportError:
    buffer = memoryview

def add_metaclass(metaclass):
    """
    Class decorator for creating a class with a metaclass. Shamelessly copied
    from Six (https://bitbucket.org/gutworth/six).

    .. code-block:: python

        @add_metaclass(MetaClass)
        def NormalClass(object):
            pass
    """

    def wrapper(cls):
        orig_vars = cls.__dict__.copy()
        slots = orig_vars.get("__slots__")

        if slots is not None:
            if isinstance(slots, string_types):
                slots = [slots]

            for slots_var in slots:
                orig_vars.pop(slots_var)

        orig_vars.pop("__dict__", None)
        orig_vars.pop("__weakref__", None)

        return metaclass(cls.__name__, cls.__bases__, orig_vars)
    return wrapper
