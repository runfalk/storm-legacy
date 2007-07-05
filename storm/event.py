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
import weakref


__all__ = ["EventSystem"]


class EventSystem(object):

    def __init__(self, owner):
        self._owner_ref = weakref.ref(owner)
        self._hooks = {}
        self._saved_hooks = {}

    def save(self):
        hooks = {}
        for name, callbacks in self._hooks.items():
            hooks[name] = callbacks.copy()
        self._saved_hooks = hooks

    def restore(self):
        hooks = self._hooks
        hooks.clear()
        for name, callbacks in self._saved_hooks.items():
            hooks[name] = callbacks.copy()

    def hook(self, name, callback, *data):
        callbacks = self._hooks.get(name)
        if callbacks is None:
            self._hooks.setdefault(name, set()).add((callback, data))
        else:
            callbacks.add((callback, data))

    def unhook(self, name, callback, *data):
        callbacks = self._hooks.get(name)
        if callbacks is not None:
            callbacks.discard((callback, data))

    def emit(self, name, *args):
        owner = self._owner_ref()
        if owner is not None:
            callbacks = self._hooks.get(name)
            if callbacks is not None:
                for callback, data in callbacks.copy():
                    if callback(owner, *(args+data)) is False:
                        callbacks.discard((callback, data))
