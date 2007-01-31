#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
import weakref


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
