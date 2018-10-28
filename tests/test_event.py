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
import pytest

from mock import call, Mock
from storm.event import EventSystem


class Marker(object):
    pass


@pytest.fixture
def marker():
    return Marker()


@pytest.fixture
def event(marker):
    return EventSystem(marker)


def test_hook_unhook_emit(event, marker):
    callback1 = Mock()
    callback2 = Mock()

    event.hook("one", callback1)
    event.hook("one", callback1)
    event.hook("one", callback2, 10, 20)
    event.hook("two", callback2, 10, 20)
    event.hook("two", callback2, 10, 20)
    event.hook("two", callback2, 30, 40)
    event.hook("three", callback1)

    event.emit("one", 1, 2)
    event.emit("two", 3, 4)
    event.unhook("two", callback2, 10, 20)
    event.emit("two", 3, 4)
    event.emit("three", 5, 6)

    calls1 = [
        call(marker, 1, 2),
        call(marker, 5, 6),
    ]
    callback1.asser_has_calls(calls1)
    assert callback1.call_count == len(calls1)

    calls2 = [
        call(marker, 1, 2, 10, 20),
        call(marker, 3, 4, 10, 20),
        call(marker, 3, 4, 30, 40),
        call(marker, 3, 4, 30, 40),
    ]
    callback2.asser_has_calls(calls2)
    assert callback2.call_count == len(calls2)


def test_unhook_by_returning_false(event, marker):
    called = []
    def callback(owner):
        called.append(owner)
        return len(called) < 2

    event.hook("event", callback)

    event.emit("event")
    event.emit("event")
    event.emit("event")
    event.emit("event")

    assert called == [marker, marker]


def test_weak_reference():
    marker = Marker()

    called = []
    def callback(owner):
        called.append(owner)

    event = EventSystem(marker)

    event.hook("event", callback)
    event.emit("event")

    assert called == [marker]
    del called[:]

    del marker
    event.emit("event")
    assert called == []
