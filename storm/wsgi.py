#
# Copyright (c) 2006, 2007 Canonical
#
# Written by Robert Collins <robert@canonical.com>
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

"""Glue to wire a storm timeline tracer up to a WSGI app."""

import functools
import threading

__all__ = ['make_app']

def make_app(app):
    """Capture the per-request timeline object needed for storm tracing.

    To use firstly make your app and then wrap it with this make_app::

       >>> app, find_timeline = make_app(app)

    Then wrap the returned app with the timeline app (or anything that sets
    environ['timeline.timeline'])::

       >>> app = timeline.wsgi.make_app(app)

    Finally install a timeline tracer to capture storm queries::

       >>> install_tracer(TimelineTracer(find_timeline))

    @return: A wrapped WSGI app and a timeline factory function for use with
    TimelineTracer.
    """
    timeline_map = threading.local()
    def wrapper(environ, start_response):
        timeline = environ.get('timeline.timeline')
        # We could clean up timeline_map.timeline after we're done with the
        # request, but for that we'd have to make assumptions on how to
        # consume the data from whatever is returned by the inner app, and
        # that has proven to be impractical (e.g. some wsgi apps will return
        # a twisted IBodyProducer which is not meant to be consumed here). The
        # downside of not cleaning up is that a thread will leak a timeline
        # until the next request comes through, which is probably no big
        # deal.
        timeline_map.timeline = timeline
        return app(environ, start_response)
    return wrapper, functools.partial(getattr, timeline_map, 'timeline', None)
