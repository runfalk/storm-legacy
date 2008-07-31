#
# Copyright (c) 2008 Canonical
#
# Written by James Henstridge <jamesh@canonical.com>
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

"""Django middleware support for the Zope transaction manager.

Adding storm.django.middleware.ZopeTransactionMiddleware to
L{MIDDLEWARE_CLASSES} in the application's settings module will cause a
Zope transaction to be run for each request.
"""

__all__ = ['ZopeTransactionMiddleware']


from django.conf import settings

import transaction


class ZopeTransactionMiddleware(object):
    """Zope Transaction middleware for Django.

    If this is enabled, a Zope transaction will be run to cover each
    request.
    """
    def process_request(self, request):
        """Begin a transaction on request start.."""
        transaction.begin()

    def process_exception(self, request, exception):
        """Abort the transaction on errors."""
        transaction.abort()

    def process_response(self, request, response):
        """Commit or abort the transaction after processing the response.

        On successful completion of the request, the transaction will
        be committed.

        As an exception to this, if the L{STORM_COMMIT_SAFE_METHODS}
        setting is False, and the request used either of the GET and
        HEAD methods, the transaction will be aborted.
        """
        commit_safe_methods = getattr(settings, 'STORM_COMMIT_SAFE_METHODS',
                                      False)
        if commit_safe_methods or request.method not in ['HEAD', 'GET']:
            transaction.commit()
        else:
            transaction.abort()
        return response
