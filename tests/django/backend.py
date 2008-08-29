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

try:
    from django.conf import settings
    from django.http import HttpRequest, HttpResponse
    import transaction
except ImportError:
    have_django_and_transaction = False
else:
    have_django_and_transaction = True
    from storm.django import stores
    from storm.zope.zstorm import global_zstorm

from tests.helper import TestHelper


class DjangoBackendTests(object):

    def is_supported(self):
        return have_django_and_transaction and self.get_store_uri() is not None

    def setUp(self):
        super(DjangoBackendTests, self).setUp()
        settings.configure(STORM_STORES={})
        settings.MIDDLEWARE_CLASSES += (
            "storm.django.middleware.ZopeTransactionMiddleware",)

    def tearDown(self):
        settings._target = None
        global_zstorm._reset()
        stores.have_configured_stores = False
        transaction.manager.free(transaction.get())
        super(DjangoBackendTests, self).tearDown()

    def get_store_uri(self):
        raise NotImplementedError
