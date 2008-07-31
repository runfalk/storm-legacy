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
    from django import conf
    from django.http import HttpRequest, HttpResponse
    import transaction
except ImportError:
    have_django_and_transaction = False
else:
    have_django_and_transaction = True
    from storm.django.middleware import ZopeTransactionMiddleware

from tests.helper import TestHelper


class TransactionMiddlewareTests(TestHelper):

    def is_supported(self):
        return have_django_and_transaction

    def setUp(self):
        super(TransactionMiddlewareTests, self).setUp()
        conf.settings.configure(STORM_COMMIT_SAFE_METHODS=False)

    def tearDown(self):
        conf.settings._target = None
        super(TransactionMiddlewareTests, self).tearDown()

    def test_process_request_begins_transaction(self):
        begin = self.mocker.replace("transaction.begin")
        self.expect(begin()).result(None)
        self.mocker.replay()

        zope_middleware = ZopeTransactionMiddleware()
        request = HttpRequest()
        request.method = "GET"
        zope_middleware.process_request(request)

    def test_process_exception_aborts_transaction(self):
        abort = self.mocker.replace("transaction.abort")
        self.expect(abort()).result(None)
        self.mocker.replay()

        zope_middleware = ZopeTransactionMiddleware()
        request = HttpRequest()
        request.method = "GET"
        exception = RuntimeError("some error")
        zope_middleware.process_exception(request, exception)

    def test_process_response_commits_transaction(self):
        commit = self.mocker.replace("transaction.commit")
        # We test three request methods
        self.expect(commit()).result(None)
        self.expect(commit()).result(None)
        self.expect(commit()).result(None)
        self.mocker.replay()

        # Commit on all methods
        conf.settings.STORM_COMMIT_SAFE_METHODS = True

        zope_middleware = ZopeTransactionMiddleware()
        request = HttpRequest()
        response = HttpResponse()

        request.method = "GET"
        zope_middleware.process_response(request, response)
        request.method = "HEAD"
        zope_middleware.process_response(request, response)
        request.method = "POST"
        zope_middleware.process_response(request, response)

    def test_process_response_aborts_transaction_for_safe_methods(self):
        abort = self.mocker.replace("transaction.abort")
        commit = self.mocker.replace("transaction.commit")
        # We test three request methods
        self.expect(abort()).result(None)
        self.expect(abort()).result(None)
        self.expect(commit()).result(None)
        self.mocker.replay()

        # Don't commit on safe methods
        conf.settings.STORM_COMMIT_SAFE_METHODS = False

        zope_middleware = ZopeTransactionMiddleware()
        request = HttpRequest()
        response = HttpResponse()

        request.method = "GET"
        zope_middleware.process_response(request, response)
        request.method = "HEAD"
        zope_middleware.process_response(request, response)
        request.method = "POST"
        zope_middleware.process_response(request, response)
