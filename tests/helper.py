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
import tempfile
import logging
import shutil
import sys

from storm.compat import iter_zip, StringIO
from tests import mocker


__all__ = ["assert_variables_equal", "TestHelper", "MakePath"]


def assert_variables_equal(checked, expected):
    assert len(checked) == len(expected)
    for check, expect in iter_zip(checked, expected):
        assert check.__class__ == expect.__class__
        assert check.get() == expect.get()


class TestHelper(mocker.MockerTestCase):
    helpers = []

    def is_supported(self):
        return True

    def setUp(self):
        super(TestHelper, self).setUp()
        self._helper_instances = []
        for helper_factory in self.helpers:
            helper = helper_factory()
            helper.set_up(self)
            self._helper_instances.append(helper)

    def tearDown(self):
        for helper in reversed(self._helper_instances):
            helper.tear_down(self)
        super(TestHelper, self).tearDown()

    @property
    def _testMethod(self):
        try:
            name = self._testMethodName
        except AttributeError:
            # On Python < 2.5
            name = self._TestCase__testMethodName
        return getattr(self, name)

    def run(self, result=None):
        # Skip if is_supported() does not return True.
        if not self.is_supported():
            if hasattr(result, "addSkip"):
                result.startTest(self)
                result.addSkip(self, "Test not supported")
            return
        super(TestHelper, self).run(result)


class MakePath(object):

    def set_up(self, test_case):
        self.dirname = tempfile.mkdtemp()
        self.dirs = []
        self.counter = 0
        test_case.make_dir = self.make_dir
        test_case.make_path = self.make_path

    def tear_down(self, test_case):
        shutil.rmtree(self.dirname)
        [shutil.rmtree(dir) for dir in self.dirs]

    def make_dir(self):
        path = tempfile.mkdtemp()
        self.dirs.append(path)
        return path

    def make_path(self, content=None, path=None):
        if path is None:
            self.counter += 1
            path = "%s/%03d" % (self.dirname, self.counter)
        if content is not None:
            file = open(path, "w")
            try:
                file.write(content)
            finally:
                file.close()
        return path
