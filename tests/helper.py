from cStringIO import StringIO
import unittest
import tempfile
import logging
import shutil
import sys


class TestHelper(unittest.TestCase):

    helpers = []

    def setUp(self):
        self._helper_instances = []
        for helper_factory in self.helpers:
            helper = helper_factory()
            helper.set_up(self)
            self._helper_instances.append(helper)

    def tearDown(self):
        for helper in reversed(self._helper_instances):
            helper.tear_down(self)


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


class LogKeeper(object):
    """Record logging information.

    Puts a 'logfile' attribute on your test case, which is a StringIO
    containing all log output.
    """

    def set_up(self, test_case):
        logger = logging.getLogger()
        test_case.logfile = StringIO()
        handler = logging.StreamHandler(test_case.logfile)
        self.old_handlers = logger.handlers
        # Sanity check; this might not be 100% what we want
        if self.old_handlers:
            test_case.assertEquals(len(self.old_handlers), 1)
            test_case.assertEquals(self.old_handlers[0].stream, sys.stderr)
        logger.handlers = [handler]

    def tear_down(self, test_case):
        logging.getLogger().handlers = self.old_handlers