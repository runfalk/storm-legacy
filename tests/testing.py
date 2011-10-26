try:
    TestWithFixtures = object
    from fixtures.testcase import TestWithFixtures
except ImportError:
    has_fixtures = False
else:
    from storm.testing import TracerFixture
    has_fixtures = True

from storm.tracer import CaptureTracer, CaptureLog, get_tracers
from tests.helper import TestHelper


class CaptureFixtureTest(TestHelper, TestWithFixtures):

    def is_supported(self):
        return has_fixtures

    def test_fixture(self):
        """
        The L{CaptureFixture} provides a catpure log that will be closed
        upon test cleanup.
        """
        fixture = self.useFixture(TracerFixture())
        [tracer] = get_tracers()
        self.assertTrue(isinstance(fixture.log, CaptureLog))
        self.assertTrue(isinstance(tracer, CaptureTracer))

        def check():
            self.assertEqual([], get_tracers())

        self.addCleanup(check)
