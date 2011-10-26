try:
    TestWithFixtures = object
    from fixtures.testcase import TestWithFixtures
except ImportError:
    has_fixtures = False
else:
    from storm.testing import TracerFixture
    has_fixtures = True

from storm.tracer import CaptureTracer, get_tracers
from tests.tracer import StubConnection
from tests.helper import TestHelper


class TracerFixtureTest(TestHelper, TestWithFixtures):

    def is_supported(self):
        return has_fixtures

    def test_fixture(self):
        """
        The L{CaptureFixture} provides a catpure log that will be closed
        upon test cleanup.
        """
        fixture = self.useFixture(TracerFixture())
        [tracer] = get_tracers()
        conn = StubConnection()
        conn.param_mark = '%s'
        tracer.connection_raw_execute(conn, "cursor", "statement", [])
        self.assertEqual(["statement"], fixture.queries)
        self.assertTrue(isinstance(tracer, CaptureTracer))

        def check():
            self.assertEqual([], get_tracers())

        self.addCleanup(check)
