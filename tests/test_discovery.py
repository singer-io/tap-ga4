import re
import unittest

from tap_tester import menagerie, connections
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest

from base import GA4Base


class GA4DiscoveryTest(GA4Base, DiscoveryTest):
    """Standard Discovery Test"""

    @staticmethod
    def name():
        return "tt_ga4_discovery"

    def streams_to_test(self):
        return set(self.expected_metadata().keys())

    @unittest.skip("Does Not Apply")
    def test_2_stream_naming(self):
        """
        This tap accepts user provided stream names in the config via the report_definitions
        field and does not conform to the expaction that a stream's name must satisfy the condition:

            re.fullmatch(r"[a-z_]+", name)

        So this test case is skipped.
        """
