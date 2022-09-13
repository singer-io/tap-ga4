import unittest

from tap_tester import menagerie, connections
from tap_tester.base_suite_tests.sync_canary_test import SyncCanaryTest

from base import GA4Base


class GA4SyncCanaryTest(GA4Base, SyncCanaryTest):
    """Standard Sync Canary Test"""

    @staticmethod
    def name():
        return "tt_ga4_sync"

    def streams_to_test(self):
        return set(self.expected_metadata().keys())
