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

    def streams_to_selected_fields(self):
        return {
            "Test Report 1": {  # engagement events
                "conversions",
                "defaultChannelGrouping",
                "eventName",
                "eventCount",
                "newUsers",
                "enagementRate",
                "engagedSessions",
            },
            "Test Report 2": {  # retention
                "pageTitle",
                "newUsers",
                "cohortActiveUsers",
                "cohortTotalUsers",
            },
        }
