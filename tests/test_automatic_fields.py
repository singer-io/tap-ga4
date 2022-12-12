from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest

from base import GA4Base


class GA4MinimumSelectionTest(MinimumSelectionTest, GA4Base):
    """Standard Sync Canary Test"""

    @staticmethod
    def name():
        return "tt_ga4_auto"

    def streams_to_test(self):
        return set(self.expected_metadata().keys())
