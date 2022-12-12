from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest

from base import GA4Base


class GA4MinimumSelectionTest(MinimumSelectionTest, GA4Base):
    """Standard Sync Canary Test"""

    @staticmethod
    def name():
        return "tt_ga4_auto"

    def streams_to_test(self):
        streams_to_test = set(self.expected_metadata().keys())
        # We have no test data for in_app_purchases stream
        streams_to_test.remove("in_app_purchases")
        return streams_to_test
