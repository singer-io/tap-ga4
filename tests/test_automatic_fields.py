from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest

from base import GA4Base


class GA4MinimumSelectionTest(MinimumSelectionTest, GA4Base):
    """Standard Automatic Fields Test"""

    @staticmethod
    def name():
        return "tt_ga4_auto"

    def streams_to_test(self):
        # We have no test data for the in_app_purchases stream
        return self.expected_stream_names().difference({'in_app_purchases'})
