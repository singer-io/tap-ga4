from tap_tester.base_suite_tests.discovery_test import DiscoveryTest

from base import GA4Base


class GA4DiscoveryTest(DiscoveryTest, GA4Base):
    """Standard Discovery Test"""

    @staticmethod
    def name():
        return "tt_ga4_discovery"

    def streams_to_test(self):
        return self.expected_stream_names()

    def test_stream_naming(self):
        def naming_streams_to_test():
            return self.expected_stream_names().difference({
                'Test Report 1',
                'Test Report 2',
                'ecommerce_purchases_item_category_2_report',
                'ecommerce_purchases_item_category_3_report',
                'ecommerce_purchases_item_category_4_report',
                'ecommerce_purchases_item_category_5_report'})
        self.streams_to_test = naming_streams_to_test
        super().test_stream_naming()
        """
        This tap accepts user provided stream names in the config via the report_definitions field
        and does not conform to the expectation that a stream's name must satisfy the condition:

            re.fullmatch(r"[a-z_]+", name)

        So this test case is skipped.
        """
