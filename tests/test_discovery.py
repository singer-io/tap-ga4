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
            # TODO - Why do we use numbers
            #   and possibly have 5 of the same report for non custom reports
            return self.expected_stream_names().difference({
                'Test Report 1',
                'Test Report 2',
                'ecommerce_purchases_item_category_2_report',
                'ecommerce_purchases_item_category_3_report',
                'ecommerce_purchases_item_category_4_report',
                'ecommerce_purchases_item_category_5_report'})
        self.streams_to_test = naming_streams_to_test
        super().test_stream_naming()
