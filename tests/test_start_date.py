from tap_tester.base_suite_tests.start_date_test import StartDateTest

from base import GA4Base


class GA4StartDateTest(StartDateTest, GA4Base):
    """Standard Sync Canary Test"""

    @staticmethod
    def name():
        return "tt_ga4_start_date"

    def streams_to_test(self):
        return set(self.expected_metadata().keys())

    # TODO
    self.start_date_1 = '2021-04-07T00:00:00Z'
    self.start_date_2 = self.timedelta_formatted(
        self.start_date_1, days=2, date_format=self.START_DATE_FORMAT
        )
