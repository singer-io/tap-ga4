import os
from tap_tester.base_suite_tests.pagination_test import PaginationTest
from datetime import datetime as dt
from datetime import timedelta

from base import GA4Base


class GA4PaginationTest(PaginationTest, GA4Base):
    """GA4 pagination test implementation """

    @staticmethod
    def name():
        return "tt_ga4_pagination"

    def streams_to_test(self):
        # testing all streams creates massive quota issues
        return {self.get_stream_id('Test Report 1')}

    @staticmethod
    def streams_to_selected_fields():
        return {
            "Test Report 1": {
                "date_hour_minute",
                "active_users",
            },
        }

    def get_page_limit_for_stream(self, stream):
        return self.PAGE_SIZE

    def __init__(self, test_run):
        super().__init__(test_run)
        self.start_date = self.timedelta_formatted(dt.now(), delta=timedelta(days=-100))
        self.request_window_size = '100'
