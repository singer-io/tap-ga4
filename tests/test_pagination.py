from datetime import datetime as dt
from datetime import timedelta

from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base import GA4Base


class GA4PaginationTest(PaginationTest, GA4Base):
    """GA4 pagination test implementation """

    start_date = GA4Base.timedelta_formatted(dt.now(), delta=timedelta(days=-330))
    request_window_size = 100

    @staticmethod
    def name():
        return "tt_ga4_pagination"

    @staticmethod
    def streams_to_test():
        # testing all streams creates massive quota issues
        return {'Test Report 1'}

    @staticmethod
    def streams_to_selected_fields():
        return {
            "Test Report 1": {
                "date_hour_minute",
                "active_users",
            },
        }
