from datetime import datetime as dt, timedelta
from tap_tester.base_suite_tests.start_date_test import StartDateTest

from base import GA4Base


class GA4StartDateTest(StartDateTest, GA4Base):
    """Standard Start Date Test"""

    start_date_1 = GA4Base.timedelta_formatted(dt.now(), delta=timedelta(days=-4))
    start_date_2 = GA4Base.timedelta_formatted(start_date_1, delta=timedelta(days=2))

    @staticmethod
    def name():
        return "tt_ga4_start_date"

    @staticmethod
    def streams_to_test():
        # testing all streams creates massive quota issues
        # return self.expected_stream_names()
        return {'content_group_report',
                'demographic_region_report',
                # 'demographic_age_report',
                'traffic_acq_session_source_and_medium_report',
                # 'conversions_report', TODO Enable once filters are implemented
                # 'Test Report 2',
                }  # TODO add stream that does not obey start date if one exists
