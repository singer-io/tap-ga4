from datetime import datetime as dt
from tap_tester.base_suite_tests.start_date_test import StartDateTest

from base import GA4Base


class GA4StartDateTest(StartDateTest, GA4Base):
    """Standard Start Date Test"""

    @staticmethod
    def name():
        return "tt_ga4_start_date"

    def streams_to_test(self):
        # testing all streams creates massive quota issues
        # return set(self.expected_metadata().keys())
        return {'content_group_report',
                'demographic_region_report',
                #'demographic_age_report',
                'traffic_acq_session_source_and_medium_report',
                # 'conversions_report', TODO Enable once filters are implemented
                #'Test Report 2',
                } # TODO add stream that does not obey start date if one exists


    # set default values for test in init
    def __init__(self, test_run):
        super().__init__(test_run)
        self.start_date_1 = self.timedelta_formatted(
            dt.now(), days=-4, date_format=self.START_DATE_FORMAT # Set start_date to 4 days ago to reduce quota usage
        )
        self.start_date_2 = self.timedelta_formatted(
            self.start_date_1, days=2, date_format=self.START_DATE_FORMAT
        )
