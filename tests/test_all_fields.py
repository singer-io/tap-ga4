import os
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from datetime import datetime as dt
from datetime import timedelta

from base import GA4Base


class GA4AllFieldsTest(AllFieldsTest, GA4Base):
    """GA4 all fields test implementation """


    @staticmethod
    def name():
        return "tt_ga4_all_fields"


    def streams_to_test(self):
        # testing all streams creates massive quota issues
        custom_id = self.custom_reports_names_to_ids()['Test Report 1']
        return {
            custom_id
        }


    def streams_to_selected_fields(self):
        return {
            "Test Report 1": {
                "date_hour_minute",
                "active_users",
            },
        }


    def __init__(self, test_run):
        super().__init__(test_run)
