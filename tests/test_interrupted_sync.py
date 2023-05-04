import os
from datetime import datetime as dt, timedelta

from base import GA4Base
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class GA4InterruptedSyncTest(InterruptedSyncTest, GA4Base):
    """GA4 interrupted sync test implementation"""


    @staticmethod
    def name():
        return "tt_ga4_interrupted_sync"


    def streams_to_test(self):
        # testing all streams creates massive quota issues
        return {
            'content_group_report',
            'demographic_country_report',
            'events_report',
            'tech_browser_report',
        }


    def manipulate_state(self):
        return {
            'currently_syncing': 'demographic_country_report',
            'bookmarks' : {
                'content_group_report': {
                    os.getenv('TAP_GA4_PROPERTY_ID'): {'last_report_date': self.completed_bookmark_date}
                },
                'demographic_country_report': {
                    os.getenv('TAP_GA4_PROPERTY_ID'): {'last_report_date': self.interrupted_bookmark_date}
                },
            }
        }


    # set default values for test in init
    def __init__(self, test_run):
        super().__init__(test_run)
        self.start_date = self.timedelta_formatted(dt.now(), delta=timedelta(days=-32))
        self.interrupted_bookmark_date = self.timedelta_formatted(dt.now(),
                                                                  delta=timedelta(days=-31),
                                                                  date_format=self.BOOKMARK_FORMAT)

        self.completed_bookmark_date = self.timedelta_formatted(dt.now(),
                                                                date_format=self.BOOKMARK_FORMAT)

        self.lookback_window = int(self.CONVERSION_WINDOW)
