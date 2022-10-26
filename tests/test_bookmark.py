import os
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest
from datetime import datetime as dt
from datetime import timedelta

from base import GA4Base


class GA4BookmarkTest(BookmarkTest, GA4Base):
    """GA4 bookmark test implementation"""


    @staticmethod
    def name():
        return "tt_ga4_bookmark"


    def streams_to_test(self):
        # testing all streams creates massive quota issues
        custom_id = self.custom_reports_names_to_ids()['Test Report 1']
        return {
            'content_group_report',
            custom_id
        }


    def manipulate_state(self, old_state):
        manipulated_state = {
            'bookmarks': {
                stream_id: { os.getenv('TAP_GA4_PROPERTY_ID'): {'last_report_date': self.bookmark_date}}
                for stream_id in old_state['bookmarks'].keys()
            }
        }

        return manipulated_state


    def get_bookmark_value(self, bookmark, stream):
        return bookmark.get(os.getenv('TAP_GA4_PROPERTY_ID')).get('last_report_date')


    def streams_to_selected_fields(self):
        return {
            "Test Report 1": {
                "conversions",
                "defaultChannelGrouping",
                "eventName",
                "eventCount",
                "newUsers",
                "enagementRate",
                "engagedSessions",
            },
            'content_group_report': {
                "date",
                "browser",
                "conversions",
            },
        }


    # set default values for test in init
    def __init__(self, test_run):
        super().__init__(test_run)
        self.start_date = self.timedelta_formatted(dt.now(),
                                                   days=-40,
                                                   date_format=self.START_DATE_FORMAT)

        self.bookmark_date = self.timedelta_formatted(dt.now(),
                                                      days=-1,
                                                      date_format=self.BOOKMARK_FORMAT)

        self.lookback_window = int(self.CONVERSION_WINDOW)
