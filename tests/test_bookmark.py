import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

from base import GA4Base
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest

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


    ##########################################################################
    ### Tap Specific Tests
    ##########################################################################


    def test_bookmark_values_are_today(self):
        """
        For taps with a BOOKMARK_FORMAT of "%Y-%m-%d", these assertions are
        valid.
        """
        today_datetime = dt.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                # gather results
                stream_bookmark_1 = self.bookmarks_1.get(stream)
                stream_bookmark_2 = self.bookmarks_2.get(stream)

                bookmark_value_1 = self.get_bookmark_value(self.state_1, stream)
                bookmark_value_2 = self.get_bookmark_value(self.state_2, stream)

                # Verify the bookmark is set based on sync end date (today) for sync 1
                # (The tap replicaates from the start date through to today)
                parsed_bookmark_value_1 = self.parse_date(bookmark_value_1)
                self.assertEqual(parsed_bookmark_value_1, today_datetime)

                # Verify the bookmark is set based on sync execution time for sync 2
                # (The tap replicaates from the manipulated state through to todayf)
                parsed_bookmark_value_2 = self.parse_date(bookmark_value_2)
                self.assertEqual(parsed_bookmark_value_2, today_datetime)


    ##########################################################################
    ### Tests To Skip
    ##########################################################################


    @unittest.skip("Second sync bookmark will almost never be greater than the first sync. Bookmark value truncates to the day.")
    def test_sync_2_bookmark_greater_than_sync_1(self):
        pass


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
