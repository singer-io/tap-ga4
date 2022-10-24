import os
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest
from singer import utils
from datetime import datetime as dt
from datetime import timedelta

from base import GA4Base


class GA4BookmarkTest(BookmarkTest, GA4Base):
    """Standard Start Date Test"""

    @staticmethod
    def name():
        return "tt_ga4_bookmark"

    def streams_to_test(self):
        # testing all streams creates massive quota issues
        # return set(self.expected_metadata().keys())
        custom_id = self.custom_reports_names_to_ids()['Test Report 1']
        return {
            'content_group_report',
            custom_id
        }

    def manipulate_state(self, old_state):
        type(self).bookmark_date = (dt.utcnow() - timedelta(days=1)).strftime(self.BOOKMARK_FORMAT)
        manipulated_state = {
            'bookmarks': {
                stream_id: { os.getenv('TAP_GA4_PROPERTY_ID'): {'last_report_date': self.bookmark_date}}
                for stream_id in old_state['bookmarks'].keys()
            }
        }

        return manipulated_state

    def get_bookmark_value(self, bookmark, stream):
        return bookmark.get(os.getenv('TAP_GA4_PROPERTY_ID')).get('last_report_date')

    def get_sync_2_start_time(self, stream):
        conversion_day = utils.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None) - timedelta(days=self.lookback_window)
        bookmark_datetime = dt.strptime(self.bookmark_date, self.BOOKMARK_FORMAT)
        start_date_datetime = dt.strptime(self.start_date, self.START_DATE_FORMAT)
        return  min(bookmark_datetime, max(start_date_datetime, conversion_day))

    def streams_to_selected_fields(self):
        return {
            "Test Report 1": {  # engagement events
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
        self.start_date = (dt.utcnow() - timedelta(days=40)).strftime(self.START_DATE_FORMAT)
        self.lookback_window = int(self.CONVERSION_WINDOW)
