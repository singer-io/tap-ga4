import os
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest

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
        return {
            'content_group_report',
            'Test Report 1',
        }

    def manipulate_state(self, old_state):

        manipulated_state = {
            'bookmarks': {
                stream_id: { os.getenv('TAP_GA4_PROPERTY_ID'): {'date': self.bookmark_date}}
                for stream_id in old_state['bookmarks'].keys()
            }
        }

        return manipulated_state

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
        self.bookmark_date = (dt.utcnow() - timedelta(days=1)).strftime(self.REPLICATION_KEY_FORMAT)
