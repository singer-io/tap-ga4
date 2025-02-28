import os
from copy import deepcopy
from datetime import datetime as dt, timedelta
import pytz
from base import GA4Base
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class GA4BookmarkTest(BookmarkTest, GA4Base):
    """
    GA4 bookmark test implementation

    MRO for test
    [<class 'test_bookmark.GA4BookmarkTest'>,
     <class 'tap_tester.base_suite_tests.bookmark_test.BookmarkTest'>,
     <class 'base.GA4Base'>,
     <class 'tap_tester.base_suite_tests.base_case.BaseCase'>,
     <class 'unittest.case.TestCase'>,
     <class 'object'>]
    """

    @property
    def start_date(self):
        """ensure first sync start_date is before the CONVERSION_WINDOW"""
        return self.timedelta_formatted(
            dt.utcnow(), delta=timedelta(days=-int(GA4Base.CONVERSION_WINDOW)-5))

    @staticmethod
    def streams_to_test():
        # testing all streams creates massive quota issues
        return {
            'content_group_report',
            'Test Report 1'
        }

    bookmark_format = "%Y-%m-%d"
    initial_bookmarks = {}

    @staticmethod
    def name():
        return "tt_ga4_bookmark"

    def manipulate_state(self, state: dict, new_bookmarks: dict):
        """
        This method will update the passed state with the new_bookmarks.

        new_bookmarks must be in the format { stream: {replication_key: replication_value}}
        """
        new_state = deepcopy(state)
        if new_state.get('bookmarks') is None:
            new_state['bookmarks'] = {}
        for stream, rep in new_bookmarks.items():
            if new_state['bookmarks'].get(stream):
                for value in rep.values():
                    new_state['bookmarks'][stream][os.getenv('TAP_GA4_PROPERTY_ID')] = \
                        {'last_report_date': value}
            else:
                # It is expected there will be only one key value pair.
                # if that is not the case this will only use the last value in the key value pair
                for value in rep.values():
                    new_state['bookmarks'][stream] = {
                        os.getenv('TAP_GA4_PROPERTY_ID'): {'last_report_date': value}}
        return new_state

    def get_bookmark_value(self, state, stream):
        bookmark = state.get('bookmarks', {})
        stream_bookmark = bookmark.get(self.get_stream_id(stream))
        if stream_bookmark:
            return stream_bookmark.get(os.getenv('TAP_GA4_PROPERTY_ID')).get('last_report_date')
        return None

    @staticmethod
    def streams_to_selected_fields():
        return {
            "Test Report 1": {"total_users"},
            'content_group_report': {"date"},
        }

    ##########################################################################
    # Tap Specific Tests
    ##########################################################################

    # TODO - for some reason look_back windows set the bookmark to now
    #   and not to the last record.  Find out if this is correct and why?
    def test_first_sync_bookmark(self):
        today_datetime = dt.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=pytz.utc)
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                # gather results
                bookmark_value_1 = self.parse_date(self.get_bookmark_value(self.state_1, stream))

                # Verify the bookmark is set based on sync end date (today) for sync 1
                # (The tap replicates from the start date through to today)
                self.assertEqual(bookmark_value_1, today_datetime)

    # TODO - for some reason look_back windows set the bookmark to now
    #   and not to the last record.  Find out if this is correct and why?
    def test_second_sync_bookmark(self):
        today_datetime = dt.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=pytz.utc)
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                # gather results
                bookmark_value_2 = self.parse_date(self.get_bookmark_value(self.state_2, stream))

                # Verify the bookmark is set based on sync end date (today) for sync 2
                # (The tap replicates from the start date through to today)
                self.assertEqual(bookmark_value_2, today_datetime)

    ##########################################################################
    # Tests To Skip
    ##########################################################################
