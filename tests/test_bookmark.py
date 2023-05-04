import os
from copy import deepcopy
from datetime import datetime as dt, timedelta
from unittest import skip

from base import GA4Base
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class GA4BookmarkTest(BookmarkTest, GA4Base):
    """GA4 bookmark test implementation"""

    start_date = GA4Base.timedelta_formatted(dt.utcnow(), delta=timedelta(days=-15))

    @staticmethod
    def streams_to_test():
        # testing all streams creates massive quota issues
        # custom_id = self.custom_reports_names_to_ids()['Test Report 1']
        return {
            'content_group_report',
            'Test Report 1'  # TODO - net getting data for bookmarks
        }

    BOOKMARK_FORMAT = "%Y-%m-%d"

    def INITIAL_BOOKMARKS(self):
        return dict()
        # return {
        #     'bookmarks': {
        #         self.custom_reports_names_to_ids().get(stream, stream): {
        #             os.getenv('TAP_GA4_PROPERTY_ID'): {
        #                 'last_report_date':
        #                     GA4Base.timedelta_formatted(dt.utcnow(),
        #                                                 delta=timedelta(days=-40),
        #                                                 date_format=self.BOOKMARK_FORMAT)}}
        #         for stream in self.streams_to_test()
        #     }
        # }

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
            new_state['bookmarks'] = dict()
        for stream, rep in new_bookmarks.items():
            if new_state['bookmarks'].get(stream):
                for k, v in rep.items():
                    new_state['bookmarks'][stream][os.getenv('TAP_GA4_PROPERTY_ID')] = \
                        {'last_report_date': v}
            else:
                # It is expected there will be only one key value pair.
                # if that is not the case this will only use the last value in the key value pair
                for v in rep.values():
                    new_state['bookmarks'][stream] = {
                        os.getenv('TAP_GA4_PROPERTY_ID'): {'last_report_date': v}}
        return new_state

    def get_bookmark_value(self, state, stream):
        bookmark = state.get('bookmarks', {})
        stream_bookmark = bookmark.get(self.get_stream_id(stream))
        if stream_bookmark:
            return stream_bookmark.get(os.getenv('TAP_GA4_PROPERTY_ID')).get('last_report_date')
        return None

    def calculate_new_bookmarks(self):
        new_bookmarks = dict()
        for stream, records in BookmarkTest.synced_records_1.items():
            replication_method = self.expected_replication_method()[stream]
            if replication_method == self.INCREMENTAL:
                look_back = self.expected_lookback_window()[stream]
                replication_key = self.expected_replication_keys()[stream]
                assert len(replication_key) == 1
                replication_key = next(iter(replication_key))

                # get the replication values that are prior to the lookback window
                replication_values = sorted({
                    message['data'][replication_key] for message in records['messages']
                    if message['action'] == 'upsert'
                       and self.parse_date(message['data'][replication_key]) <
                       self.parse_date(self.get_bookmark_value(
                           BookmarkTest.state_1, self.get_stream_id(stream))) - look_back})
                print(f"unique replication values for stream {stream} are: {replication_values}")

                # There should be 3 or more records (prior to the look back window)
                # so we can set the bookmark to get the last 2 records (+ the look back)
                # self.assertGreater(len(replication_values), 2,
                #                    msg="We need to have more than two replication dates "
                #                        "to test a stream")
                new_bookmarks[self.get_stream_id(stream)] = {
                    replication_key:
                        self.timedelta_formatted(self.parse_date(replication_values[-2]),
                                                 date_format=self.BOOKMARK_FORMAT)}
        return new_bookmarks

    @staticmethod
    def streams_to_selected_fields():
        return {
            "Test Report 1": {
                "conversions",
                # "defaultChannelGrouping",  # Are these are valid selections?
                # "eventName",
                # "eventCount",
                # "newUsers",
                # "enagementRate",
                # "engagedSessions",
            },
            'content_group_report': set()
            #     {
            #         "date",  # Are these are valid selections?
            #         "browser",
            #         "conversions",
            # },
        }

    @classmethod
    def get_stream_id(cls, stream_name):
        """
        Returns the stream_id given the stream_name because synced_records
        from the target output batches records by stream_name

        Since the GA4 tap_stream_id is a UUID instead of the usual case of
        tap_stream_id == stream_name, we need to get the stream_name that
        maps to tap_stream_id
        """
        stream_mapping = {
            "Test Report 1": cls.custom_report_id_1,
            "Test Report 2": cls.custom_report_id_2,
        }
        return stream_mapping.get(stream_name, stream_name)

    @classmethod
    def get_stream_name(cls, tap_stream_id):
        """
        Returns the stream_name given the stream_id because bookmarks uses stream_id

        Since the GA4 tap_stream_id is a UUID instead of the usual case of
        tap_stream_id == stream_name, we need to get the tap_stream_id that
        maps to stream_name
        """
        stream_mapping = {
            cls.custom_report_id_1: "Test Report 1",
            cls.custom_report_id_2: "Test Report 2",
        }
        return stream_mapping.get(tap_stream_id, tap_stream_id)

    ##########################################################################
    # Tap Specific Tests
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
                bookmark_value_1 = self.get_bookmark_value(self.state_1, stream)
                bookmark_value_2 = self.get_bookmark_value(self.state_2, stream)

                # Verify the bookmark is set based on sync end date (today) for sync 1
                # (The tap replicates from the start date through to today)
                parsed_bookmark_value_1 = self.parse_date(bookmark_value_1)
                self.assertEqual(parsed_bookmark_value_1, today_datetime)

                # Verify the bookmark is set based on sync execution time for sync 2
                # (The tap replicates from the manipulated state through to today)
                parsed_bookmark_value_2 = self.parse_date(bookmark_value_2)
                self.assertEqual(parsed_bookmark_value_2, today_datetime)

    ##########################################################################
    # Tests To Skip
    ##########################################################################

    # TODO - for some reason look_back windows set the bookmark to now
    #   and not to the last record.  Find out if this is correct and why?
    def test_first_sync_bookmark(self):
        today_datetime = dt.utcnow().date()
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
        today_datetime = dt.utcnow().date()
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                # gather results
                bookmark_value_2 = self.parse_date(self.get_bookmark_value(self.state_2, stream))

                # Verify the bookmark is set based on sync end date (today) for sync 2
                # (The tap replicates from the start date through to today)
                self.assertEqual(bookmark_value_2, today_datetime)
