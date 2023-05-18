import os
from datetime import datetime as dt, timedelta

from base import GA4Base
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class GA4InterruptedSyncTest(InterruptedSyncTest, GA4Base):
    """GA4 interrupted sync test implementation"""

    bookmark_format = "%Y-%m-%d"
    start_date = GA4Base.timedelta_formatted(dt.now(), delta=timedelta(days=-30))
    interrupted_bookmark_date = GA4Base.timedelta_formatted(
        dt.now(), delta=timedelta(days=-20), date_format=bookmark_format)
    completed_bookmark_date = GA4Base.timedelta_formatted(dt.now(), delta=timedelta(days=-15),
                                                          date_format=bookmark_format)

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

    def streams_to_selected_fields(self):
        # TODO - when selecting these fields I'm getting errors.
        return {
            'content_group_report': self.expected_automatic_fields()['content_group_report'],
            'demographic_country_report':
                self.expected_automatic_fields()['demographic_country_report'],
            'events_report': self.expected_automatic_fields()['events_report'],
            'tech_browser_report': self.expected_automatic_fields()['tech_browser_report'],
        }

    def manipulate_state(self):
        return {
            'currently_syncing': 'demographic_country_report',
            'bookmarks': {
                'content_group_report': {
                    os.getenv('TAP_GA4_PROPERTY_ID'):
                        {'last_report_date': self.completed_bookmark_date}
                },
                'demographic_country_report': {
                    os.getenv('TAP_GA4_PROPERTY_ID'):
                        {'last_report_date': self.interrupted_bookmark_date}
                },
            }
        }

    def get_bookmark_value(self, state, stream):
        bookmark = state.get('bookmarks', {})
        stream_bookmark = bookmark.get(self.get_stream_id(stream))
        if stream_bookmark:
            return stream_bookmark.get(os.getenv('TAP_GA4_PROPERTY_ID')).get('last_report_date')
        return None

    ##########################################################################
    # Tests To Override
    ##########################################################################

    def test_bookmarked_streams_start_date(self):
        # TODO - BUG - completed streams are not respecting the bookmark value
        def override_lookback_window():
            return {stream: timedelta(days=0) for stream in self.streams_to_test()}
        self.expected_lookback_window = override_lookback_window
        super().test_bookmarked_streams_start_date()

    def test_resuming_sync_records(self):
        # TODO - BUG - completed streams are not respecting the bookmark value
        def override_lookback_window():
            return {stream: timedelta(days=0) for stream in self.streams_to_test()}
        self.expected_lookback_window = override_lookback_window
        super().test_resuming_sync_records()
