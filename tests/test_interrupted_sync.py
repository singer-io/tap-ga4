import os
from datetime import datetime as dt, timedelta, timezone

from base import GA4Base, get_jira_status_category
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class GA4InterruptedSyncTest(InterruptedSyncTest, GA4Base):
    """GA4 interrupted sync test implementation"""

    bookmark_format = "%Y-%m-%d"
    start_date = GA4Base.timedelta_formatted(dt.now(), delta=timedelta(days=-35))
    interrupted_bookmark_date = GA4Base.timedelta_formatted(
        dt.now(), delta=timedelta(days=-20), date_format=bookmark_format)
    completed_bookmark_date = GA4Base.timedelta_formatted(dt.now(), delta=timedelta(days=-15),
                                                          date_format=bookmark_format)
    card_is_done = None

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
        return {
            'content_group_report': self.expected_automatic_fields()['content_group_report'],
            'demographic_country_report': self.expected_automatic_fields()['demographic_country_report'],
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
    # Tests and Functions To Override
    ##########################################################################

    def calculate_expected_sync_start_time(self, bookmark, stream, completed=True):
        """This method is only for streams that have bookmarks and a sync has been started"""

        bookmark = self.get_bookmark_value(self.resuming_sync_state, stream)
        start_date = self.parse_date(self.start_date)

        if not bookmark:
            return start_date

        bookmark = self.parse_date(bookmark)
        conversion_window = int(self.get_properties().get('conversion_window', GA4Base.CONVERSION_WINDOW))
        conversion_day = dt.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=conversion_window)
        return min(bookmark, max(start_date, conversion_day))
