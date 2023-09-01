"""Test tap configurable properties. Specifically the conversion_window"""
import os
from datetime import datetime as dt
from datetime import timedelta
import pytz
from tap_tester import menagerie, connections, runner

from base import GA4Base


class ConversionWindowBaseTest(GA4Base):
    """
    Test tap's sync mode can execute with valid conversion_window values set.
    Validate setting the conversion_window configurable property.

    Test Cases:

    Verify tap throws critical error when a value is provided directly by a user which is
    outside the set of acceptable values.

    Verify connection can be created, and tap can discover and sync with a conversion window
    set to the following values
      Acceptable values: { 30, 60, 90 }
    """
    conversion_window = ''
    bookmark_format = "%Y-%m-%d"
    start_date = ''

    @classmethod
    def name(cls):
        return f"tt_ga4_conv_window_{cls.conversion_window}"

    @staticmethod
    def streams_to_test():
        # testing all streams creates massive quota issues
        # custom_id = self.custom_reports_names_to_ids()['Test Report 1']
        return {
            'content_group_report',
        }

    def run_test(self):
        """
        Testing that basic sync functions without Critical Errors when
        a valid conversion_window is set.
        """

        # Create a connection
        conn_id = connections.ensure_connection(self)

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select tables and fields
        test_catalogs = [catalog
                         for catalog in found_catalogs
                         if catalog['stream_name'] in self.streams_to_test()]
        streams_to_selected_fields = {stream: set() for stream in self.streams_to_test()}
        self.select_streams_and_fields(conn_id, test_catalogs, streams_to_selected_fields)

        # set state to ensure conversion window is used
        today_datetime = dt.strftime(dt.utcnow(), self.bookmark_format)
        initial_state = {
            'bookmarks': {
                stream: {os.getenv('TAP_GA4_PROPERTY_ID'): {'last_report_date': today_datetime}}
                for stream in self.streams_to_test()
            }, 'currently_syncing': None
        }

        menagerie.set_state(conn_id, initial_state)

        # Run a sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify the tap and target do not throw a critical error
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify tap replicates through today by check state
        final_state = menagerie.get_state(conn_id)
        self.assertDictEqual(final_state, initial_state)

        # Verify converstion window is used
        synced_messages_by_stream = runner.get_records_from_target_output()
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):

                stream_name = self.get_stream_name(stream)
                expected_replication_key = next(iter(
                    self.expected_metadata().get(stream_name).get(self.REPLICATION_KEYS)))
                self.assertTrue(expected_replication_key)

                replication_dates = [
                    row.get('data').get(expected_replication_key) for row in
                    synced_messages_by_stream.get(stream, {'messages': []}).get('messages', [])
                    if row.get('data')]
                self.assertTrue(replication_dates)

                conversion_window_int = int(self.CONVERSION_WINDOW)
                oldest_date = self.parse_date(min(replication_dates))
                oldest_possible_date = (dt.utcnow().replace(
                    hour=0, minute=0, second=0, microsecond=0) -
                        timedelta(days=conversion_window_int)).replace(tzinfo=pytz.utc)
                start_date = self.parse_date(self.start_date)

                # Verify start date is before the oldest replicated record per test set up
                self.assertGreater(oldest_date, start_date)
                # Verify the oldest record is not older than conversion window
                self.assertGreaterEqual(oldest_date, oldest_possible_date)
                # Verify minimal gap between oldest record and conversion_window
                # allow a 1 day gap for UTC / day rollover, stream generates 1 rec per day
                self.assertGreaterEqual(timedelta(days=1), (oldest_date - oldest_possible_date))


class ConversionWindowTestThirty(ConversionWindowBaseTest):

    CONVERSION_WINDOW = '30'

    def test_run(self):
        self.start_date = (dt.utcnow() - timedelta(days=33)).strftime(self.START_DATE_FORMAT)
        self.run_test()


class ConversionWindowTestSixty(ConversionWindowBaseTest):

    CONVERSION_WINDOW = '60'

    def test_run(self):
        self.start_date = (dt.utcnow() - timedelta(days=62)).strftime(self.START_DATE_FORMAT)
        self.run_test()


class ConversionWindowTestNinety(ConversionWindowBaseTest):

    CONVERSION_WINDOW = '90'

    def test_run(self):
        self.start_date = (dt.utcnow() - timedelta(days=91)).strftime(self.START_DATE_FORMAT)
        self.run_test()
