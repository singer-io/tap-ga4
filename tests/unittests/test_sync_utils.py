import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from singer import CatalogEntry, utils
from tap_ga4.sync import (DEFAULT_CONVERSION_WINDOW, generate_sdc_record_hash,
                          get_report_start_date, generate_report_dates,
                          sort_and_shuffle_streams)


class TestRecordHashing(unittest.TestCase):
    """
    Canary test with a constant hash, if this value ever changes, it
    indicates that the primary key has been invalidated by changes.
    """
    def test_record_hash_canary(self):
        test_record = {"property_id": "123456789",
                       "account_id": "123456"}

        dimension_pairs = [('achievementId', 'hi'),
                           ('campaignId', '(not set)'),
                           ('date', '20220906'),
                           ('campaignName', '(my_campaign)'),
                           ('country', 'my_country'),
                           ('city', 'my_city'),
                           ('firstSessionDate', '20220906')]

        expected_hash = "0854e5a26abcccf6990128ab5581b429698e05a6436fc09defe5a22d7f479f9e"
        self.assertEqual(expected_hash, generate_sdc_record_hash(test_record, dimension_pairs))


class TestConversionWindow(unittest.TestCase):
    """
    Test the correct report start date is chosen, between start_date,
    bookmark and conversion_date.

    Cases:
    start_date: bookmark is empty.
                OR
                conversion_date is earlier than the start_date AND bookmark is later than start_date.

    bookmark: bookmark earlier than the conversion_date (this could happen if the tap was paused for awhile).

    conversion_date: the conversion_date is after the start_date AND earlier than the bookmark.
    """
    config = {"start_date": "2022-01-01"}
    property_id = "123456789"

    # Pin now to 9-9-2022 to future-proof tests
    fake_now = datetime(2022, 9, 9, 14, 7, 49, 340301, tzinfo=timezone.utc)
    utils.now = MagicMock(return_value=fake_now)

    def test_conversion_day_is_first_report_date(self):
        state = {"currently_syncing": None, "bookmarks": {"my_stream_id": {"123456789": {"last_report_date": "2022-09-07"}}}}
        expected_date = utils.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=DEFAULT_CONVERSION_WINDOW)
        self.assertEqual(expected_date, get_report_start_date(self.config, self.property_id, state, "my_stream_id"))

    def test_bookmark_is_first_report_date(self):
        state = {"currently_syncing": None, "bookmarks": {"my_stream_id": {"123456789": {"last_report_date": "2022-03-03"}}}}
        expected_date = utils.strptime_to_utc(state["bookmarks"]["my_stream_id"]["123456789"]["last_report_date"])
        self.assertEqual(expected_date, get_report_start_date(self.config, self.property_id, state, "my_stream_id"))

    def test_start_date_is_first_report_date_no_bookmark(self):
        state={}
        expected_date = utils.strptime_to_utc(self.config["start_date"])
        self.assertEqual(expected_date, get_report_start_date(self.config, self.property_id, state, "my_stream_id"))

    def test_start_date_is_first_report_date_bookmark_exists(self):
        state = {"currently_syncing": None, "bookmarks": {"my_stream_id": {"123456789": {"last_report_date": "2022-09-09"}}}}
        self.config["start_date"] = (utils.now() - timedelta(days=3)).strftime("%Y-%m-%d")
        expected_date = utils.strptime_to_utc(self.config["start_date"])
        self.assertEqual(expected_date, get_report_start_date(self.config, self.property_id, state, "my_stream_id"))


class TestGenerateReportDates(unittest.TestCase):

    def test_same_start_and_end_date_week_range(self):
        start_date = datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)

        expected_ranges = [("2022-01-01", "2022-01-01")]
        actual_ranges = []
        for date_range in generate_report_dates(start_date, end_date, 7):
            actual_ranges.append(date_range)

        self.assertEqual(expected_ranges, actual_ranges)

    def test_week_range_week_window(self):
        start_date = datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2022, 1, 7, 0, 0, 0, 0, tzinfo=timezone.utc)

        expected_ranges = [("2022-01-01", "2022-01-07")]
        actual_ranges = []
        for date_range in generate_report_dates(start_date, end_date, 7):
            actual_ranges.append(date_range)

        self.assertEqual(expected_ranges, actual_ranges)

    def test_week_range_uneven_window(self):
        start_date = datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2022, 1, 12, 0, 0, 0, 0, tzinfo=timezone.utc)

        expected_ranges = [("2022-01-01", "2022-01-07"), ("2022-01-08", "2022-01-12")]
        actual_ranges = []
        for date_range in generate_report_dates(start_date, end_date, 7):
            actual_ranges.append(date_range)

        self.assertEqual(expected_ranges, actual_ranges)

    def test_week_range_long_window(self):
        start_date = datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2022, 3, 1, 0, 0, 0, 0, tzinfo=timezone.utc)

        expected_ranges = [('2022-01-01', '2022-01-07'),
                           ('2022-01-08', '2022-01-14'),
                           ('2022-01-15', '2022-01-21'),
                           ('2022-01-22', '2022-01-28'),
                           ('2022-01-29', '2022-02-04'),
                           ('2022-02-05', '2022-02-11'),
                           ('2022-02-12', '2022-02-18'),
                           ('2022-02-19', '2022-02-25'),
                           ('2022-02-26', '2022-03-01')]
        actual_ranges = []
        for date_range in generate_report_dates(start_date, end_date, 7):
            actual_ranges.append(date_range)

        self.assertEqual(expected_ranges, actual_ranges)

    def test_day_range(self):
        start_date = datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2022, 1, 12, 0, 0, 0, 0, tzinfo=timezone.utc)

        expected_ranges = [('2022-01-01', '2022-01-01'),
                           ('2022-01-02', '2022-01-02'),
                           ('2022-01-03', '2022-01-03'),
                           ('2022-01-04', '2022-01-04'),
                           ('2022-01-05', '2022-01-05'),
                           ('2022-01-06', '2022-01-06'),
                           ('2022-01-07', '2022-01-07'),
                           ('2022-01-08', '2022-01-08'),
                           ('2022-01-09', '2022-01-09'),
                           ('2022-01-10', '2022-01-10'),
                           ('2022-01-11', '2022-01-11'),
                           ('2022-01-12', '2022-01-12')]
        actual_ranges = []
        for date_range in generate_report_dates(start_date, end_date, 1):
            actual_ranges.append(date_range)

        self.assertEqual(expected_ranges, actual_ranges)

class TestStreamShuffling(unittest.TestCase):
    stream_ids = ["stream5", "stream4", "stream3", "stream2", "stream1"]
    def get_selected_streams(self):
        for id in self.stream_ids:
            yield CatalogEntry(tap_stream_id=id)

    def test_no_currently_syncing(self):
        state = {"currently_syncing": None, "bookmarks": {}}
        actual = sort_and_shuffle_streams(state["currently_syncing"], self.get_selected_streams())
        actual_streams = [stream.tap_stream_id for stream in actual]
        expected_streams = ["stream1", "stream2", "stream3", "stream4", "stream5"]
        self.assertEqual(expected_streams, actual_streams)

    def test_with_currently_syncing(self):
        state = {"currently_syncing": "stream2", "bookmarks": {}}
        actual = sort_and_shuffle_streams(state["currently_syncing"], self.get_selected_streams())
        actual_streams = [stream.tap_stream_id for stream in actual]
        expected_streams = ["stream2", "stream3", "stream4", "stream5", "stream1"]
        self.assertEqual(expected_streams, actual_streams)

    def test_currently_syncing_not_selected(self):
        state = {"currently_syncing": "stream2", "bookmarks": {}}
        self.stream_ids = ["stream5", "stream4", "stream3", "stream1"]
        actual = sort_and_shuffle_streams(state["currently_syncing"], self.get_selected_streams())
        actual_streams = [stream.tap_stream_id for stream in actual]
        expected_streams = ["stream1", "stream3", "stream4", "stream5"]
        self.assertEqual(expected_streams, actual_streams)

    def test_currently_syncing_at_start(self):
        state = {"currently_syncing": "stream1", "bookmarks": {}}
        actual = sort_and_shuffle_streams(state["currently_syncing"], self.get_selected_streams())
        actual_streams = [stream.tap_stream_id for stream in actual]
        expected_streams = ["stream1", "stream2", "stream3", "stream4", "stream5"]
        self.assertEqual(expected_streams, actual_streams)

    def test_currently_syncing_at_end(self):
        state = {"currently_syncing": "stream5", "bookmarks": {}}
        actual = sort_and_shuffle_streams(state["currently_syncing"], self.get_selected_streams())
        actual_streams = [stream.tap_stream_id for stream in actual]
        expected_streams = ["stream5", "stream1", "stream2", "stream3", "stream4"]
        self.assertEqual(expected_streams, actual_streams)
