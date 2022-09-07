import unittest
from datetime import timedelta

from singer import utils
import tap_ga4.sync
from tap_ga4.sync import generate_sdc_record_hash, get_report_start_date, CONVERSION_WINDOW


class TestRecordHashing(unittest.TestCase):
    """
    Canary test with a constant hash, if this value ever changes, it
    indicates that the primary key has been invalidated by changes.
    """
    def test_record_hash_canary(self):
        test_record = {"property_id": "123456789",
                       "start_date": "2022-09-05",
                       "end_date": "2022-09-05"}

        dimension_pairs = [('achievementId', 'hi'),
                           ('campaignId', '(not set)'),
                           ('date', '20220906'),
                           ('campaignName', '(my_campaign)'),
                           ('country', 'my_country'),
                           ('city', 'my_city'),
                           ('firstSessionDate', '20220906')]

        expected_hash = "a36ae7fa8d7da9ad5403e10375f4aced9a90ee8a0f9a7f7e747e59052c302af4"
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
    # TODO: mock utils.now, to clean up future-proofing
    def test_conversion_day_is_first_report_date(self):
        state = {"currently_syncing": None, "bookmarks": {"my_stream_id": {"123456789": {"last_report_date": "2022-09-07"}}}}
        # update bookmark to now, so the test works in the future
        state["bookmarks"]["my_stream_id"]["123456789"]["last_report_date"] = utils.now().strftime("%Y-%m-%d")
        expected_date = utils.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=CONVERSION_WINDOW)
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
        state = {"currently_syncing": None, "bookmarks": {"my_stream_id": {"123456789": {"last_report_date": "2022-03-03"}}}}
        # update bookmark to now, to future-proof test
        state["bookmarks"]["my_stream_id"]["123456789"]["last_report_date"] = utils.now().strftime("%Y-%m-%d")
        # update start_date to now - 3days to future-proof test
        self.config["start_date"] = (utils.now() - timedelta(days=3)).strftime("%Y-%m-%d")
        expected_date = utils.strptime_to_utc(self.config["start_date"])
        self.assertEqual(expected_date, get_report_start_date(self.config, self.property_id, state, "my_stream_id"))


if __name__ == '__main__':
    unittest.main()
