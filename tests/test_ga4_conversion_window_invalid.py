"""Test tap configurable properties. Specifically the conversion_window"""
import os
import uuid
from datetime import datetime as dt
from datetime import timedelta

from tap_tester import connections, LOGGER

from base import GA4Base


class ConversionWindowInvalidTest(GA4Base):
    """
    Test tap's sync mode throws expected exception with invalid conversion_window values set.
    Validate setting the conversion_window configurable property.

    Test Cases:

    Verify tap throws critical error when a value is provided directly by a user which is
    outside the set of acceptable values.

      Acceptable values: { 30, 60, 90 } type = string
    """
    conversion_window = ''
    conversion_window_type = ''


    @classmethod
    def name(cls):
        return f"tt_ga4_conv_window_invalid_{cls.conversion_window_type}"

    def get_properties(self):
        # Use the same UUID for each custom report
        if not self.custom_report_id_1 and not self.custom_report_id_2:
            type(self).custom_report_id_1 = str(uuid.uuid4())
            type(self).custom_report_id_2 = str(uuid.uuid4())

        return_value = {
            'start_date': (dt.utcnow() - timedelta(days=3)).strftime(self.START_DATE_FORMAT),
            'conversion_window': self.conversion_window,
            'property_id': os.getenv('TAP_GA4_PROPERTY_ID'),
            'account_id': '659787',
            'oauth_client_id': os.getenv('TAP_GA4_CLIENT_ID'),
            'user_id': os.getenv('TAP_GA4_USER_ID'),
            'report_definitions': [
                {"id": self.custom_report_id_1, "name": "Test Report 1"},
                {"id": self.custom_report_id_2, "name": "Test Report 2"},
            ]
        }

        if self.request_window_size:
            return_value["request_window_size"] = self.request_window_size
        return return_value

    def streams_to_test(self):
        # testing all streams creates massive quota issues
        return {
            'content_group_report',
        }

    def run_test(self):
        """
        Testing that basic sync throws an exception  when an invalid conversion window is set.
        """

        with self.assertRaises(Exception) as context:
            err_msg_1 = "'message': 'properties do not match schema'"
            err_msg_2 = "'bad_properties': ['conversion_window']"

            # Create a connection
            connections.ensure_connection(self)

        # Verify connection cannot be made with invalid conversion_window
        LOGGER.info("********** Validating error message contains %s", err_msg_1)
        self.assertIn(err_msg_1, str(context.exception))
        LOGGER.info("********** Validating error message contains %s", err_msg_2)
        self.assertIn(err_msg_2, str(context.exception))


# BUG https://jira.talendforge.org/browse/TDL-21395
# class ConversionWindowTestZeroInteger(ConversionWindowInvalidTest):

#     # Fails (does not throw exception) with values 0, 1 as ints
#     # actually used conversion window to back up one day when set to 1 but sync'd 0 records
#     # actually used conversion window and made request for date range today to today
#     #     when set to 0 but sync'd 0 records

#     conversion_window = 0
#     conversion_window_type = 'int'

#     def test_run(self):
#         self.run_test()


class ConversionWindowTestZeroString(ConversionWindowInvalidTest):

    conversion_window = '0'
    conversion_window_type = 'string'

    def test_run(self):
        self.run_test()
