"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import os
import uuid
from datetime import datetime as dt
from datetime import timedelta

from tap_tester import connections, menagerie, runner, LOGGER
from tap_tester.base_suite_tests.base_case import BaseCase


class GA4Base(BaseCase):
    """
    Setup expectations for test sub classes.
    Metadata describing streams.

    A bunch of shared methods that are used in tap-tester tests.
    Shared tap-specific methods (as needed).
    """
    HASHED_KEYS = "default-hashed-keys"
    REPLICATION_KEY_FORMAT = "%Y-%m-%dT00:00:00.000000Z"

    start_date = ""

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-ga4"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.ga4"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date' : (dt.utcnow() - timedelta(days=3)).strftime(self.START_DATE_FORMAT),
            'property_id': os.getenv('TAP_GA4_PROPERTY_ID'),
            'oauth_client_id': os.getenv('TAP_GA4_CLIENT_ID'),
            'user_id': os.getenv('TAP_GA4_USER_ID'), # TODO what is?  should orca handle this?
            'report_definitions': [
                {"id": str(uuid.uuid4()), "name": "Test Report 1"},
                {"id": str(uuid.uuid4()), "name": "Test Report 2"},
            ]
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    @staticmethod
    def get_credentials():
        return {
            'oauth_client_secret': os.getenv('TAP_GA4_CLIENT_SECRET'),
            'access_token': os.getenv('TAP_GA4_ACCESS_TOKEN'),
            'refresh_token': os.getenv('TAP_GA4_REFRESH_TOKEN'),
        }

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""
        default_expectations = {
            self.PRIMARY_KEYS: {"_sdc_record_hash"},
            self.REPLICATION_METHOD: self.INCREMENTAL,
            self.REPLICATION_KEYS: {"start_date"}, # TODO may be 'date'
            self.HASHED_KEYS: { # TODO also sorted dimensions and values...
                'property_id',
                'end_date',
                'end_date',
            },
        }

        return {
            "Test Report 1": default_expectations,
            "Test Report 2": default_expectations,
            # TODO what predefined are we supporting?
        }

    def expected_hashed_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of hashed key fields used to form the primary key
        """
        return {table: properties.get(self.HASHED_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_automatic_fields(self):
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set()) \
                | v.get(self.HASHED_KEYS, set())

        return auto_fields

    @classmethod
    def setUpClass(cls):
        super().setUpClass(logging="Ensuring environment variables are sourced.")
        missing_envs = [
            x for x in [
                'TAP_GA4_PROPERTY_ID',
                'TAP_GA4_CLIENT_SECRET',
                'TAP_GA4_CLIENT_ID',
                'TAP_GA4_ACCESS_TOKEN',
                'TAP_GA4_REFRESH_TOKEN',
                'TAP_GA4_USER_ID',
            ] if os.getenv(x) is None
        ]

        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    ##########################################################################
    ### Tap Specific Methods
    ##########################################################################

    @staticmethod
    def expected_default_fields():
        """

        TODO setup a standard custom report to start iterating through tests
        TODO need to determine how to cover all metrics and dimensions via custom reports.
        TODO need to determine if there are any specific combinations that need to be covered
             (combinations that may later be used as pre-defiined reports?)

        GA4 NOTES:
          Segment are based on dimensions and metrics
           - Users: People interact with your property (e.g., your website or app)
           - Sessions: Interactions by a single user are grouped into sessions.
           - Hits: Interactions during a session are referred to as hits. Hits include interactions like pageviews, events, and transactions.

          Dimensions are data attributes like City, Browser, PAGE, etc.

          Metrics are quantitative measures like Clicks, Sessions, Pages per Session, etc.



        NOTE: See method in tap-google-analytics/tests/base.py
        """
        return {
            "Test Report 1": {'TODO',},
            "Test Report 2": {'TODO',},
        }


    @staticmethod
    def expected_pagination_fields(): # TODO does this apply?
        return {
            "Test Report 1" : set(),
            "Audience Overview": {
                "ga:users", "ga:newUsers", "ga:sessions", "ga:sessionsPerUser", "ga:pageviews",
                "ga:pageviewsPerSession", "ga:sessionDuration", "ga:bounceRate", "ga:date",
                # "ga:pageviews",
            },
            "Audience Geo Location": set(),
            "Audience Technology": set(),
            "Acquisition Overview": set(),
            "Behavior Overview": set(),
            "Ecommerce Overview": set(),
        }

    def custom_reports_names_to_ids(self): # TODO does this apply?
        report_definitions =self.get_properties()['report_definitions']
        name_to_id_map = {
            definition.get('name'): definition.get('id')
            for definition in report_definitions
        }

        return name_to_id_map

    # TODO this will apply but not yet. And the standard list will likely be different
    # @staticmethod
    # def is_custom_report(stream):
    #     standard_reports = {
    #         "Audience Overview",
    #         "Audience Geo Location",
    #         "Audience Technology",
    #         "Acquisition Overview",
    #         "Behavior Overview",
    #         "Ecommerce Overview",
    #     }
    #     return stream not in standard_reports

    @staticmethod
    def custom_report_minimum_valid_field_selection():
        """
        The uncommented dimensions and metrics are sufficient for the current test suite.
        In the future consider mixing up the selection to increase test covereage.
        See TODO header at top of file.
        """
        return {
            'Test Report 1': {
                #"ga:sessions",  # Metric
                "ga:avgSessionDuration",  # Metric
                "ga:bounceRate",  # Metric
                "ga:users",  # Metric
                # "ga:pagesPerSession",  # Metric
                "ga:avgTimeOnPage",  # Metric
                "ga:bounces",  # Metric
                "ga:hits",  # Metric
                "ga:sessionDuration",  # Metric
                "ga:newUsers",  # Metric
                "ga:deviceCategory",  # Dimension
                # "ga:eventAction",  # Dimension
                "ga:date",  # Dimension
                # "ga:eventLabel",  # Dimension
                # "ga:eventCategory"  # Dimension
            },
        }
