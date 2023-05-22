"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import os
import uuid
from datetime import datetime as dt
from datetime import timedelta

from tap_tester.base_suite_tests.base_case import BaseCase


class GA4Base(BaseCase):
    """
    Setup expectations for test sub classes.
    Metadata describing streams.

    A bunch of shared methods that are used in tap-tester tests.
    Shared tap-specific methods (as needed).
    """

    HASHED_KEYS = "default-hashed-keys"
    # REPLICATION_KEY_FORMAT = "%Y-%m-%dT00:00:00.000000Z"
    CONVERSION_WINDOW = 10

    custom_report_id_1 = None
    custom_report_id_2 = None
    request_window_size = None

    # set the default start date which can be overridden in the tests.
    start_date = BaseCase.timedelta_formatted(dt.utcnow(), delta=timedelta(days=-3))

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-ga4"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.ga4"

    def get_properties(self):
        """Configuration properties required for the tap."""
        # Use the same UUID for each custom report
        if not GA4Base.custom_report_id_1 or not GA4Base.custom_report_id_2:
            GA4Base.custom_report_id_1 = str(uuid.uuid4())
            GA4Base.custom_report_id_2 = str(uuid.uuid4())

        return_value = {
            'start_date': self.start_date,
            'conversion_window': self.CONVERSION_WINDOW,
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

    @staticmethod
    def get_credentials():
        return {
            'oauth_client_secret': os.getenv('TAP_GA4_CLIENT_SECRET'),
            'access_token': os.getenv('TAP_GA4_ACCESS_TOKEN'),
            'refresh_token': os.getenv('TAP_GA4_REFRESH_TOKEN'),
        }

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams"""
        default_expectations = {
            GA4Base.HASHED_KEYS: {  # TODO also sorted dimensions and values...
                'account_id',
                'property_id',
            },
            BaseCase.PRIMARY_KEYS: {"_sdc_record_hash"},
            BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
            BaseCase.REPLICATION_KEYS: {"date"},
            BaseCase.RESPECTS_START_DATE: True,
            BaseCase.LOOK_BACK_WINDOW: timedelta(days=int(GA4Base.CONVERSION_WINDOW)),
            BaseCase.API_LIMIT: 100_000
        }

        return {
            'Test Report 1': default_expectations,  # TODO stitch QA generated, necessary?
            'Test Report 2': {       # TODO stitch QA generated, necessary?
                GA4Base.HASHED_KEYS: {  # TODO also sorted dimensions and values...
                    'account_id',
                    'property_id',
                },
                BaseCase.PRIMARY_KEYS: {"_sdc_record_hash"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date"},
                BaseCase.RESPECTS_START_DATE: False,
                BaseCase.LOOK_BACK_WINDOW: timedelta(days=int(GA4Base.CONVERSION_WINDOW)),
                BaseCase.API_LIMIT: 100_000
            },
            'content_group_report': default_expectations,
            'conversions_report': default_expectations,
            'demographic_age_report': default_expectations,
            'demographic_city_report': default_expectations,
            'demographic_country_report': default_expectations,
            'demographic_gender_report': default_expectations,
            'demographic_interests_report': default_expectations,
            'demographic_language_report': default_expectations,
            'demographic_region_report': default_expectations,
            'ecommerce_purchases_item_brand_report': default_expectations,
            'ecommerce_purchases_item_category_2_report': default_expectations,
            'ecommerce_purchases_item_category_3_report': default_expectations,
            'ecommerce_purchases_item_category_4_report': default_expectations,
            'ecommerce_purchases_item_category_5_report': default_expectations,
            'ecommerce_purchases_item_category_report': default_expectations,
            'ecommerce_purchases_item_id_report': default_expectations,
            'ecommerce_purchases_item_name_report': default_expectations,
            'events_report': default_expectations,
            'in_app_purchases': default_expectations,
            'page_path_and_screen_class_report': default_expectations,
            'page_title_and_screen_class_report': default_expectations,
            'page_title_and_screen_name_report': default_expectations,
            'publisher_ads_ad_format_report': default_expectations,
            'publisher_ads_ad_source_report': default_expectations,
            'publisher_ads_ad_unit_report': default_expectations,
            'publisher_ads_page_path_report': default_expectations,
            'tech_app_version_report': default_expectations,
            'tech_browser_report': default_expectations,
            'tech_device_category_report': default_expectations,
            'tech_device_model_report': default_expectations,
            'tech_operating_system_report': default_expectations,
            'tech_os_version_report': default_expectations,
            'tech_os_with_version_report': default_expectations,
            'tech_platform_and_device_category_report': default_expectations,
            'tech_platform_report': default_expectations,
            'tech_screen_resolution_report': default_expectations,
            'traffic_acq_session_campaign_report': default_expectations,
            'traffic_acq_session_default_channel_group_report': default_expectations,
            'traffic_acq_session_medium_report': default_expectations,
            'traffic_acq_session_source_and_medium_report': default_expectations,
            'traffic_acq_session_source_platform_report': default_expectations,
            'traffic_acq_session_source_report': default_expectations,
            'user_acq_first_user_campaign_report': default_expectations,
            'user_acq_first_user_default_channel_group_report': default_expectations,
            'user_acq_first_user_google_ads_ad_group_name_report': default_expectations,
            'user_acq_first_user_google_ads_network_type_report': default_expectations,
            'user_acq_first_user_medium_report': default_expectations,
            'user_acq_first_user_source_and_medium_report': default_expectations,
            'user_acq_first_user_source_platform_report': default_expectations,
            'user_acq_first_user_source_report': default_expectations,
        }

    def expected_hashed_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of hashed key fields used to form the primary key
        """
        return {table: properties.get(self.HASHED_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_automatic_fields(self, stream=None):
        auto_fields = {}
        for table, properties in self.expected_metadata().items():
            auto_fields[table] = properties.get(self.PRIMARY_KEYS, set()) \
                | properties.get(self.REPLICATION_KEYS, set()) \
                | properties.get(self.HASHED_KEYS, set())
        if stream:
            return auto_fields[stream]
        return auto_fields

    @classmethod
    def setUpClass(cls, logging="Ensuring environment variables are sourced."):
        super().setUpClass(logging=logging)
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
            raise ValueError(f"Missing environment variables: {missing_envs}")

    ##########################################################################
    # Tap Specific Methods
    ##########################################################################

    #     TODO setup a standard custom report to start iterating through tests
    #     TODO need to determine how to cover all metrics and dimensions via custom reports.
    #     TODO need to determine if there are any specific combinations that need to be covered
    #          (combinations that may later be used as pre-defiined reports?)

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
    def get_stream_name(cls, stream_id):
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
        return stream_mapping.get(stream_id, stream_id)
