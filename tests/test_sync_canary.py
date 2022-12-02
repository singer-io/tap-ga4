import unittest

from tap_tester.base_suite_tests.sync_canary_test import SyncCanaryTest

from base import GA4Base


class GA4SyncCanaryTest(SyncCanaryTest, GA4Base):
    """Standard Sync Canary Test"""

    @staticmethod
    def name():
        return "tt_ga4_sync"

    def streams_to_test(self):
        streams_to_test = set(self.expected_metadata().keys())
        # We have no test data for in_app_purchases stream
        streams_to_test.remove("in_app_purchases")
        return streams_to_test


    def streams_to_selected_fields(self):
        return {
            "Test Report 1": {  # engagement events
                "conversions",
                "defaultChannelGrouping",
                "eventName",
                "eventCount",
                "newUsers",
                "enagementRate",
                "engagedSessions",
            },
            "Test Report 2": {
                "pageTitle",
                "newUsers",
            },
            'content_group_report': { "date", "browser", "conversions", },
            # TODO clean up fields and formatting
            'conversions_report': { "date", "browser", "conversions", },
            'demographic_age_report': { "date", "browser", "conversions", },
            'demographic_city_report': { "date", "browser", "conversions", },
            'demographic_country_report': { "date", "browser", "conversions", },
            'demographic_gender_report': { "date", "browser", "conversions", },
            'demographic_interests_report': { "date", "browser", "conversions", },
            'demographic_language_report': { "date", "browser", "conversions", },
            'demographic_region_report': { "date", "browser", "conversions", },
            'ecommerce_purchases_item_brand_report': { "date", "browser", "conversions", },
            'ecommerce_purchases_item_category_report': { "date", "browser", "conversions", },
            'ecommerce_purchases_item_category_2_report': { "date", "browser", "conversions", },
            'ecommerce_purchases_item_category_3_report': { "date", "browser", "conversions", },
            'ecommerce_purchases_item_category_4_report': { "date", "browser", "conversions", },
            'ecommerce_purchases_item_category_5_report': { "date", "browser", "conversions", },
            'ecommerce_purchases_item_category_combined_report': { "date", "browser", "conversions", },
            'ecommerce_purchases_item_id_report': { "date", "browser", "conversions", },
            'ecommerce_purchases_item_name_report': { "date", "browser", "conversions", },
            'events_report': { "date", "browser", "conversions", },
            'page_path_and_screen_class_report': { "date", "browser", "conversions", },
            'page_title_and_screen_class_report': { "date", "browser", "conversions", },
            'page_title_and_screen_name_report': { "date", "browser", "conversions", },
            'publisher_ads_ad_format_report': { "date", "browser", "conversions", },
            'publisher_ads_ad_source_report': { "date", "browser", "conversions", },
            'publisher_ads_ad_unit_report': { "date", "browser", "conversions", },
            'publisher_ads_page_path_report': { "date", "browser", "conversions", },
            'tech_app_version_report': { "date", "browser", "conversions", },
            'tech_browser_report': { "date", "browser", "conversions", },
            'tech_device_category_report': { "date", "browser", "conversions", },
            'tech_device_model_report': { "date", "browser", "conversions", },
            'tech_operating_system_report': { "date", "browser", "conversions", },
            'tech_os_version_report': { "date", "browser", "conversions", },
            'tech_os_with_version_report': { "date", "browser", "conversions", },
            'tech_platform_and_device_category_report': { "date", "browser", "conversions", },
            'tech_platform_report': { "date", "browser", "conversions", },
            'tech_screen_resolution_report': { "date", "browser", "conversions", },
            'traffic_acq_session_campaign_report': { "date", "browser", "conversions", },
            'traffic_acq_session_default_channel_group_report': { "date", "browser", "conversions", },
            'traffic_acq_session_medium_report': { "date", "browser", "conversions", },
            'traffic_acq_session_source_and_medium_report': { "date", "browser", "conversions", },
            'traffic_acq_session_source_platform_report': { "date", "browser", "conversions", },
            'traffic_acq_session_source_report': { "date", "browser", "conversions", },
            'user_acq_first_user_campaign_report': { "date", "browser", "conversions", },
            'user_acq_first_user_default_channel_group_report': { "date", "browser", "conversions", },
            'user_acq_first_user_google_ads_ad_group_name_report': { "date", "browser", "conversions", },
            'user_acq_first_user_google_ads_network_type_report': { "date", "browser", "conversions", },
            'user_acq_first_user_medium_report': { "date", "browser", "conversions", },
            'user_acq_first_user_source_and_medium_report': { "date", "browser", "conversions", },
            'user_acq_first_user_source_platform_report': { "date", "browser", "conversions", },
            'user_acq_first_user_source_report': { "date", "browser", "conversions", },
        }
