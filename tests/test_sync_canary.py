from tap_tester.base_suite_tests.sync_canary_test import SyncCanaryTest

from base import GA4Base


class GA4SyncCanaryTest(SyncCanaryTest, GA4Base):
    """Standard Sync Canary Test"""

    @staticmethod
    def name():
        return "tt_ga4_sync"

    def streams_to_test(self):
        # We have no test data for in_app_purchases stream
        return self.expected_stream_names().difference({"in_app_purchases"})

    @staticmethod
    def streams_to_selected_fields():
        return {
            "Test Report 1": {"total_users", },
            "Test Report 2": set(),
            'content_group_report': {"date", "total_users", },
            'conversions_report': {"date", "total_users", },
            'demographic_age_report': {"date", "total_users", },
            'demographic_city_report': {"date", "total_users", },
            'demographic_country_report': {"date", "total_users", },
            'demographic_gender_report': {"date", "total_users", },
            'demographic_interests_report': {"date", "total_users", },
            'demographic_language_report': {"date", "total_users", },
            'demographic_region_report': {"date", "total_users", },
            'ecommerce_purchases_item_brand_report': {"date", },
            'ecommerce_purchases_item_category_report': {"date", },
            'ecommerce_purchases_item_category_2_report': {"date", },
            'ecommerce_purchases_item_category_3_report': {"date", },
            'ecommerce_purchases_item_category_4_report': {"date", },
            'ecommerce_purchases_item_category_5_report': {"date", },
            'ecommerce_purchases_item_category_combined_report': {"date", "total_users", },
            'ecommerce_purchases_item_id_report': {"date", },
            'ecommerce_purchases_item_name_report': {"date", },
            'events_report': {"date", },
            'page_path_and_screen_class_report': {"date", "total_users", },
            'page_title_and_screen_class_report': {"date", "total_users", },
            'page_title_and_screen_name_report': {"date", "total_users", },
            'publisher_ads_ad_format_report': {"date", },
            'publisher_ads_ad_source_report': {"date", },
            'publisher_ads_ad_unit_report': {"date", },
            'publisher_ads_page_path_report': {"date", },
            'tech_app_version_report': {"date", "total_users", },
            'tech_browser_report': {"date", "total_users", },
            'tech_device_category_report': {"date", "total_users", },
            'tech_device_model_report': {"date", "total_users", },
            'tech_operating_system_report': {"date", "total_users", },
            'tech_os_version_report': {"date", "total_users", },
            'tech_os_with_version_report': {"date", "total_users", },
            'tech_platform_and_device_category_report': {"date", "total_users", },
            'tech_platform_report': {"date", "total_users", },
            'tech_screen_resolution_report': {"date", "total_users", },
            'traffic_acq_session_campaign_report': {"date", "total_users", },
            'traffic_acq_session_default_channel_group_report': {"date",
                                                                 "total_users", },
            'traffic_acq_session_medium_report': {"date", "total_users", },
            'traffic_acq_session_source_and_medium_report': {"date", "total_users", },
            'traffic_acq_session_source_platform_report': {"date", "total_users", },
            'traffic_acq_session_source_report': {"date", "total_users", },
            'user_acq_first_user_campaign_report': {"date", "total_users", },
            'user_acq_first_user_default_channel_group_report': {"date",
                                                                 "total_users", },
            'user_acq_first_user_google_ads_ad_group_name_report': {"date",
                                                                    "total_users", },
            'user_acq_first_user_google_ads_network_type_report': {"date",
                                                                   "total_users", },
            'user_acq_first_user_medium_report': {"date", "total_users", },
            'user_acq_first_user_source_and_medium_report': {"date", "total_users", },
            'user_acq_first_user_source_platform_report': {"date", "total_users", },
            'user_acq_first_user_source_report': {"date", "total_users", },
        }
