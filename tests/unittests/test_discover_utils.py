import unittest

from tap_ga4.discover import is_valid_alphanumeric_name, to_snake_case


class TestCanonicalization(unittest.TestCase):

    def test_to_snake_case(self):
        api_name = "audienceId"
        expected_name = "audience_id"
        self.assertEqual(expected_name, to_snake_case(api_name))

    def test_to_snake_case_custom_event(self):
        api_name = "customEvent:pageLocation"
        expected_name = "custom_event_page_location"
        self.assertEqual(expected_name, to_snake_case(api_name))

    def test_to_snake_case_custom_event_capitals(self):
        # Custom events must follow these rules:
        # https://support.google.com/analytics/answer/10085872?hl=en#event-name-rules
        api_name = "customEvent:PageLocation"
        expected_name = "custom_event_page_location"
        self.assertEqual(expected_name, to_snake_case(api_name))


class TestInvalidMetrics(unittest.TestCase):

    def test_valid_metric(self):
        api_name = "metric"
        self.assertTrue(is_valid_alphanumeric_name(api_name))

    def test_valid_metric_with_colon(self):
        api_name = "metric:event"
        self.assertTrue(is_valid_alphanumeric_name(api_name))

    def test_valid_metric_with_brackets(self):
        api_name = "customEvent:customMetric[event]"
        self.assertTrue(is_valid_alphanumeric_name(api_name))

    def test_invalid_metric(self):
        api_name = "metric:(other)"
        self.assertFalse(is_valid_alphanumeric_name(api_name))

    def test_invalid_metric_non_ascii(self):
        api_name = "ÜwÜ"
        self.assertFalse(is_valid_alphanumeric_name(api_name))
