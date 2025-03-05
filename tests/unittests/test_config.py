import unittest
from tap_ga4 import maybe_parse_report_definitions


class TestMaybeParseReportDefinitions(unittest.TestCase):
    config_with_list = {'report_definitions':
                        [{'name': 'report1', 'id': 'id1'},
                         {'name': 'report2', 'id': 'id2'}],
                        'start_date': '2024-02-24T00:00:00Z',}

    config_with_string = {'report_definitions':
                          "[{\"name\":\"report1\",\"id\":\"id1\"},{\"name\":\"report2\",\"id\":\"id2\"}]",
                          'start_date': '2024-02-24T00:00:00Z',}

    config_without_report_definitions = {'start_date': '2024-02-24T00:00:00Z'}

    config_with_bad_type = {'report_definitions':
                            12345,
                            'start_date': '2024-02-24T00:00:00Z',}

    def test_with_list(self):
        """Test that config with report_definitions of type list remains unchanged"""
        self.assertIsInstance(self.config_with_list["report_definitions"], list)
        maybe_parse_report_definitions(self.config_with_list)
        self.assertIsInstance(self.config_with_list["report_definitions"], list)
        self.assertEqual(self.config_with_list["start_date"], '2024-02-24T00:00:00Z')

    def test_with_string(self):
        """Test that config with report_definitions of type string is converted to type list"""
        self.assertIsInstance(self.config_with_string["report_definitions"], str)
        maybe_parse_report_definitions(self.config_with_string)
        self.assertIsInstance(self.config_with_string["report_definitions"], list)
        self.assertEqual(self.config_with_string["start_date"], '2024-02-24T00:00:00Z')
        self.assertEqual(self.config_with_string, self.config_with_list)

    def test_without_report_definitions(self):
        """Test that config without report_definitions does not break"""
        assert "report_definitions" not in self.config_without_report_definitions
        maybe_parse_report_definitions(self.config_without_report_definitions)
        assert "report_definitions" not in self.config_without_report_definitions
        self.assertEqual(self.config_without_report_definitions["start_date"], '2024-02-24T00:00:00Z')

    def test_with_bad_type(self):
        """Test that config with report_definitions with unexpected type is unchanged by the function"""
        self.assertIsInstance(self.config_with_bad_type["report_definitions"], int)
        maybe_parse_report_definitions(self.config_with_bad_type)
        self.assertIsInstance(self.config_with_bad_type["report_definitions"], int)
        self.assertEqual(self.config_with_bad_type["start_date"], '2024-02-24T00:00:00Z')
