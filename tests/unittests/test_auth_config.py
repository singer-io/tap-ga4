import unittest
from tap_ga4 import validate_auth_config


class TestValidateAuthConfig(unittest.TestCase):
    """Test authentication configuration validation."""

    # Valid configurations
    def test_valid_oauth_config(self):
        """Test that valid OAuth config passes validation."""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'property_id': '123',
            'account_id': '456',
            'oauth_client_id': 'client-id',
            'oauth_client_secret': 'client-secret',
            'refresh_token': 'refresh-token',
        }
        auth_type = validate_auth_config(config)
        self.assertEqual(auth_type, 'oauth')

    def test_valid_service_account_json_config(self):
        """Test that valid service account JSON config passes validation."""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'property_id': '123',
            'account_id': '456',
            'service_account_json': '{"type": "service_account", "project_id": "test"}',
        }
        auth_type = validate_auth_config(config)
        self.assertEqual(auth_type, 'service_account')

    def test_valid_service_account_json_as_dict(self):
        """Test that service account JSON as dict passes validation."""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'property_id': '123',
            'account_id': '456',
            'service_account_json': {'type': 'service_account', 'project_id': 'test'},
        }
        auth_type = validate_auth_config(config)
        self.assertEqual(auth_type, 'service_account')

    def test_valid_service_account_path_config(self):
        """Test that valid service account path config passes validation."""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'property_id': '123',
            'account_id': '456',
            'service_account_json_path': '/path/to/sa.json',
        }
        auth_type = validate_auth_config(config)
        self.assertEqual(auth_type, 'service_account')

    # Invalid configurations
    def test_no_auth_config_raises(self):
        """Test that missing auth config raises ValueError."""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'property_id': '123',
            'account_id': '456',
        }
        with self.assertRaises(ValueError) as ctx:
            validate_auth_config(config)
        self.assertIn('No authentication method provided', str(ctx.exception))

    def test_mixed_auth_config_raises(self):
        """Test that mixing OAuth and service account raises ValueError."""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'property_id': '123',
            'account_id': '456',
            'oauth_client_id': 'client-id',
            'service_account_json': '{"type": "service_account"}',
        }
        with self.assertRaises(ValueError) as ctx:
            validate_auth_config(config)
        self.assertIn('Cannot mix OAuth and service account', str(ctx.exception))

    def test_incomplete_oauth_config_raises(self):
        """Test that incomplete OAuth config raises ValueError."""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'property_id': '123',
            'account_id': '456',
            'oauth_client_id': 'client-id',
            # Missing oauth_client_secret and refresh_token
        }
        with self.assertRaises(ValueError) as ctx:
            validate_auth_config(config)
        self.assertIn('Incomplete OAuth configuration', str(ctx.exception))
        self.assertIn('oauth_client_secret', str(ctx.exception))
        self.assertIn('refresh_token', str(ctx.exception))

    def test_incomplete_oauth_missing_one_key_raises(self):
        """Test that OAuth config missing one key raises ValueError."""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'property_id': '123',
            'account_id': '456',
            'oauth_client_id': 'client-id',
            'oauth_client_secret': 'client-secret',
            # Missing refresh_token
        }
        with self.assertRaises(ValueError) as ctx:
            validate_auth_config(config)
        self.assertIn('Incomplete OAuth configuration', str(ctx.exception))
        self.assertIn('refresh_token', str(ctx.exception))

    def test_both_service_account_options_raises(self):
        """Test that providing both SA options raises ValueError."""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'property_id': '123',
            'account_id': '456',
            'service_account_json': '{"type": "service_account"}',
            'service_account_json_path': '/path/to/sa.json',
        }
        with self.assertRaises(ValueError) as ctx:
            validate_auth_config(config)
        self.assertIn("'service_account_json' OR 'service_account_json_path'",
                      str(ctx.exception))

    def test_invalid_service_account_json_raises(self):
        """Test that invalid JSON in service_account_json raises ValueError."""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'property_id': '123',
            'account_id': '456',
            'service_account_json': 'not valid json',
        }
        with self.assertRaises(ValueError) as ctx:
            validate_auth_config(config)
        self.assertIn('not valid JSON', str(ctx.exception))

    def test_service_account_json_not_object_raises(self):
        """Test that non-object JSON raises ValueError."""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'property_id': '123',
            'account_id': '456',
            'service_account_json': '["not", "an", "object"]',
        }
        with self.assertRaises(ValueError) as ctx:
            validate_auth_config(config)
        self.assertIn('must be a JSON object', str(ctx.exception))

    def test_empty_string_values_treated_as_missing(self):
        """Test that empty string values are treated as missing."""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'property_id': '123',
            'account_id': '456',
            'oauth_client_id': '',
            'oauth_client_secret': '',
            'refresh_token': '',
        }
        with self.assertRaises(ValueError) as ctx:
            validate_auth_config(config)
        self.assertIn('No authentication method provided', str(ctx.exception))

    def test_none_values_treated_as_missing(self):
        """Test that None values are treated as missing."""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'property_id': '123',
            'account_id': '456',
            'oauth_client_id': None,
            'oauth_client_secret': None,
            'refresh_token': None,
        }
        with self.assertRaises(ValueError) as ctx:
            validate_auth_config(config)
        self.assertIn('No authentication method provided', str(ctx.exception))

    def test_partial_empty_oauth_raises(self):
        """Test OAuth with some empty values raises incomplete error."""
        config = {
            'start_date': '2024-01-01T00:00:00Z',
            'property_id': '123',
            'account_id': '456',
            'oauth_client_id': 'client-id',
            'oauth_client_secret': '',
            'refresh_token': 'token',
        }
        with self.assertRaises(ValueError) as ctx:
            validate_auth_config(config)
        self.assertIn('Incomplete OAuth configuration', str(ctx.exception))
