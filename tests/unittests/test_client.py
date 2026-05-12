import unittest
from unittest.mock import patch, MagicMock
import json
from tap_ga4.client import Client


class TestClientAuthentication(unittest.TestCase):
    """Test Client initialization with different auth types."""

    @patch('tap_ga4.client.BetaAnalyticsDataClient')
    @patch('tap_ga4.client.Credentials')
    def test_oauth_client_creation(self, mock_credentials, mock_client_class):
        """Test that OAuth credentials are used correctly."""
        config = {
            'oauth_client_id': 'client-id',
            'oauth_client_secret': 'client-secret',
            'refresh_token': 'refresh-token',
        }

        mock_credentials_instance = MagicMock()
        mock_credentials.return_value = mock_credentials_instance

        client = Client(config, auth_type='oauth')

        mock_credentials.assert_called_once_with(
            None,
            refresh_token='refresh-token',
            token_uri='https://www.googleapis.com/oauth2/v4/token',
            client_id='client-id',
            client_secret='client-secret'
        )
        mock_client_class.assert_called_once_with(credentials=mock_credentials_instance)

    @patch('tap_ga4.client.BetaAnalyticsDataClient')
    def test_service_account_json_string_client_creation(self, mock_client_class):
        """Test that service account JSON string is used correctly."""
        sa_info = {'type': 'service_account', 'project_id': 'test-project'}
        config = {
            'service_account_json': json.dumps(sa_info),
        }

        Client(config, auth_type='service_account')

        mock_client_class.from_service_account_info.assert_called_once_with(sa_info)

    @patch('tap_ga4.client.BetaAnalyticsDataClient')
    def test_service_account_json_dict_client_creation(self, mock_client_class):
        """Test that service account JSON as dict is used correctly."""
        sa_info = {'type': 'service_account', 'project_id': 'test-project'}
        config = {
            'service_account_json': sa_info,
        }

        Client(config, auth_type='service_account')

        mock_client_class.from_service_account_info.assert_called_once_with(sa_info)

    @patch('tap_ga4.client.BetaAnalyticsDataClient')
    @patch('os.path.exists', return_value=True)
    def test_service_account_path_client_creation(self, mock_exists, mock_client_class):
        """Test that service account file path is used correctly."""
        config = {
            'service_account_json_path': '/path/to/sa.json',
        }

        Client(config, auth_type='service_account')

        mock_exists.assert_called_once_with('/path/to/sa.json')
        mock_client_class.from_service_account_file.assert_called_once_with(
            '/path/to/sa.json'
        )

    @patch('os.path.exists', return_value=False)
    def test_service_account_path_not_found_raises(self, mock_exists):
        """Test that missing service account file raises FileNotFoundError."""
        config = {
            'service_account_json_path': '/nonexistent/path.json',
        }

        with self.assertRaises(FileNotFoundError) as ctx:
            Client(config, auth_type='service_account')
        self.assertIn('/nonexistent/path.json', str(ctx.exception))

    def test_invalid_auth_type_raises(self):
        """Test that invalid auth_type raises ValueError."""
        config = {}
        with self.assertRaises(ValueError) as ctx:
            Client(config, auth_type='invalid')
        self.assertIn('Unknown auth_type', str(ctx.exception))

    def test_service_account_missing_credentials_raises(self):
        """Test that missing service account credentials raises ValueError."""
        config = {}
        with self.assertRaises(ValueError) as ctx:
            Client(config, auth_type='service_account')
        self.assertIn("'service_account_json' or 'service_account_json_path'",
                      str(ctx.exception))

    @patch('tap_ga4.client.BetaAnalyticsDataClient')
    @patch('tap_ga4.client.Credentials')
    def test_default_auth_type_is_oauth(self, mock_credentials, mock_client_class):
        """Test that default auth_type is oauth for backward compatibility."""
        config = {
            'oauth_client_id': 'client-id',
            'oauth_client_secret': 'client-secret',
            'refresh_token': 'refresh-token',
        }

        # Call without auth_type argument
        Client(config)

        # Should use OAuth credentials
        mock_credentials.assert_called_once()
