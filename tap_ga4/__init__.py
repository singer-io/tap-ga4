import json
import singer
from singer import utils
from singer.catalog import Catalog
from tap_ga4.client import Client
from tap_ga4.discover import discover
from tap_ga4.sync import sync

LOGGER = singer.get_logger()

# Keys that are always required regardless of authentication method
REQUIRED_CONFIG_KEYS = [
    "start_date",
    "property_id",
    "account_id",
]

# OAuth authentication keys (all required together)
OAUTH_CONFIG_KEYS = [
    "oauth_client_id",
    "oauth_client_secret",
    "refresh_token",
]

# Service account authentication keys (mutually exclusive)
SERVICE_ACCOUNT_CONFIG_KEYS = [
    "service_account_json",
    "service_account_json_path",
]


def validate_auth_config(config):
    """
    Validates that exactly one authentication method is provided and complete.

    Returns:
        str: The authentication type ('oauth' or 'service_account')

    Raises:
        ValueError: If authentication configuration is invalid
    """
    # Check which OAuth keys are present (non-empty)
    oauth_keys_present = [key for key in OAUTH_CONFIG_KEYS if config.get(key)]

    # Check which service account keys are present (non-empty)
    sa_keys_present = [key for key in SERVICE_ACCOUNT_CONFIG_KEYS if config.get(key)]

    has_oauth = len(oauth_keys_present) > 0
    has_service_account = len(sa_keys_present) > 0

    # Rule 1: Cannot mix authentication methods
    if has_oauth and has_service_account:
        raise ValueError(
            "Configuration error: Cannot mix OAuth and service account authentication. "
            f"Found OAuth keys: {oauth_keys_present}, Service account keys: {sa_keys_present}. "
            "Please use either OAuth (oauth_client_id, oauth_client_secret, refresh_token) "
            "OR service account (service_account_json or service_account_json_path), not both."
        )

    # Rule 2: Must have at least one complete authentication method
    if not has_oauth and not has_service_account:
        raise ValueError(
            "Configuration error: No authentication method provided. "
            "Please provide either OAuth credentials (oauth_client_id, oauth_client_secret, refresh_token) "
            "or service account credentials (service_account_json or service_account_json_path)."
        )

    # Validate OAuth completeness
    if has_oauth:
        missing_oauth_keys = [key for key in OAUTH_CONFIG_KEYS if not config.get(key)]
        if missing_oauth_keys:
            raise ValueError(
                f"Incomplete OAuth configuration. Missing required keys: {missing_oauth_keys}. "
                "OAuth authentication requires: oauth_client_id, oauth_client_secret, refresh_token."
            )
        return 'oauth'

    # Validate service account configuration
    if has_service_account:
        # Rule 3: Cannot have both service_account_json and service_account_json_path
        if len(sa_keys_present) > 1:
            raise ValueError(
                "Configuration error: Provide either 'service_account_json' OR "
                "'service_account_json_path', not both."
            )

        # Validate service_account_json is valid JSON if provided as string
        if 'service_account_json' in sa_keys_present:
            sa_json = config['service_account_json']
            if isinstance(sa_json, str):
                try:
                    parsed = json.loads(sa_json)
                    if not isinstance(parsed, dict):
                        raise ValueError("service_account_json must be a JSON object")
                except json.JSONDecodeError as e:
                    raise ValueError(f"service_account_json is not valid JSON: {e}") from e

        return 'service_account'


def maybe_parse_report_definitions(config):
    """Converts report_definitions into a list if it is a JSON-encoded string."""
    if isinstance(config.get("report_definitions", []), str):
        try:
            config.update(report_definitions = json.loads(config["report_definitions"]))
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing report_definitions string: {e}") from e

def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    catalog = args.catalog or Catalog([])
    state = {}

    config = args.config

    # Validate authentication configuration
    auth_type = validate_auth_config(config)
    LOGGER.info("Using %s authentication", auth_type)

    maybe_parse_report_definitions(config)

    client = Client(config, auth_type=auth_type)

    if args.state:
        state.update(args.state)
    if args.discover:
        discover(client, config.get("report_definitions", []), config["property_id"])
        LOGGER.info("Discovery complete")
    elif args.catalog:
        sync(client, config, catalog, state)
        LOGGER.info("Sync Completed")
    else:
        LOGGER.info("No properties were selected")


def main():
    try:
        main_impl()
    except Exception as e:
        for line in str(e).splitlines():
            LOGGER.critical(line)
        raise e

if __name__ == "__main__":
    main()
