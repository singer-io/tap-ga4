import json
import singer
from singer import utils
from singer.catalog import Catalog
from tap_ga4.client import Client
from tap_ga4.discover import discover
from tap_ga4.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "oauth_client_id",
    "oauth_client_secret",
    "refresh_token",
    "property_id",
    "account_id",
]

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
    maybe_parse_report_definitions(config)

    client = Client(config)

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
