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
    "report_definitions",
]


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    catalog = args.catalog or Catalog([])
    state = {}

    client = Client(config)

    if args.state:
        state.update(args.state)
    if args.discover:
        discover(client, config["report_definitions"], config["property_id"])
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
