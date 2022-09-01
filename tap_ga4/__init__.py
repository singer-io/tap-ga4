import logging
import singer
from singer import utils
from tap_ga4.discover import discover
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.oauth2.credentials import Credentials


LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "oauth_client_id",
    "oauth_client_secret",
    "refresh_token",
    "property_id",
    "report_definitions",
]


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    state = {}

    # access_token (1st param) can be None when refresh_token is supplied
    credentials = Credentials(None,
                              refresh_token=config["refresh_token"],
                              token_uri='https://www.googleapis.com/oauth2/v4/token',
                              client_id=config["oauth_client_id"],
                              client_secret=config["oauth_client_secret"])

    client = BetaAnalyticsDataClient(credentials=credentials)

    if args.state:
        state.update(args.state)
    if args.discover:
        discover(client, config["report_definitions"], config["property_id"])
        LOGGER.info("Discovery complete")
    elif args.catalog:
        # TODO: write sync
        LOGGER.info("Sync Completed")
    else:
        LOGGER.info("No properties were selected")


def main():
    main_impl()


if __name__ == "__main__":
    main()
