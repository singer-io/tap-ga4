from datetime import timedelta, datetime
import hashlib
import json
import singer
from singer import get_bookmark, metadata, Transformer, utils
# from singer.catalog import Catalog

LOGGER = singer.get_logger()

def generate_sdc_record_hash(raw_report, row, start_date, end_date):
    """
    Generates a SHA 256 hash to be used as the primary key for records
    associated with a report. This consists of a UTF-8 encoded JSON list
    containing:
    - The account_id, web_property_id, profile_id of the associated report
    - Pairs of ("ga:dimension_name", "dimension_value")
    - Report start_date value in YYYY-mm-dd format
    - Report end_date value in YYYY-mm-dd format

    Start and end date are included to maintain flexibility in the event the
    tap is extended to support wider date ranges.

    WARNING: Any change in the hashing mechanism, data, or sorting will
    REQUIRE a major version bump! As it will invalidate all previous
    primary keys and cause new data to be appended.
    """
    dimensions_headers = raw_report["reports"][0]["columnHeader"].get("dimensions", [])
    profile_id = raw_report["profileId"]
    web_property_id = raw_report["webPropertyId"]
    account_id = raw_report["accountId"]

    dimensions_pairs = sorted(zip(dimensions_headers, row.get("dimensions", [])), key=lambda x: x[0])

    # NB: Do not change the ordering of this list, it is the source of the PK hash
    hash_source_data = [account_id,
                        web_property_id,
                        profile_id,
                        dimensions_pairs,
                        start_date.strftime("%Y-%m-%d"),
                        end_date.strftime("%Y-%m-%d")]

    hash_source_bytes = json.dumps(hash_source_data).encode('utf-8')
    return hashlib.sha256(hash_source_bytes).hexdigest()


def generate_report_dates(start_date, end_date):
    total_days = (end_date - start_date).days
    # NB: Add a day to be inclusive of both start and end
    for day_offset in range(total_days + 1):
        yield start_date + timedelta(days=day_offset)

def report_to_records(raw_report):
    """
    Parse a single report object into Singer records, with added runtime info and PK.

    NOTE: This function assumes one report being run for one date range
    per request. For optimizations, the structure of the response will
    change, and this will need to be refactored.
    """
    # TODO: Handle data sampling keys and values, either in the records or as a separate stream? They look like arrays.
    # - https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#ReportData
    report = raw_report["reports"][0]
    column_headers = report["columnHeader"]
    metrics_headers = [mh["name"] for mh in column_headers["metricHeader"]["metricHeaderEntries"]]
    dimensions_headers = column_headers.get("dimensions", [])

    for row in report.get("data", {}).get("rows", []):
        record = {}
        record.update(zip(dimensions_headers, row.get("dimensions", [])))
        record.update(zip(metrics_headers, row["metrics"][0]["values"]))

        report_date = raw_report["reportDate"]
        _sdc_record_hash = generate_sdc_record_hash(raw_report, row, report_date, report_date)
        record["_sdc_record_hash"] = _sdc_record_hash

        report_date_string = report_date.strftime("%Y-%m-%d")
        record["start_date"] = report_date_string
        record["end_date"] = report_date_string

        record["account_id"] = raw_report["accountId"]
        record["web_property_id"] = raw_report["webPropertyId"]
        record["profile_id"] = raw_report["profileId"]

        yield record

DATETIME_FORMATS = {
    "ga:dateHour": '%Y%m%d%H',
    "ga:dateHourMinute": '%Y%m%d%H%M',
    "ga:date": '%Y%m%d',
}

def parse_datetime(field_name, value):
    """
    Handle the case where the datetime value is not a valid datetime format.

    Google will return `(other)` as the value when the underlying database table
    from which the report is built reaches its row limit.

    See https://support.google.com/analytics/answer/9309767
    """
    is_valid_datetime = True
    try:
        parsed_datetime = datetime.strptime(value, DATETIME_FORMATS[field_name]).strftime(singer.utils.DATETIME_FMT)
        return parsed_datetime, is_valid_datetime
    except ValueError:
        is_valid_datetime = False
        return value, is_valid_datetime

def transform_datetimes(report_name, rec):
    """ Datetimes have a compressed format, so this ensures they parse correctly. """
    row_limit_reached = False
    for field_name, value in rec.items():
        if value and field_name in DATETIME_FORMATS:
            rec[field_name], is_valid_datetime = parse_datetime(field_name, value)
            row_limit_reached = row_limit_reached or (not is_valid_datetime and value == "(other)")
    if row_limit_reached:
        LOGGER.warning(f"Row limit reached for report: {report_name}. See https://support.google.com/analytics/answer/9309767 for more info.")
    return rec

def sync_report(client, schema, report, start_date, end_date, state, historically_syncing=False):
    """
    Run a sync, beginning from either the start_date or bookmarked date,
    requesting a report per day, until the last full day of data. (e.g.,
    "Yesterday")

    report = {"name": stream.tap_stream_id,
              "profile_id": view_id,
              "metrics": metrics,
              "dimensions": dimensions}
    """
    LOGGER.info("Syncing %s for view_id %s", report['name'], report['profile_id'])

    all_data_golden = True
    # TODO: Is it better to query by multiple days if `ga:date` is present?
    # - If so, we can optimize the calls here to generate date ranges and reduce request volume
    for report_date in generate_report_dates(start_date, end_date):
        for raw_report_response in client.get_report(report['name'], report['profile_id'],
                                                     report_date, report['metrics'],
                                                     report['dimensions']):

            with singer.metrics.record_counter(report['name']) as counter:
                time_extracted = singer.utils.now()
                with Transformer() as transformer:
                    for rec in report_to_records(raw_report_response):
                        singer.write_record(report["name"],
                                            transformer.transform(
                                                transform_datetimes(report["name"], rec),
                                                schema),
                                            time_extracted=time_extracted)
                        counter.increment()

                # NB: Bookmark all days with "golden" data until you find the first non-golden day
                # - "golden" refers to data that will not change in future
                #   requests, so we can use it as a bookmark
                is_data_golden = raw_report_response["reports"][0]["data"].get("isDataGolden")
                if historically_syncing:
                    # Switch to regular bookmarking at first golden
                    historically_syncing = not is_data_golden

                # The assumption here is that today's data cannot be golden if yesterday's is also not golden
                if all_data_golden and not historically_syncing:
                    singer.write_bookmark(state,
                                          report["id"],
                                          report['profile_id'],
                                          {'last_report_date': report_date.strftime("%Y-%m-%d")})
                    singer.write_state(state)
                    if not is_data_golden and not historically_syncing:
                        # Stop bookmarking on first "isDataGolden": False
                        all_data_golden = False
                else:
                    LOGGER.info("Did not detect that data was golden. Skipping writing bookmark.")
    LOGGER.info("Done syncing %s for view_id %s", report['name'], report['profile_id'])


def get_start_date(config, property_id, state, tap_stream_id):
    """
    Returns a date bookmark in state for the given stream, or the
    `start_date` from config, if no bookmark exists.
    """
    start = get_bookmark(state,
                         tap_stream_id,
                         property_id,
                         default={}).get('last_report_date',
                                         config['start_date'])
    is_historical_sync = start == config['start_date']
    return is_historical_sync, utils.strptime_to_utc(start)


def get_end_date(config):
    """
    Returns the end_date for the reporting sync. Under normal operation,
    this is defined as that date portion of UTC now.

    This can be overridden by the `end_date` config.json value.
    """
    if 'end_date' in config:
        return utils.strptime_to_utc(config['end_date'])
    return utils.now().replace(hour=0, minute=0, second=0, microsecond=0)


def generate_report_dates(start_date, end_date):
    total_days = (end_date - start_date).days
    # NB: Add a day to be inclusive of both start and end
    for day_offset in range(total_days + 1):
        yield start_date + timedelta(days=day_offset)


def sync_report(client, schema, report, start_date, end_date, state, is_historical_sync):
    for report_date in generate_report_dates(start_date, end_date):
        pass


# bookmark format
# singer.write_bookmark(state, report["id"], report["property_id"], {"last_report_date": utils.now()})
def sync(client, config, catalog, state):
    selected_streams = catalog.get_selected_streams(state)
    #TODO add start with currently syncing
    for stream in selected_streams:
        state = singer.set_currently_syncing(state, stream.tap_stream_id)
        singer.write_state(state)

        metrics = []
        dimensions = []
        mdata = metadata.to_map(stream.metadata)
        for field_path, field_mdata in mdata.items():
            if field_path == tuple():
                continue
            if field_mdata.get('inclusion') == 'unsupported':
                continue
            _, field_name = field_path
            if field_mdata.get('inclusion') == 'automatic' or \
               field_mdata.get('selected') or \
               (field_mdata.get('selected-by-default') and field_mdata.get('selected') is None):
                if field_mdata.get('behavior') == 'METRIC':
                    metrics.append(field_name)
                elif field_mdata.get('behavior') == 'DIMENSION':
                    dimensions.append(field_name)

        end_date = get_end_date(config)
        schema = stream.schema.to_dict()
        singer.write_schema(
            stream.stream,
            schema,
            stream.key_properties
            )

        report = {"property_id": config["property_id"],
                  "name": stream.stream,
                  "id": stream.tap_stream_id,
                  "metrics": metrics,
                  "dimensions": dimensions}

        is_historical_sync, start_date = get_start_date(config, report['property_id'], state, report['id'])
        sync_report(client, schema, report, start_date, end_date, state, is_historical_sync)
        singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
