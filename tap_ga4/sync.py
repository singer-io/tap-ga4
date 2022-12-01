import hashlib
import json
from datetime import datetime, timedelta
import singer
from singer import Transformer, get_bookmark, metadata, utils
from google.analytics.data_v1beta.types import (Metric, Dimension)

from tap_ga4.discover import to_snake_case


LOGGER = singer.get_logger()

DEFAULT_CONVERSION_WINDOW = 90
DEFAULT_REQUEST_WINDOW_SIZE = 7


def sort_and_shuffle_streams(currently_syncing, selected_streams):
    """
    Order selected streams and shuffle if currently_syncing is set.
    """
    stream_list = list(selected_streams)
    sorted_selected_streams = sorted(stream_list, key=lambda x: x.tap_stream_id)

    if not currently_syncing:
        return sorted_selected_streams

    currently_syncing_idx = None
    for i, stream in enumerate(sorted_selected_streams):
        if currently_syncing == stream.tap_stream_id:
            currently_syncing_idx = i
            break

    if currently_syncing_idx:
        return sorted_selected_streams[currently_syncing_idx:] + sorted_selected_streams[:currently_syncing_idx]
    return sorted_selected_streams


def generate_sdc_record_hash(record, dimension_pairs):
    """
    Generates a SHA 256 hash to be used as the primary key for records
    associated with a report. This consists of a UTF-8 encoded JSON list
    containing:
    - The property_id of the associated report
    - The account_id of the associated report
    - Pairs of ("dimension_name", "dimension_value")

    Start and end date are included to maintain flexibility in the event the
    tap is extended to support wider date ranges.

    WARNING: Any change in the hashing mechanism, data, or sorting will
    REQUIRE a major version bump! As it will invalidate all previous
    primary keys and cause new data to be appended.
    """
    hash_source_data = sorted([("property_id", record["property_id"]),
                               ("account_id", record["account_id"]),
                               *dimension_pairs])
    hash_source_bytes = json.dumps(hash_source_data).encode('utf-8')
    return hashlib.sha256(hash_source_bytes).hexdigest()


def generate_report_dates(start_date, end_date, request_window_size):
    """
    Splits date range from start_date to end_date into chunks of request_window_size
    length and yields each chunk as a tuple (start_date, end_date).
    """
    range_start = start_date
    while range_start <= end_date:
        # NB: Subtract 1 from request_window_size because date range in RunReportRequest is inclusive
        range_end = range_start + timedelta(days=request_window_size - 1)
        yield (range_start.strftime("%Y-%m-%d"), min(end_date, range_end).strftime("%Y-%m-%d"))
        range_start = range_end + timedelta(days=1)


def transform_headers(dimension_headers, metric_headers):
    dim_headers = [to_snake_case(dimension) for dimension in dimension_headers]
    metric_headers = [to_snake_case(metric) for metric in metric_headers]
    return dim_headers, metric_headers


def row_to_record(report, row, dimension_headers, metric_headers):
    """
    Parse a RunReportResponse row into a single Singer record, with added
    runtime info and PK.
    """
    record = {}
    dimension_headers, metric_headers = transform_headers(dimension_headers, metric_headers)
    dimension_pairs = list(zip(dimension_headers, [dimension.value for dimension in row.dimension_values]))
    record.update(dimension_pairs)
    record.update(zip(metric_headers, [metric.value for metric in row.metric_values]))
    record["property_id"] = report["property_id"]
    record["account_id"] = report["account_id"]
    record["_sdc_record_hash"] = generate_sdc_record_hash(record, dimension_pairs)
    return record


DATETIME_FORMATS = {
    "date_hour": '%Y%m%d%H',
    "date_hour_minute": '%Y%m%d%H%M',
    "date": "%Y%m%d",
    "first_session_date": "%Y%m%d"
}


def parse_datetime(field_name, value, fmt=singer.utils.DATETIME_FMT):
    """
    Handle the case where the datetime value is not a valid datetime format.

    Google will return `(other)` as the value when the underlying database table
    from which the report is built reaches its row limit.

    See https://support.google.com/analytics/answer/9309767
    """
    is_valid_datetime = True
    try:
        parsed_datetime = datetime.strptime(value, DATETIME_FORMATS[field_name]).strftime(fmt)
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
        LOGGER.warning("Row limit reached for report: %s. See https://support.google.com/analytics/answer/9309767 for more info.", report_name)
    return rec


def get_report_start_date(config, property_id, state, tap_stream_id):
    """
    Returns the correct report start date.

    Cases:
    start_date: bookmark is empty.
                OR
                conversion_date is earlier than the start_date AND bookmark is later than start_date.

    bookmark: bookmark earlier than the conversion_date (this could happen if the tap was paused for awhile).

    conversion_date: the conversion_date is after the start_date AND earlier than the bookmark.
    """
    bookmark = get_bookmark(state,
                            tap_stream_id,
                            property_id,
                            default={}).get('last_report_date')
    start_date = utils.strptime_to_utc(config['start_date'])
    if not bookmark:
        return start_date
    else:
        bookmark = utils.strptime_to_utc(bookmark)
        conversion_window = int(config.get("conversion_window", DEFAULT_CONVERSION_WINDOW))
        conversion_day = utils.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=conversion_window)
    return min(bookmark, max(start_date, conversion_day))


def get_end_date(config):
    """
    Returns the end_date for the reporting sync. Under normal operation,
    this is defined as that date portion of UTC now.

    This can be overridden by the `end_date` config.json value.
    """
    if 'end_date' in config:
        return utils.strptime_to_utc(config['end_date'])
    return utils.now().replace(hour=0, minute=0, second=0, microsecond=0)


def sync_report(client, schema, report, start_date, end_date, request_window_size, state):
    """
    Run a sync, beginning from either the start_date, bookmarked date, or
    (now - CONVERSION_WINDOW) requesting a report per day.

    report = {"name": stream.tap_stream_id,
              "property_id": property_id,
              "account_id": account_id,
              "metrics": metrics,
              "dimensions": dimensions}
    """
    LOGGER.info("Syncing %s for property_id %s", report['name'], report['property_id'])

    for range_start_date, range_end_date in generate_report_dates(start_date, end_date, request_window_size):
        for response in client.get_report(report, range_start_date, range_end_date):
            dimension_headers = [dimension.name for dimension in response.dimension_headers]
            metric_headers = [metric.name for metric in response.metric_headers]
            with singer.metrics.record_counter(report['name']) as counter:
                with Transformer() as transformer:
                    for row in response.rows:
                        time_extracted = singer.utils.now()
                        rec = row_to_record(report, row, dimension_headers, metric_headers)
                        singer.write_record(report["name"],
                                            transformer.transform(
                                                transform_datetimes(report["name"], rec),
                                                schema),
                                            time_extracted=time_extracted)
                        counter.increment()
        singer.write_bookmark(state,
                              report["id"],
                              report["property_id"],
                              {"last_report_date": range_end_date})
        singer.write_state(state)
    LOGGER.info("Done syncing %s for property_id %s", report["name"], report["property_id"])


def sync(client, config, catalog, state):
    selected_streams = catalog.get_selected_streams(state)
    currently_syncing = state.get("currently_syncing", None)
    selected_streams = sort_and_shuffle_streams(currently_syncing, selected_streams)

    for stream in selected_streams:
        state = singer.set_currently_syncing(state, stream.tap_stream_id)
        singer.write_state(state)

        metrics = []
        dimensions = []
        mdata = metadata.to_map(stream.metadata)
        for field_path, field_mdata in mdata.items():
            if field_path == tuple():
                continue
            if field_mdata.get("inclusion") == "unsupported":
                continue
            if field_mdata.get("inclusion") == "automatic" or \
               field_mdata.get("selected") or \
               (field_mdata.get("selected-by-default") and field_mdata.get("selected") is None):
                if field_mdata.get("behavior") == "METRIC":
                    metrics.append(Metric(name=field_mdata.get("tap-ga4.api-field-names")))
                elif field_mdata.get("behavior") == "DIMENSION":
                    dimensions.append(Dimension(name=field_mdata.get("tap-ga4.api-field-names")))

        end_date = get_end_date(config)
        schema = stream.schema.to_dict()
        singer.write_schema(stream.stream,
                            schema,
                            stream.key_properties)

        report = {"property_id": config["property_id"],
                  "account_id": config["account_id"],
                  "name": stream.stream,
                  "id": stream.tap_stream_id,
                  "metrics": metrics,
                  "dimensions": dimensions}

        start_date = get_report_start_date(config, report["property_id"], state, report["id"])
        request_window_size = int(config.get("request_window_size", DEFAULT_REQUEST_WINDOW_SIZE))

        sync_report(client, schema, report, start_date, end_date, request_window_size, state)
        singer.write_state(state)
    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
