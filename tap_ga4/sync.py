import hashlib
import json
from datetime import datetime, timedelta

import singer
from singer import Transformer, get_bookmark, metadata, utils

from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest


LOGGER = singer.get_logger()

CONVERSION_WINDOW = 90
REPORT_LIMIT = 100000

def generate_sdc_record_hash(record, dimension_pairs):
    """
    Generates a SHA 256 hash to be used as the primary key for records
    associated with a report. This consists of a UTF-8 encoded JSON list
    containing:
    - The property_id of the associated report
    - Pairs of ("dimension_name", "dimension_value")
    - Report start_date value in YYYY-mm-dd format
    - Report end_date value in YYYY-mm-dd format

    Start and end date are included to maintain flexibility in the event the
    tap is extended to support wider date ranges.

    WARNING: Any change in the hashing mechanism, data, or sorting will
    REQUIRE a major version bump! As it will invalidate all previous
    primary keys and cause new data to be appended.
    """
    property_id = record["property_id"]
    sorted_dimension_pairs = sorted(dimension_pairs)

    # NB: Do not change the ordering of this list, it is the source of the PK hash
    hash_source_data = [property_id,
                        sorted_dimension_pairs,
                        record["start_date"],
                        record["end_date"]]

    hash_source_bytes = json.dumps(hash_source_data).encode('utf-8')
    return hashlib.sha256(hash_source_bytes).hexdigest()


def generate_report_dates(start_date, end_date):
    total_days = (end_date - start_date).days
    # NB: Add a day to be inclusive of both start and end
    for day_offset in range(total_days + 1):
        yield start_date + timedelta(days=day_offset)

def row_to_record(report, report_date, row, dimension_headers, metric_headers):
    """
    Parse a RunReportResponse row into a single Singer record, with added runtime info and PK.
    """
    # TODO: Handle data sampling keys and values, either in the records or as a separate stream? They look like arrays.
    # - https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#ReportData
    record = {}
    dimension_pairs = list(zip(dimension_headers, [dimension.value for dimension in row.dimension_values]))
    record.update(dimension_pairs)
    record.update(zip(metric_headers, [metric.value for metric in row.metric_values]))

    report_date_string = report_date.strftime("%Y-%m-%d")
    record["start_date"] = report_date_string
    record["end_date"] = report_date_string
    record["property_id"] = report["property_id"]
    record["_sdc_record_hash"] = generate_sdc_record_hash(record, dimension_pairs)
    return record


DATETIME_FORMATS = {
    "dateHour": '%Y%m%d%H',
    "dateHourMinute": '%Y%m%d%H%M',
    "date": "%Y%m%d",
    "firstSessionDate": "%Y%m%d"
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
        LOGGER.warning("Row limit reached for report: %. See https://support.google.com/analytics/answer/9309767 for more info.", report_name)
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
        conversion_day = utils.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=CONVERSION_WINDOW)
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


def get_report(client, property_id, report_date, dimensions, metrics):
    """
    Calls run_report and paginates over the request if the
    response.row_count is greater than 100,000.
    """
    offset = 0
    has_more_rows = True
    while has_more_rows:
        request = RunReportRequest(
            property=f'properties/{property_id}',
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[DateRange(start_date='2022-09-06', end_date='2022-09-06')],
            limit=REPORT_LIMIT,
            offset=offset,
            return_property_quota=True
        )

        response = client.run_report(request)
        has_more_rows = response.row_count > REPORT_LIMIT + offset
        offset += REPORT_LIMIT

        yield response


def sync_report(client, schema, report, start_date, end_date, state):
    """
    Run a sync, beginning from either the start_date, bookmarked date, or
    (now - CONVERSION_WINDOW) requesting a report per day.

    report = {"name": stream.tap_stream_id,
              "property_id": property_id,
              "metrics": metrics,
              "dimensions": dimensions}
    """
    LOGGER.info("Syncing %s for property_id %s", report['name'], report['property_id'])

    for report_date in generate_report_dates(start_date, end_date):
        for response in get_report(client, report["property_id"], report_date, report["dimensions"], report["metrics"]):
            dimension_headers = [dimension.name for dimension in response.dimension_headers]
            metric_headers = [metric.name for metric in response.metric_headers]
            with singer.metrics.record_counter(report['name']) as counter:
                with Transformer() as transformer:
                    for row in response.rows:
                        time_extracted = singer.utils.now()
                        rec = row_to_record(report, report_date, row, dimension_headers, metric_headers)
                        singer.write_record(report["name"],
                                            transformer.transform(
                                                transform_datetimes(report["name"], rec),
                                                schema),
                                            time_extracted=time_extracted)
                        counter.increment()
            singer.write_bookmark(state,
                                  report["id"],
                                  report['property_id'],
                                  {'last_report_date': report_date.strftime("%Y-%m-%d")})
            singer.write_state(state)
    LOGGER.info("Done syncing %s for property_id %s", report['name'], report['property_id'])


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
                    metrics.append(Metric(name=field_name))
                elif field_mdata.get('behavior') == 'DIMENSION':
                    dimensions.append(Dimension(name=field_name))

        end_date = get_end_date(config)
        schema = stream.schema.to_dict()
        singer.write_schema(stream.stream,
                            schema,
                            stream.key_properties)

        report = {"property_id": config["property_id"],
                  "name": stream.stream,
                  "id": stream.tap_stream_id,
                  "metrics": metrics,
                  "dimensions": dimensions}

        start_date = get_report_start_date(config, report['property_id'], state, report['id'])
        sync_report(client, schema, report, start_date, end_date, state)
        singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
