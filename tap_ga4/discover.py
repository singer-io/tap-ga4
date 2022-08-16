import singer
from singer import metadata, Schema, CatalogEntry, Catalog
from functools import reduce
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import GetMetadataRequest
import google.oauth2.credentials

LOGGER = singer.get_logger()

dimension_integer_field_overrides = {'cohortNthDay',
                                     'cohortNthMonth',
                                     'cohortNthWeek',
                                     'day',
                                     'dayOfWeek',
                                     'hour',
                                     'minute',
                                     'month',
                                     'nthDay',
                                     'nthHour',
                                     'nthMinute',
                                     'nthMonth',
                                     'nthWeek',
                                     'nthYear',
                                     'percentScrolled',
                                     'week',
                                     'year'}

dimension_datetime_field_overrides = {'date',
                                      'dateHour',
                                      'dateHourMinute',
                                      'firstSessionDate'}

FLOAT_TYPES = {'TYPE_FLOAT',
               'TYPE_SECONDS',
               'TYPE_MILLISECONDS',
               'TYPE_MINUTES',
               'TYPE_HOURS',
               'TYPE_STANDARD',
               'TYPE_CURRENCY',
               'TYPE_FEET',
               'TYPE_MILES',
               'TYPE_METERS',
               'TYPE_KILOMETERS'}


def add_metrics_to_schema(schema, metrics):
    for metric in metrics:
        metric_type = metric.type_.name
        if metric_type == 'TYPE_INTEGER':
            schema["properties"][metric.api_name] = {"type": ["integer", "null"]}
        elif metric_type in FLOAT_TYPES:
            schema["properties"][metric.api_name] = {"type": ["number", "null"]}
        else:
            raise Exception("Unknown Google Analytics type: {}".format(metric_type))


def add_dimension_to_schema(schema, dimensions):
    for dimension in dimensions:
        if dimension.api_name in dimension_integer_field_overrides:
            schema["properties"][dimension.api_name] = {"type": ["integer", "null"]}
        elif dimension.api_name in dimension_datetime_field_overrides:
            # datetime is not always a valid datetime string
            # https://support.google.com/analytics/answer/9309767
            schema["properties"][dimension.api_name] = \
                {"anyOf": [
                    {"type": ["string", "null"], "format": "date-time"},
                    {"type": ["string", "null"]}
                ]}
        else:
            schema["properties"][dimension.api_name] = {"type": ["string", "null"]}

def generate_base_schema():
    return {"type": "object", "properties": {"_sdc_record_hash": {"type": "string"},
                                             "start_date": {"type": "string",
                                                            "format": "date-time"},
                                             "end_date": {"type": "string",
                                                          "format": "date-time"},
                                             "property_id": {"type": "string"}}}

def generate_metadata(schema, dimensions, metrics):
    # TODO: add field exclusion metadata once google adds it
    mdata = metadata.get_standard_metadata(schema=schema, key_properties=["_sdc_record_hash"])
    mdata = metadata.to_map(mdata)
    mdata = reduce(lambda mdata, field_name: metadata.write(mdata, ("properties", field_name), "inclusion", "automatic"),
                   ["_sdc_record_hash", "start_date", "end_date", "property_id"],
                   mdata)
    mdata = reduce(lambda mdata, field_name: metadata.write(mdata, ("properties", field_name), "tap_ga4.group", "Report Fields"),
                   ["_sdc_record_hash", "start_date", "end_date", "property_id"],
                   mdata)
    for dimension in dimensions:
        mdata = metadata.write(mdata, ("properties", dimension.api_name), "tap_ga4.group", dimension.category)
    for metric in metrics:
        mdata = metadata.write(mdata, ("properties", metric.api_name), "tap_ga4.group", metric.category)
    return mdata



def generate_schema_and_metadata(dimensions, metrics):
    schema = generate_base_schema()
    add_dimension_to_schema(schema, dimensions)
    add_metrics_to_schema(schema, metrics)
    mdata = generate_metadata(schema, dimensions, metrics)
    return schema, mdata

def discover(client, config, property_id):
    request = GetMetadataRequest(
        name=f"properties/{property_id}/metadata",
    )
    response = client.get_metadata(request)
    generate_schema_and_metadata(response.dimensions, response.metrics)
    # TODO: generate catalog from custom reports once front end is implemented
