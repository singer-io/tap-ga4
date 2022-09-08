from collections import defaultdict
from functools import reduce
import singer
from singer import metadata, Schema, CatalogEntry, Catalog
from singer.catalog import write_catalog
from google.analytics.data_v1beta.types import CheckCompatibilityRequest, GetMetadataRequest, Dimension, Metric


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
            raise Exception(f"Unknown Google Analytics 4 type: {metric_type}")


def add_dimensions_to_schema(schema, dimensions):
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


def generate_metadata(schema, dimensions, metrics, field_exclusions):
    mdata = metadata.get_standard_metadata(schema=schema, key_properties=["_sdc_record_hash"])
    mdata = metadata.to_map(mdata)
    mdata = reduce(lambda mdata, field_name: metadata.write(mdata, ("properties", field_name), "inclusion", "automatic"),
                   ["_sdc_record_hash", "start_date", "end_date", "property_id"],
                   mdata)
    mdata = reduce(lambda mdata, field_name: metadata.write(mdata, ("properties", field_name), "tap_ga4.group", "Report Field"),
                   ["_sdc_record_hash", "start_date", "end_date", "property_id"],
                   mdata)
    for dimension in dimensions:
        mdata = metadata.write(mdata, ("properties", dimension.api_name), "tap_ga4.group", dimension.category)
        mdata = metadata.write(mdata, ("properties", dimension.api_name), "behavior", "DIMENSION")
        mdata = metadata.write(mdata, ("properties", dimension.api_name), "fieldExclusions", field_exclusions[dimension.api_name])
    for metric in metrics:
        mdata = metadata.write(mdata, ("properties", metric.api_name), "tap_ga4.group", metric.category)
        mdata = metadata.write(mdata, ("properties", metric.api_name), "behavior", "METRIC")
        mdata = metadata.write(mdata, ("properties", metric.api_name), "fieldExclusions", field_exclusions[metric.api_name])
    return mdata


def generate_schema_and_metadata(dimensions, metrics, field_exclusions):
    LOGGER.info("Discovering fields")
    schema = generate_base_schema()
    add_dimensions_to_schema(schema, dimensions)
    add_metrics_to_schema(schema, metrics)
    mdata = generate_metadata(schema, dimensions, metrics, field_exclusions)
    return schema, mdata


def generate_catalog(reports, dimensions, metrics, field_exclusions):
    schema, mdata = generate_schema_and_metadata(dimensions, metrics, field_exclusions)
    catalog_entries = []
    LOGGER.info("Generating catalog")
    for report in reports:
        catalog_entries.append(CatalogEntry(schema=Schema.from_dict(schema),
                                            key_properties=['_sdc_record_hash'],
                                            stream=report['name'],
                                            tap_stream_id=report['id'],
                                            metadata=metadata.to_list(mdata)))

    return Catalog(catalog_entries)


def get_field_exclusions(client, property_id, dimensions, metrics):
    field_exclusions = defaultdict(list)
    LOGGER.info("Discovering dimension field exclusions")
    # TODO: Get the exclusions for `Cohort` fields
    for dimension in dimensions:
        if dimension.category != 'Cohort':
            res = client.check_compatibility(CheckCompatibilityRequest(
                property=f'properties/{property_id}',
                dimensions=[Dimension(name=dimension.api_name)],
                compatibility_filter='INCOMPATIBLE'
            ))
            for field in res.dimension_compatibilities:
                field_exclusions[dimension.api_name].append(field.dimension_metadata.api_name)
            for field in res.metric_compatibilities:
                field_exclusions[dimension.api_name].append(field.metric_metadata.api_name)

    LOGGER.info("Discovering metric field exclusions")
    for metric in metrics:
        if metric.category != 'Cohort':
            res = client.check_compatibility(CheckCompatibilityRequest(
                property=f'properties/{property_id}',
                metrics=[Metric(name=metric.api_name)],
                compatibility_filter='INCOMPATIBLE'
            ))
            for field in res.dimension_compatibilities:
                field_exclusions[metric.api_name].append(field.dimension_metadata.api_name)
            for field in res.metric_compatibilities:
                field_exclusions[metric.api_name].append(field.metric_metadata.api_name)

    return field_exclusions


def get_dimensions_and_metrics(client, property_id):
    request = GetMetadataRequest(
        name=f"properties/{property_id}/metadata",
    )

    response = client.get_metadata(request)
    return response.dimensions, response.metrics


def discover(client, reports, property_id):
    dimensions, metrics = get_dimensions_and_metrics(client, property_id)
    field_exclusions = get_field_exclusions(client, property_id, dimensions, metrics)
    catalog = generate_catalog(reports, dimensions, metrics, field_exclusions)
    write_catalog(catalog)
