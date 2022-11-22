from collections import defaultdict
from functools import reduce
import json
import re
import os
import singer
from singer import Catalog, CatalogEntry, Schema, metadata
from singer.catalog import write_catalog
from tap_ga4.reports import PREMADE_REPORTS

LOGGER = singer.get_logger()


DIMENSION_INTEGER_FIELD_OVERRIDES = {"cohortNthDay",
                                     "cohortNthMonth",
                                     "cohortNthWeek",
                                     "day",
                                     "dayOfWeek",
                                     "hour",
                                     "minute",
                                     "month",
                                     "nthDay",
                                     "nthHour",
                                     "nthMinute",
                                     "nthMonth",
                                     "nthWeek",
                                     "nthYear",
                                     "percentScrolled",
                                     "week",
                                     "year"}

DIMENSION_DATETIME_FIELD_OVERRIDES = {"date",
                                      "dateHour",
                                      "dateHourMinute",
                                      "firstSessionDate"}

FLOAT_TYPES = {"TYPE_FLOAT",
               "TYPE_SECONDS",
               "TYPE_MILLISECONDS",
               "TYPE_MINUTES",
               "TYPE_HOURS",
               "TYPE_STANDARD",
               "TYPE_CURRENCY",
               "TYPE_FEET",
               "TYPE_MILES",
               "TYPE_METERS",
               "TYPE_KILOMETERS"}

# Cohort is incompatible with `date`, which is required.
INCOMPATIBLE_CATEGORIES = {"Cohort"}


def add_metrics_to_schema(schema, metrics):
    for metric in metrics.keys():
        metric_type = metrics[metric].type_.name
        if metric_type == "TYPE_INTEGER":
            schema["properties"][metric] = {"type": ["integer", "null"]}
        elif metric_type in FLOAT_TYPES:
            schema["properties"][metric] = {"type": ["number", "null"]}
        else:
            raise Exception(f"Unknown Google Analytics 4 type: {metric_type}")


def add_dimensions_to_schema(schema, dimensions):
    for dimension in dimensions.keys():
        if dimensions[dimension].api_name in DIMENSION_INTEGER_FIELD_OVERRIDES:
            schema["properties"][dimension] = {"type": ["integer", "null"]}
        elif dimensions[dimension].api_name in DIMENSION_DATETIME_FIELD_OVERRIDES:
            # datetime is not always a valid datetime string
            # https://support.google.com/analytics/answer/9309767
            schema["properties"][dimension] = \
                {"anyOf": [
                    {"type": ["string", "null"], "format": "date-time"},
                    {"type": ["string", "null"]}
                ]}
        else:
            schema["properties"][dimension] = {"type": ["string", "null"]}


def generate_base_schema():
    return {"type": "object", "properties": {"_sdc_record_hash": {"type": "string"},
                                             "property_id": {"type": "string"},
                                             "account_id": {"type": "string"}}}



def to_snake_case(name):
    """
    GA4 api names are camelCase, and GA4 custom api names must follow
    these rules:
    https://support.google.com/analytics/answer/10085872?hl=en#event-name-rules

    Match the position before a capital letter or position of a `:`
    unless capital letter is at the beginning of the word or after a `:`

    example:
      name:    customEvent:PageLocation
      match:         ^    ^    ^
      return:  custom_event_page_location
    """
    return re.sub(r'(?<!^)(?<!:)(?=[A-Z])|[:]', '_', name).lower()


def generate_metadata(schema, dimensions, metrics, invalid_metrics, field_exclusions, is_premade=False):
    mdata = metadata.get_standard_metadata(schema=schema, key_properties=["_sdc_record_hash"], valid_replication_keys=["date"],
                                           replication_method=["INCREMENTAL"])
    mdata = metadata.to_map(mdata)
    mdata = reduce(lambda mdata, field_name: metadata.write(mdata, ("properties", field_name), "inclusion", "automatic"),
                   ["_sdc_record_hash", "property_id", "account_id", "date"],
                   mdata)
    mdata = reduce(lambda mdata, field_name: metadata.write(mdata, ("properties", field_name), "tap_ga4.group", "Report Field"),
                   ["_sdc_record_hash", "property_id", "account_id"],
                   mdata)

    for dimension in dimensions.keys():
        mdata = metadata.write(mdata, ("properties", dimension), "tap_ga4.group", dimensions[dimension].category)
        mdata = metadata.write(mdata, ("properties", dimension), "behavior", "DIMENSION")
        mdata = metadata.write(mdata, ("properties", dimension), "fieldExclusions", field_exclusions[dimension])
        mdata = metadata.write(mdata, ("properties", dimension), "tap-ga4.api-field-names", dimensions[dimension].api_name)
        if is_premade:
            mdata = metadata.write(mdata, ("properties", dimension), "selected-by-default", True)

    for metric in metrics.keys():
        mdata = metadata.write(mdata, ("properties", metric), "tap_ga4.group", metrics[metric].category)
        mdata = metadata.write(mdata, ("properties", metric), "behavior", "METRIC")
        mdata = metadata.write(mdata, ("properties", metric), "fieldExclusions", field_exclusions[metric])
        mdata = metadata.write(mdata, ("properties", metric), "tap-ga4.api-field-names", metrics[metric].api_name)
        if is_premade:
            mdata = metadata.write(mdata, ("properties", metric), "selected-by-default", True)

    for metric in invalid_metrics.keys():
        mdata = metadata.write(mdata, ("properties", metric), "tap_ga4.group", invalid_metrics[metric].category)
        mdata = metadata.write(mdata, ("properties", metric), "behavior", "METRIC")
        mdata = metadata.write(mdata, ("properties", metric), "tap-ga4.api-field-names", invalid_metrics[metric].api_name)
        mdata = metadata.write(mdata, ("properties", metric), "inclusion", "unsupported")

    return mdata


def generate_schema_and_metadata(dimensions, metrics, invalid_metrics, field_exclusions, report, is_premade=False):
    LOGGER.info("Discovering fields for report: %s", report["name"])
    schema = generate_base_schema()
    # Convert field names to snake_case for consistency across downstream use-cases
    snake_dimensions = {to_snake_case(dimension.api_name):dimension for dimension in dimensions}
    snake_metrics = {to_snake_case(metric.api_name):metric for metric in metrics}
    snake_invalids = {}
    if not is_premade:
        snake_invalids = {to_snake_case(metric.api_name):metric for metric in invalid_metrics}
    add_dimensions_to_schema(schema, snake_dimensions)
    add_metrics_to_schema(schema, snake_metrics)
    add_metrics_to_schema(schema, snake_invalids)
    mdata = generate_metadata(schema, snake_dimensions, snake_metrics, snake_invalids, field_exclusions, is_premade)
    return schema, mdata


def generate_catalog(reports, dimensions, metrics, invalid_metrics, field_exclusions):
    catalog_entries = []
    LOGGER.info("Generating catalog")
    for report in PREMADE_REPORTS:
        report_dimensions = [dimension for dimension in dimensions
                             if dimension.api_name in report["dimensions"]]
        report_metrics = [metric for metric in metrics
                          if metric.api_name in report["metrics"]]
        schema, mdata = generate_schema_and_metadata(report_dimensions, report_metrics, None, field_exclusions, report, is_premade=True)
        catalog_entries.append(CatalogEntry(schema=Schema.from_dict(schema),
                                            key_properties=["_sdc_record_hash"],
                                            stream=report["name"],
                                            tap_stream_id=report["name"],
                                            metadata=metadata.to_list(mdata)))
    for report in reports:
        schema, mdata = generate_schema_and_metadata(dimensions, metrics, invalid_metrics, field_exclusions, report)
        catalog_entries.append(CatalogEntry(schema=Schema.from_dict(schema),
                                            key_properties=["_sdc_record_hash"],
                                            stream=report["name"],
                                            tap_stream_id=report["id"],
                                            metadata=metadata.to_list(mdata)))

    return Catalog(catalog_entries)


def get_field_exclusions(client, property_id, dimensions, metrics):
    field_exclusions = defaultdict(list)
    field_exclusions_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "field_exclusions.json")
    with open(field_exclusions_path, "r", encoding="utf-8") as infile:
        field_exclusions.update(json.load(infile))

    LOGGER.info("Discovering dimension field exclusions")
    for dimension in dimensions:
        if dimension.api_name in field_exclusions:
            continue
        res = client.check_dimension_compatibility(property_id, dimension)
        for field in res.dimension_compatibilities:
            field_exclusions[dimension.api_name].append(
                field.dimension_metadata.api_name)
        for field in res.metric_compatibilities:
            field_exclusions[dimension.api_name].append(
                field.metric_metadata.api_name)

    LOGGER.info("Discovering metric field exclusions")
    for metric in metrics:
        if metric.api_name in field_exclusions:
            continue
        res = client.check_metric_compatibility(property_id, metric)
        for field in res.dimension_compatibilities:
            field_exclusions[metric.api_name].append(field.dimension_metadata.api_name)
        for field in res.metric_compatibilities:
            field_exclusions[metric.api_name].append(field.metric_metadata.api_name)

    field_exclusions = {to_snake_case(key):[to_snake_case(v) for v in value] for (key,value) in field_exclusions.items()}
    return field_exclusions


# We've observed failures in metric compatiblity requests
# where the api_name contains non-alphanumeric, non-ascii characters.
# see: https://support.google.com/analytics/thread/176551995/conversion-event-api-calls-should-use-event-id-not-name-sessionconversionrate-conversion-event-name
def is_valid_alphanumeric_name(name):
    """
    Returns boolean determining if name is an ascii alphanumeric string.
    Brackets and colons are also part of valid names to send to Google.
    """
    return re.match(r"^[a-zA-Z0-9\[\]_:]+$", name)


def get_dimensions_and_metrics(client, property_id):
    response = client.get_dimensions_and_metrics(property_id)
    dimensions = [dimension for dimension in response.dimensions
                  if dimension.category not in INCOMPATIBLE_CATEGORIES]
    metrics = [metric for metric in response.metrics
               if metric.category not in INCOMPATIBLE_CATEGORIES
               and is_valid_alphanumeric_name(metric.api_name)]
    invalid_metrics = [metric for metric in response.metrics
                            if metric.category not in INCOMPATIBLE_CATEGORIES
                            and not is_valid_alphanumeric_name(metric.api_name)]
    return dimensions, metrics, invalid_metrics


def discover(client, reports, property_id):
    dimensions, metrics, invalid_metrics = get_dimensions_and_metrics(client, property_id)
    field_exclusions = get_field_exclusions(client, property_id, dimensions, metrics)
    catalog = generate_catalog(reports, dimensions, metrics, invalid_metrics, field_exclusions)
    write_catalog(catalog)
