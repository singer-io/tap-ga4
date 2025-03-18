import json
import os
from collections import defaultdict
import unittest
from tap_ga4.discover import get_dimensions_and_metrics
from tap_ga4.client import Client


class TestFieldExclusions(unittest.TestCase):

    def setUp(self):
        self.client_config = {
            "refresh_token": os.getenv("TAP_GA4_REFRESH_TOKEN"),
            "oauth_client_id": os.getenv("TAP_GA4_CLIENT_ID"),
            "oauth_client_secret": os.getenv("TAP_GA4_CLIENT_SECRET"),
            "property_id": os.getenv("TAP_GA4_PROPERTY_ID")
        }
        self.client = Client(self.client_config)

    def get_default_field_exclusions(self, client, property_id):
        
        dimensions, metrics, _ = get_dimensions_and_metrics(client, 0)
        fields = defaultdict(list)
        for dimension in dimensions:
            # checkCompatibility fails for the "comparison" dimension. Leave its
            # exclusions empty to not block the user.
            if dimension.api_name == "comparison":
                fields[dimension.api_name] = []
            else:
                res = client.check_dimension_compatibility(property_id, dimension)

                for field in res.dimension_compatibilities:
                    fields[dimension.api_name].append(field.dimension_metadata.api_name)
                    for field in res.metric_compatibilities:
                        fields[dimension.api_name].append(field.metric_metadata.api_name)

        for metric in metrics:
            # The checkCompatibility request fails for the following metrics.
            # Their compatibility changes depending on what is selected with
            # them. Leave their exclusions empty to not block the user.
            if metric.api_name in ["advertiserAdClicks", "advertiserAdCost", "advertiserAdCostPerClick", "advertiserAdCostPerKeyEvent",
                                   "advertiserAdImpressions", "organicGoogleSearchAveragePosition", "organicGoogleSearchClickThroughRate",
                                   "organicGoogleSearchImpressions", "returnOnAdSpend", "organicGoogleSearchClicks"]:
                fields[metric.api_name] = []
            else:
                res = client.check_metric_compatibility(property_id, metric)

                for field in res.dimension_compatibilities:
                    fields[metric.api_name].append(field.dimension_metadata.api_name)
                    for field in res.metric_compatibilities:
                        fields[metric.api_name].append(field.metric_metadata.api_name)

        # Used by CircleCi to automatically commit changes
        with open("tap_ga4/new_field_exclusions.json", "w", encoding="utf-8") as outfile:
            fields_json = json.dumps(fields, indent=4)
            outfile.write(fields_json)

        return fields

    def test_field_exclusions_match_cached(self):
        with open("tap_ga4/field_exclusions.json", "r") as infile:
            cached_field_exclusions = json.load(infile)
        generated_field_exclusions = self.get_default_field_exclusions(self.client, self.client_config["property_id"])
        self.assertEqual(generated_field_exclusions, cached_field_exclusions)
