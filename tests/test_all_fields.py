from collections import defaultdict
from random import choice
import json
import os
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from datetime import datetime as dt
from datetime import timedelta

from base import GA4Base


class GA4AllFieldsTest(AllFieldsTest, GA4Base):
    """GA4 all fields test implementation """


    @staticmethod
    def name():
        return "tt_ga4_all_fields"


    def streams_to_test(self):
        # testing all streams creates massive quota issues
        custom_id = self.custom_reports_names_to_ids()['Test Report 1']
        return {
            custom_id
        }


    def streams_to_selected_fields(self):
        self.get_field_exclusions("Test Report 1")
        return {
            "Test Report 1": {
                "active_users",
            },
        }


    def get_field_exclusions(self, stream):
        field_exclusions = {'DIMENSION': {}, 'METRIC': {}}
        for field in self.streams_to_schemas[stream]['metadata']:
            behavior = field['metadata'].get('behavior')

            if field['breadcrumb'] == [] or (behavior != 'DIMENSION' and behavior != 'METRIC'):
                continue

            field_name = field['breadcrumb'][1]
            field_exclusions[behavior][field_name] = set(field['metadata'].get('fieldExclusions'))

            import ipdb; ipdb.set_trace()
            1+1

        return field_exclusions


    def select_random_fields(self):
        all_field_exclusions = {}
        dimensions = set()
        metrics = set()

        field_exclusions = self.get_field_exclusions('Test Report 1')

        while len(dimensions) < 8 or len(metrics) < 10:
            random_dimension = choice([field_exclusions['DIMENSION'].keys()])
            random_metric = choice([field_exclusions['METRIC'].keys()])

            if random_dimension not in all_field_exclusions:
                dimensions.add(random_dimension)
                all_field_exclusions.update(field_exclusions['DIMENSION'][random_dimension])

            if random_metric not in all_field_exclusions:
                metrics.add(random_metric)
                all_field_exclusions.update(field_exclusions['METRIC'][random_metric])

        return dimensions | metrics


    def __init__(self, test_run):
        super().__init__(test_run)
