from collections import defaultdict
from random import choice
import json
import os
import unittest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from tap_tester import menagerie, runner, connections
from tap_tester.logger import LOGGER
from datetime import datetime as dt
from datetime import timedelta
from base import GA4Base


class GA4AllFieldsTest(AllFieldsTest, GA4Base):
    """GA4 all fields test implementation """

    DIMENSION = 'DIMENSION'
    METRIC = 'METRIC'
    fields_1 = None
    fields_2 = None


    @staticmethod
    def name():
        return "tt_ga4_all_fields"


    def streams_to_test(self):
        # testing all streams creates massive quota issues
        custom_id_1 = self.custom_reports_names_to_ids()['Test Report 1']
        custom_id_2 = self.custom_reports_names_to_ids()['Test Report 2']
        return {
            custom_id_1,
            custom_id_2
        }


    def streams_to_selected_fields(self):
        if not self.fields_1 and not self.fields_2:
            self.fields_1 = self.select_random_fields()
            self.fields_2 = self.select_random_fields()
        return {
            "Test Report 1": self.fields_1,
            "Test Report 2": self.fields_2
        }


    ##########################################################################
    ### Overriden setup
    ##########################################################################


    # Set up requires and extra step to get catalogs for selecting random fields
    def setUp(self):
        """
        Setup for tests in this module.
        """
        cls = type(self)

        if cls.synced_records:
            return

        # instantiate connection
        conn_id = connections.ensure_connection(self, original_properties=True)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('tap_stream_id') in self.streams_to_test()]

        # For taps that need random selection of fields
        self.schemas = {catalog['stream_name']: menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                        for catalog in test_catalogs}

        self.select_streams_and_fields(conn_id, test_catalogs, self.streams_to_selected_fields())
        self.selected_fields = self.get_all_selected_fields_from_metadata(conn_id, test_catalogs)

        # run initial sync
        cls.record_count_by_stream = self.run_and_verify_sync_mode(conn_id)
        cls.synced_records = runner.get_records_from_target_output()


    ##########################################################################
    ### Helper methods
    ##########################################################################


    def get_field_exclusions(self):
        field_exclusions = {'DIMENSION': {}, 'METRIC': {}}
        for field in self.schemas['Test Report 1']['metadata']:
            behavior = field['metadata'].get('behavior')

            if field['breadcrumb'] == [] or (behavior != 'DIMENSION' and behavior != 'METRIC'):
                continue

            field_name = field['breadcrumb'][1]
            field_exclusions[behavior][field_name] = set(field['metadata'].get('fieldExclusions'))

        self.field_exclusions = field_exclusions


    def exclude_field(self, all_selectable_dimensions, all_selectable_metrics, selected_field, selected_field_type):
        excluded_fields = self.field_exclusions[selected_field_type][selected_field]

        if selected_field_type == self.DIMENSION:
            all_selectable_dimensions.remove(selected_field)
        else:
            all_selectable_metrics.remove(selected_field)

        for field in excluded_fields:
            if field in all_selectable_dimensions:
                all_selectable_dimensions.remove(field)
            if field in all_selectable_metrics:
                all_selectable_metrics.remove(field)
        return all_selectable_dimensions, all_selectable_metrics


    def select_random_fields(self):

        selected_dimensions = set()
        selected_metrics = set()
        self.get_field_exclusions()

        all_selectable_dimensions = list(self.field_exclusions['DIMENSION'].keys())
        all_selectable_metrics = list(self.field_exclusions['METRIC'].keys())

        # date is always selected, include its field_exclusions
        all_selectable_dimensions, all_selectable_metrics = self.exclude_field(all_selectable_dimensions, all_selectable_metrics, 'date', self.DIMENSION)

        # select fewer dimensions to increase chance of replicating data
        max_dimensions = 4
        max_metrics = 10

        # Select random but compatible dimensions and metrics until the max is
        # reached, or there are no more possible selections
        while len(selected_dimensions) < max_dimensions or len(selected_metrics) < max_metrics:

            if not all_selectable_dimensions and not all_selectable_metrics:
                break

            if all_selectable_dimensions and len(selected_dimensions) < max_dimensions:
                random_dimension = choice(all_selectable_dimensions)
                selected_dimensions.add(random_dimension)
                all_selectable_dimensions, all_selectable_metrics = self.exclude_field(all_selectable_dimensions, all_selectable_metrics, random_dimension, self.DIMENSION)

            if all_selectable_metrics and len(selected_metrics) < max_metrics:
                random_metric = choice(all_selectable_metrics)
                selected_metrics.add(random_metric)
                all_selectable_dimensions, all_selectable_metrics = self.exclude_field(all_selectable_dimensions, all_selectable_metrics, random_metric, self.METRIC)

        return selected_dimensions | selected_metrics


    ##########################################################################
    ### Overridden Methods
    ##########################################################################


    # Removes assertion that each stream syncs records
    # No guarantee random selection will sync records
    def run_and_verify_sync_mode(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        sync_record_count = runner.examine_target_output_file(
             self, conn_id, self.expected_stream_names(), self.expected_primary_keys())
        LOGGER.info("total replicated row count: %s", sum(sync_record_count.values()))

        return sync_record_count


    ##########################################################################
    ### Overridden Tests
    ##########################################################################

    # Tests are redefined because there is no guarantee a random field
    # selection will returns records

    def test_no_unexpected_streams_replicated(self):
        # gather expectations
        expected_streams = {self.get_stream_name(stream) for stream in self.streams_to_test()}

        # gather results
        synced_stream_names = set(self.synced_records.keys())

        self.assertTrue(synced_stream_names.issubset(expected_streams))


    def test_all_fields_for_streams_are_replicated(self):
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                stream_name = self.get_stream_name(stream)

                # gather expectations
                expected_all_keys = self.selected_fields.get(stream_name, set())

                if self.synced_records.get(stream_name):
                    # gather results
                    actual_all_keys_per_record = [set(message['data'].keys()) for message in
                                                  self.get_upsert_messages_for_stream(self.synced_records, stream_name)]

                    for actual_all_keys in actual_all_keys_per_record:
                        self.assertSetEqual(expected_all_keys, actual_all_keys)


    ##########################################################################
    ### Tests To Skip
    ##########################################################################


    @unittest.skip("Random selection doesn't always sync records")
    def test_all_streams_sync_records(self):
        pass


    def __init__(self, test_run):
        super().__init__(test_run)
