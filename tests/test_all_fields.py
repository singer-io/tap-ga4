from random import choice
import unittest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from tap_tester import menagerie, runner
from tap_tester.logger import LOGGER
from base import GA4Base


class GA4AllFieldsTest(AllFieldsTest, GA4Base):
    """GA4 all fields test implementation """

    DIMENSION = 'DIMENSION'
    METRIC = 'METRIC'
    fields_1 = None
    fields_2 = None
    field_exclusions = {}

    @staticmethod
    def name():
        return "tt_ga4_all_fields"

    def streams_to_test(self):
        # testing all streams creates massive quota issues
        return {'Test Report 1', 'Test Report 2'}

    def streams_to_selected_fields(self):  # pylint: disable=arguments-differ
        # TODO - BUG https://jira.talendforge.org/browse/TDL-21467
        if not self.fields_1 and not self.fields_2:
            self.fields_1 = self.select_random_fields() - {"nth_hour"}
            self.fields_2 = self.select_random_fields() - {"nth_hour"}
        return {
            "Test Report 1": self.fields_1 | self.expected_automatic_fields()["Test Report 1"],
            "Test Report 2": self.fields_2 | self.expected_automatic_fields()["Test Report 2"]
        }

    @property
    def schemas(self):
        test_catalogs = [catalog for catalog in self.found_catalogs
                         if catalog.get('stream_name') in self.streams_to_test()]
        return {
            catalog['stream_name']: menagerie.get_annotated_schema(
                self.conn_id, catalog['stream_id'])
            for catalog in test_catalogs}

    ##########################################################################
    # Overridden setup
    ##########################################################################

    ##########################################################################
    # Helper methods
    ##########################################################################

    def get_field_exclusions(self):
        field_exclusions = {'DIMENSION': {}, 'METRIC': {}}
        for field in self.schemas['Test Report 1']['metadata']:
            behavior = field['metadata'].get('behavior')

            if field['breadcrumb'] == [] or behavior not in ('DIMENSION', 'METRIC'):
                continue

            field_name = field['breadcrumb'][1]
            field_exclusions[behavior][field_name] = set(field['metadata'].get('fieldExclusions'))

        self.field_exclusions = field_exclusions

    def exclude_field(self, all_selectable_dimensions, all_selectable_metrics,
                      selected_field, selected_field_type):
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
        all_selectable_dimensions, all_selectable_metrics = self.exclude_field(
            all_selectable_dimensions, all_selectable_metrics, 'date', self.DIMENSION)

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
                all_selectable_dimensions, all_selectable_metrics = self.exclude_field(
                    all_selectable_dimensions, all_selectable_metrics,
                    random_dimension, self.DIMENSION)

            if all_selectable_metrics and len(selected_metrics) < max_metrics:
                random_metric = choice(all_selectable_metrics)
                selected_metrics.add(random_metric)
                all_selectable_dimensions, all_selectable_metrics = self.exclude_field(
                    all_selectable_dimensions, all_selectable_metrics, random_metric, self.METRIC)

        return selected_dimensions | selected_metrics

    ##########################################################################
    # Overridden Methods
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
    # Overridden Tests
    ##########################################################################

    # Tests are redefined because there is no guarantee a random field
    # selection will returns records

    def test_no_unexpected_streams_replicated(self):
        # gather expectations
        expected_streams = {self.get_stream_name(stream) for stream in self.streams_to_test()}

        # gather results
        synced_stream_names = set(self.synced_records.keys())

        self.assertTrue(synced_stream_names.issubset(expected_streams))

    @unittest.skip("skip until fixed in: https://qlik-dev.atlassian.net/browse/TDL-27149")
    def test_all_fields_for_streams_are_replicated(self):
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                stream_name = self.get_stream_name(stream)

                # gather expectations
                expected_all_keys = self.selected_fields.get(stream_name, set())

                records = [
                    record['data'] for record in
                    self.synced_records.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']

                if records:
                    fields_replicated = self.actual_fields.get(stream, set())

                    # verify that all fields are sent to the target
                    # test the combination of all records
                    self.assertSetEqual(
                        fields_replicated, expected_all_keys,
                        logging=f"verify all fields are replicated for stream {stream}")

    ##########################################################################
    # Tests To Skip
    ##########################################################################

    @unittest.skip("Random selection doesn't always sync records")
    def test_all_streams_sync_records(self):
        pass

#  TODO - Run failed, might have an issue with the way we expect data for some fields
#   https://jira.talendforge.org/browse/TDL-21467 is for the nth_hour field being other
#   We should remove nth_hour from random selection until the bug is addressed.
#  Traceback (most recent call last):
#    File "/usr/local/share/virtualenvs/tap-ga4/bin/tap-ga4", line 11, in <module>
#      load_entry_point('tap-ga4', 'console_scripts', 'tap-ga4')()
#    File "/opt/code/tap-ga4/tap_ga4/__init__.py", line 47, in main
#       raise e
#    File "/opt/code/tap-ga4/tap_ga4/__init__.py", line 43, in main
#       main_impl()
#    File "/opt/code/tap-ga4/tap_ga4/__init__.py", line 35, in main_impl
#       sync(client, config, catalog, state)
#    File "/opt/code/tap-ga4/tap_ga4/sync.py", line 250, in sync
#       sync_report(client, schema, report, start_date, end_date, request_window_size, state)
#    File "/opt/code/tap-ga4/tap_ga4/sync.py", line 196, in sync_report
#      transformer.transform(
#    File "/home/ubuntu/.virtualenvs/tap-ga4/lib/python3.8/site-packages/singer/transform.py", line 153, in transform
#      raise SchemaMismatch(self.errors)
#  singer.transform.SchemaMismatch: Errors during transform
#   date_hour: data does not match {'type': ['string', 'null'], 'format': 'date-time'}
#   date_hour_minute: data does not match {'type': ['string', 'null'], 'format': 'date-time'}
#   nth_hour: data does not match {'selected': True, 'inclusion': 'available', 'type': ['integer', 'null']}
