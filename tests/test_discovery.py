import unittest

from tap_tester import menagerie, connections, LOGGER
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest
from tap_tester.jira_client import JiraClient as jira_client
from tap_tester.jira_client import CONFIGURATION_ENVIRONMENT as jira_config

JIRA_CLIENT = jira_client({ **jira_config })

from base import GA4Base


class GA4DiscoveryTest(DiscoveryTest, GA4Base):
    """Standard Discovery Test"""

    jira_status = None

    @staticmethod
    def name():
        return "tt_ga4_discovery"

    def streams_to_test(self):
        return set(self.expected_metadata().keys())

    def test_replication_metadata_by_streams(self):
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):

                # gather expectations
                expected_replication_key = self.expected_replication_keys()[stream]
                expected_replication_method = self.expected_replication_method()[stream]

                # gather results
                catalog = [catalog for catalog in self.found_catalogs
                           if catalog["stream_name"] == stream][0]
                schema_and_metadata = menagerie.get_annotated_schema(self.conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                self.assertIn( "metadata", stream_properties[0])
                stream_metadata = stream_properties[0]["metadata"]
                self.assertIn( self.REPLICATION_KEYS, stream_metadata)
                self.assertIn( self.REPLICATION_METHOD, stream_metadata)
                actual_replication_key = set(stream_metadata[self.REPLICATION_KEYS])
                actual_replication_method = stream_metadata[self.REPLICATION_METHOD]

                if not isinstance(actual_replication_method, str):
                    LOGGER.info("***** Conditional pass / fail based on JIRA status of TDL-22416")
                    if not self.jira_status:
                        self.jira_status = JIRA_CLIENT.get_jira_issue_status('TDL-22416')
                    self.assertNotEqual(self.jira_status, "Done",
                                        msg ="JIRA status = Done. Remove test work around")
                    self.assertTrue(isinstance(actual_replication_method, list))
                    self.assertEqual(len(actual_replication_method), 1)
                    actual_replication_method = actual_replication_method[0]

                # verify replication key(s) are marked in metadata
                with self.subTest(msg="validating replication keys"):
                    self.assertTrue(expected_replication_key.issubset(actual_replication_key),
                        logging=f"verify {expected_replication_key} is saved in metadata as a valid-replication-key"
                    )

                # verify the actual replication matches our expected replication method
                with self.subTest(msg="validating replication method"):
                    self.assertEqual(expected_replication_method, actual_replication_method,
                        logging=f"verify the replication method is {expected_replication_method}"
                    )

                # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                # If replication keys are not specified in metadata, skip this check
                with self.subTest(msg="validating expectations consistency"):
                    if actual_replication_key:
                        self.assertEqual(actual_replication_method, self.INCREMENTAL,
                            logging=f"verify the forced replication method is {self.INCREMENTAL} since there is a replication-key"
                        )
                    else:
                        self.assertEqual(actual_replication_method, self.FULL_TABLE,
                            logging=f"verify the forced replication method is {self.FULL_TABLE} since there is no replication-key"
                        )

    @unittest.skip("Does Not Apply")
    def test_stream_naming(self):
        """
        This tap accepts user provided stream names in the config via the report_definitions
        field and does not conform to the expaction that a stream's name must satisfy the condition:


            re.fullmatch(r"[a-z_]+", name)

        So this test case is skipped.
        """
