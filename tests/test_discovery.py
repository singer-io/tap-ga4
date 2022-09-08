"""
Test tap discovery
"""
import re

from tap_tester import menagerie, connections

from base import GA4Base


class DiscoveryTest(GA4Base):
    """ Test the tap discovery """

    @staticmethod
    def name():
        return "tt_ga4_discovery_test"

    def streams_to_test(self):
        return self.expected_metadata().keys()

    def test_run(self):
        """
        Verify that discover creates the appropriate catalog, schema, metadata, etc.

        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • verify there is only 1 top level breadcrumb
        • verify there are no duplicate/conflicting metadata entries
        • verify replication key(s)
        • verify primary key(s)
        • verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • verify the actual replication matches our expected replication method
        • verify that primary, replication and foreign keys
          are given the inclusion of automatic (metadata and annotated schema).
        • verify that all other fields have inclusion of available (metadata and schema)
        """

        streams_to_test = self.streams_to_test()

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Verify the number of catalog entries match the number of expected streams
        expected_stream_names = set(self.expected_stream_names())
        with self.subTest(msg="catalog size"):
            self.assertEqual(len(found_catalogs), len(expected_stream_names),
                             logging=f"verify {len(expected_stream_names)} streams were discovered")

        # Verify the stream names discovered were what we expect
        found_stream_names = {c['stream_name'] for c in found_catalogs}
        with self.subTest(msg="streams present"):
            self.assertEqual(
                expected_stream_names, set(found_stream_names),
                logging=f"verify the expected streams discovered were: {expected_stream_names}"
            )

        # *** Does not apply ***

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        with self.subTest(msg="stream naming conventions"):
            self.assertTrue(
                all([re.fullmatch(r"[a-z_]+", name) for name in found_stream_names]),
                logging="verify stream names use on lower case alpha or underscore characters"
            )

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # gather expectations
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_key = self.expected_replication_keys()[stream]
                expected_automatic_fields = self.expected_automatic_fields()[stream]
                expected_replication_method = self.expected_replication_method()[stream]

                # gather results
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                actual_replication_key = set(stream_properties[0].get(
                    "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, []))
                actual_primary_keys = set(stream_properties[0].get(
                    "metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, []))
                actual_replication_method = stream_properties[0].get(
                    "metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)
                actual_automatic_fields = {item.get("breadcrumb", ["properties", None])[1]
                                           for item in metadata
                                           if item.get("metadata").get("inclusion") == "automatic"}
                actual_fields = [md_entry["breadcrumb"][1]
                                 for md_entry in metadata
                                 if md_entry["breadcrumb"] != []]

                # verify the stream level properties are as expected
                # verify there is only 1 top level breadcrumb
                with self.subTest(msg="breadcrumb structure"):
                    self.assertEqual(
                        len(stream_properties), 1,
                        logging="verify there is only one top level breadcrumb"
                    )

                # Verify there are no duplicate/conflicting metadata entries.
                with self.subTest(msg="metadata structure"):
                    self.assertEqual(
                        len(actual_fields), len(set(actual_fields)),
                        logging="verify there are no duplicate entries in metadata"
                    )

                # verify primary key(s) are marked in metadata
                with self.subTest(msg="primary keys"):
                    self.assertEqual(
                        actual_primary_keys, expected_primary_keys,
                        logging=f"verify {expected_primary_keys} is saved in metadata as the primary-key"
                    )

                # verify replication key(s) are marked in metadata
                with self.subTest(msg="replication keys"):
                    self.assertEqual(
                        actual_replication_key, expected_replication_key,
                        logging=f"verify {expected_replication_key} is saved in metadata as the replication-key"
                    )

                # verify the actual replication matches our expected replication method
                with self.subTest(msg="replication method"):
                    self.assertEqual(
                        expected_replication_method, actual_replication_method,
                        logging=f"verify the replication method is {expected_replication_method}"
                    )

                # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                # If replication keys are not specified in metadata, skip this check
                with self.subTest(msg="expectations consistency"):
                    if actual_replication_key:
                        self.assertEqual(
                            actual_replication_method, self.INCREMENTAL,
                            logging=f"verify the forced replication method is {self.INCREMENTAL} since there is a replication-key"
                        )
                    else:
                        self.assertEqual(
                            actual_replication_method, self.FULL_TABLE,
                            logging=f"verify the forced replication method is {self.FULL_TABLE} since there is no replication-key"
                        )

                # verify that primary, replication are given the inclusion of automatic in metadata.
                with self.subTest(msg="automatic fields"):
                    self.assertSetEqual(
                        expected_automatic_fields, actual_automatic_fields,
                        logging="verify primary and replication key fields are automatic"
                    )

                # verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                inclusions_other_than_automatic = {item.get("metadata").get("inclusion")
                                                   for item in metadata
                                                   if item.get("breadcrumb", []) != []
                                                   and item.get("breadcrumb", ["properties", None])[1] not in actual_automatic_fields}
                with self.subTest(msg="automatic fields"):
                    self.assertSetEqual(
                        inclusions_other_than_automatic, {'available'},
                        msg="Not all non key properties are set to 'available' in metadata",
                        logging="verify all non-automatic fields are available")
