import unittest

from opensearch_loader.config import Config
from opensearch_loader.memgraph_client import MemgraphClient


class ConfigTestModePagesTests(unittest.TestCase):
    def test_defaults_to_first_page_only(self):
        config = Config()
        self.assertEqual(config.get_test_mode_page_range(), (1, 1))

    def test_reads_explicit_page_range(self):
        config = Config()
        config.config['test_mode_page_start'] = 460
        config.config['test_mode_page_end'] = 470
        self.assertEqual(config.get_test_mode_page_range(), (460, 470))

    def test_start_page_without_end_uses_single_page(self):
        config = Config()
        config.config['test_mode_page_start'] = 460
        self.assertEqual(config.get_test_mode_page_range(), (460, 460))

    def test_rejects_descending_page_range(self):
        config = Config()
        config.config['test_mode_page_start'] = 470
        config.config['test_mode_page_end'] = 460
        with self.assertRaises(ValueError):
            config.get_test_mode_page_range()


class ExecutePaginatedQueryTestModeTests(unittest.TestCase):
    def test_stops_after_configured_number_of_pages(self):
        client = object.__new__(MemgraphClient)
        page_calls = []
        fake_pages = [
            [{'id': 1}],
            [{'id': 2}],
            [{'id': 3}],
        ]

        def fake_execute_query(query, parameters=None):
            page_calls.append(parameters['skip'])
            return fake_pages[len(page_calls) - 1]

        client.execute_query = fake_execute_query
        client.last_query_time = 0.0

        results = list(
            MemgraphClient.execute_paginated_query(
                client,
                query='RETURN 1',
                page_size=1,
                test_mode=True,
                test_page_start=1,
                test_page_end=2,
            )
        )

        self.assertEqual(results, fake_pages[:2])
        self.assertEqual(page_calls, [0, 1])

    def test_starts_and_stops_at_configured_page_range(self):
        client = object.__new__(MemgraphClient)
        page_calls = []
        fake_pages = [
            [{'id': 1}],
            [{'id': 2}],
        ]

        def fake_execute_query(query, parameters=None):
            page_calls.append(parameters['skip'])
            return fake_pages[len(page_calls) - 1]

        client.execute_query = fake_execute_query
        client.last_query_time = 0.0

        results = list(
            MemgraphClient.execute_paginated_query(
                client,
                query='RETURN 1',
                page_size=1,
                test_mode=True,
                test_page_start=460,
                test_page_end=461,
            )
        )

        self.assertEqual(results, fake_pages)
        self.assertEqual(page_calls, [459, 460])


if __name__ == '__main__':
    unittest.main()