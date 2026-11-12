"""Verify per-query Memgraph and OpenSearch timing reports."""

import os
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path
from unittest.mock import MagicMock, patch

from opensearch_loader.loader import Loader


class QueryTimingTests(unittest.TestCase):
    def setUp(self):
        self.loader = object.__new__(Loader)
        self.loader.config = MagicMock()
        self.loader.config.get_selected_update_queries.return_value = None
        self.loader.config.get_test_mode.return_value = False
        self.loader.memgraph = MagicMock(last_query_time=1.25)
        self.loader.opensearch = MagicMock()
        self.loader.query_timings = defaultdict(list)
        self.loader.opensearch_timings = defaultdict(float)
        self.loader._parse_mapping = MagicMock(return_value={'id': {}})
        self.loader._validate_query_fields = MagicMock(return_value=True)

    def test_initial_and_update_queries_accumulate_opensearch_pages_and_retries(self):
        self.loader.memgraph.execute_paginated_query.side_effect = [
            iter([[{'id': '1'}], [{'id': '2'}]]),
            iter([[{'id': '1'}]]),
        ]
        self.loader.opensearch.bulk_update.side_effect = [RuntimeError('retry'), None]
        spec = {
            'index_name': 'participants', 'id_field': 'id',
            'mapping': {'keyword': ['id']},
            'initial_query': {'query': 'RETURN initial'},
            'update_queries': [{'name': 'diagnoses', 'query': 'RETURN updates'}],
        }
        with patch('opensearch_loader.loader.time.perf_counter',
                   side_effect=[0, 2, 10, 13, 20, 24, 30, 36]), \
                patch('opensearch_loader.loader.time.sleep'):
            self.assertEqual(self.loader._process_query_index(spec), 2)
        self.assertEqual(dict(self.loader.opensearch_timings), {
            'participants:Initial Query': 5, 'participants:diagnoses': 10,
        })
        self.assertEqual(dict(self.loader.query_timings), {
            'participants:Initial Query': [1.25], 'participants:diagnoses': [1.25],
        })

    def test_failed_bulk_attempts_are_timed(self):
        self.loader.memgraph.execute_paginated_query.return_value = iter([[{'id': '1'}]])
        self.loader.opensearch.bulk_update.side_effect = RuntimeError('failed')
        with patch('opensearch_loader.loader.time.perf_counter', side_effect=[0, 2, 10, 13]), \
                patch('opensearch_loader.loader.time.sleep'):
            with self.assertRaises(RuntimeError):
                self.loader._process_update_query(
                    'participants', 'id', {'name': 'diagnoses', 'query': 'RETURN updates'}, {})
        self.assertEqual(self.loader.opensearch_timings['participants:diagnoses'], 5)

    def test_no_results_reports_zero_opensearch_time(self):
        self.loader.memgraph.execute_paginated_query.return_value = iter([])
        self.loader._process_update_query(
            'participants', 'id', {'name': 'empty', 'query': 'RETURN updates'}, {})
        self.assertEqual(self.loader.query_timings['participants:empty'], [1.25])
        self.assertEqual(self.loader.opensearch_timings.get('participants:empty', 0), 0)
        self.loader.opensearch.bulk_update.assert_not_called()

    def test_console_and_saved_report_include_both_times(self):
        self.loader.index_stats = [
            {'index_name': 'participants', 'document_count': 2, 'duration': 65, 'error': False}]
        self.loader.query_timings.update({
            'participants:Initial Query': [1.25], 'participants:diagnoses': [2.5]})
        self.loader.opensearch_timings['participants:Initial Query'] = 3.75
        self.loader.opensearch_timings['participants:diagnoses'] = 4.5
        with tempfile.TemporaryDirectory() as directory:
            original_directory = os.getcwd()
            try:
                os.chdir(directory)
                with patch('opensearch_loader.loader.logger') as logger:
                    self.loader._print_summary(70)
                report = next(Path('logs').glob('*.loading-summary')).read_text()
                self.assertEqual(logger.info.call_args_list[0].args[0], '\n' + report)
                self.assertIn('Memgraph (s)', report)
                self.assertIn('OpenSearch (s)', report)
                self.assertIn('1m 5s', report)
                for key, expected in [('participants:Initial Query', ['1.2500', '3.7500']),
                                      ('participants:diagnoses', ['2.5000', '4.5000'])]:
                    row = next(line for line in report.splitlines() if line.startswith(key))
                    self.assertEqual(row.split()[-2:], expected)
            finally:
                os.chdir(original_directory)
