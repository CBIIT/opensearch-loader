import logging
import unittest
from unittest.mock import Mock
from opensearch_loader.benchmark import prepare_benchmark


class BenchmarkTests(unittest.TestCase):
    def test_same_boundary_in_initial_and_updates_without_mutating_source(self):
        specs = [{'index_name': 'dashboard_file',
                  'initial_query': {'query': 'MATCH (f:file)\nWITH f RETURN f'},
                  'update_queries': [{'name': 'Studies', 'query':
                      'MATCH (s:study)\nOPTIONAL MATCH (s)<--(f:file)\nWITH s, f RETURN f'}]}]
        mg = Mock()
        mg.execute_query.side_effect = [[{'total': 622840}], [{'cutoff': 'file-062284'}]]
        result = prepare_benchmark(specs, .1, mg, logging.getLogger())
        self.assertEqual(mg.execute_query.call_args.args[1]['limit'], 62284)
        self.assertEqual(result[0]['index_name'], 'dashboard_file_benchmark')
        for q in [result[0]['initial_query']] + result[0]['update_queries']:
            self.assertIn('WHERE f.file_id <= $benchmark_file_id_max', q['query'])
            self.assertEqual(q['variables']['benchmark_file_id_max'], 'file-062284')
        self.assertNotIn('variables', specs[0]['initial_query'])
        self.assertEqual(specs[0]['index_name'], 'dashboard_file')

    def test_full_run_unchanged_and_unknown_shapes_fail_before_database_access(self):
        mg = Mock()
        specs = [{'index_name': 'dashboard_file', 'initial_query': {'query': 'RETURN 1'}}]
        self.assertIs(prepare_benchmark(specs, 1, mg, logging.getLogger()), specs)
        for fraction in [0, -1, 2, float('nan'), .1]:
            with self.assertRaises(ValueError):
                prepare_benchmark(specs, fraction, mg, logging.getLogger())
        mg.execute_query.assert_not_called()
