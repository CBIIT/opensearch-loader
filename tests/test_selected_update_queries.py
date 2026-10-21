"""Tests for selecting update queries independently of list append behavior."""

import os
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from opensearch_loader.cli import parse_args
from opensearch_loader.config import Config
from opensearch_loader.loader import Loader


class QuerySelectionConfigTests(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(os.environ, {}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_default_is_full_load(self):
        self.assertIsNone(Config().get_selected_update_queries())

    def test_normalizes_names_and_keeps_boolean_like_names_as_strings(self):
        for value, expected in [(' diagnoses, exposures, diagnoses ', ['diagnoses', 'exposures']),
                                ('false', ['false']), ('1', ['1'])]:
            with self.subTest(value=value):
                with patch.dict(os.environ, {'OS_LOADER_SELECTED_UPDATE_QUERIES': value}):
                    self.assertEqual(Config().get_selected_update_queries(), expected)

    def test_cli_overrides_env_and_yaml_and_env_overrides_yaml(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.yaml'
            path.write_text('selected_update_queries: [from_yaml]\n')
            self.assertEqual(Config(str(path)).get_selected_update_queries(), ['from_yaml'])
            with patch.dict(os.environ, {'OS_LOADER_SELECTED_UPDATE_QUERIES': 'from_env'}):
                self.assertEqual(Config(str(path)).get_selected_update_queries(), ['from_env'])
                with patch('sys.argv', ['loader', '--selected-update-queries', 'from_cli']):
                    args = parse_args()
                self.assertEqual(Config(str(path), args).get_selected_update_queries(), ['from_cli'])

    def test_rejects_empty_or_invalid_selection(self):
        for value in ['', [], ' , ', False, 1, ['diagnoses', 2]]:
            with self.subTest(value=value):
                config = Config()
                config.config['selected_update_queries'] = value
                with self.assertRaises(ValueError):
                    config.get_selected_update_queries()


class QuerySelectionLoaderTests(unittest.TestCase):
    def setUp(self):
        self.loader = object.__new__(Loader)
        with patch.dict(os.environ, {}, clear=True):
            self.loader.config = Config()
        self.loader.max_nesting_depth = self.loader.config.get_max_nesting_depth()
        self.loader.config.config.update({
            'selected_update_queries': ['second', 'first'],
            'clear_existing_indices': True,
        })
        self.loader.opensearch = MagicMock()
        self.loader.memgraph = MagicMock(last_query_time=0)
        self.loader.query_timings = defaultdict(list)
        self.loader.index_stats = []
        self.loader.model = None
        self.spec = {
            'index_name': 'participants', 'id_field': 'id',
            'mapping': {'keyword': ['id', 'value']},
            'initial_query': {'query': 'RETURN initial'},
            'update_queries': [
                {'name': 'first', 'query': 'RETURN first'},
                {'name': 'unselected', 'query': 'RETURN unselected'},
                {'name': 'second', 'query': 'RETURN second'},
            ],
        }

    def test_runs_only_selected_updates_in_spec_order_and_preserves_index(self):
        self.loader.memgraph.execute_paginated_query.side_effect = [
            iter([[{'id': 'p1', 'value': 'first'}]]),
            iter([[{'id': 'p1', 'value': 'second'}]]),
        ]
        self.assertEqual(self.loader._process_query_index(self.spec), 0)
        self.assertEqual([call.args[0] for call in
                          self.loader.memgraph.execute_paginated_query.call_args_list],
                         ['RETURN first', 'RETURN second'])
        self.loader.opensearch.create_index.assert_not_called()
        self.loader.opensearch.delete_index.assert_not_called()
        self.loader.opensearch.bulk_upsert.assert_not_called()
        self.assertEqual(self.loader.opensearch.bulk_update.call_count, 2)
        self.assertEqual([call.kwargs['query_name'] for call in
                          self.loader.opensearch.bulk_update.call_args_list],
                         ['first', 'second'])
        self.loader.opensearch.refresh_index.assert_called_once_with('participants')

    def test_missing_index_fails_without_creating_or_loading(self):
        self.loader.opensearch.index_exists.return_value = False
        with self.assertRaisesRegex(ValueError, 'must exist'):
            self.loader._process_query_index(self.spec)
        self.loader.opensearch.create_index.assert_not_called()
        self.loader.memgraph.execute_paginated_query.assert_not_called()

    def test_unmatched_index_is_skipped(self):
        self.loader.config.config['selected_update_queries'] = ['other']
        self.assertEqual(self.loader._process_query_index(self.spec), 0)
        self.assertEqual(self.loader.opensearch.mock_calls, [])
        self.loader.memgraph.execute_paginated_query.assert_not_called()

    def test_selected_updates_do_not_require_initial_query(self):
        self.spec.pop('initial_query')
        with patch.object(self.loader, '_process_update_query') as update:
            self.assertEqual(self.loader._process_query_index(self.spec), 0)
        self.assertEqual(update.call_count, 2)

    def test_default_retains_initial_load_and_all_updates(self):
        self.loader.config.config.pop('selected_update_queries')
        self.loader.memgraph.execute_paginated_query.return_value = iter([
            [{'id': 'p1', 'value': 'initial'}],
        ])
        with patch.object(self.loader, '_process_update_query') as update:
            self.assertEqual(self.loader._process_query_index(self.spec), 1)
        self.assertEqual([call.args[2]['name'] for call in update.call_args_list],
                         ['first', 'unselected', 'second'])
        self.loader.opensearch.create_index.assert_called_once()
        self.loader.opensearch.bulk_upsert.assert_called_once()

    def run_load(self, indices):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'spec.yaml'
            path.write_text(yaml.safe_dump({'indices': indices}))
            self.loader.config.config['index_spec_file'] = str(path)
            self.loader.load()

    def test_unknown_query_fails_before_any_index_processing(self):
        self.loader.config.config['selected_update_queries'] = ['typo']
        with patch.object(self.loader, '_process_index') as process:
            with self.assertRaisesRegex(ValueError, 'Unknown update query names: typo'):
                self.run_load([self.spec])
            process.assert_not_called()

    def test_selection_skips_model_about_and_indices_without_matching_queries(self):
        others = [{'index_name': 'model', 'type': 'model'},
                  {'index_name': 'about', 'type': 'about_file'},
                  {**self.spec, 'index_name': 'other', 'update_queries': []}]
        with patch.object(self.loader, '_process_index', return_value=0) as process, \
                patch.object(self.loader, '_print_summary'), \
                patch.object(self.loader, '_save_query_timings'):
            self.run_load([self.spec, *others])
        process.assert_called_once_with(self.spec)

    def test_validates_query_names_within_selected_indices(self):
        self.loader.config.config['selected_indices'] = ['participants']
        self.loader.config.config['selected_update_queries'] = ['outside']
        other = {**self.spec, 'index_name': 'other',
                 'update_queries': [{'name': 'outside', 'query': 'RETURN outside'}]}
        with patch.object(self.loader, '_process_index') as process:
            with self.assertRaisesRegex(ValueError, 'Unknown update query names: outside'):
                self.run_load([self.spec, other])
            process.assert_not_called()


if __name__ == '__main__':
    unittest.main()
