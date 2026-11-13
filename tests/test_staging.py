import json
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from opensearch_loader.staging import DocumentStore, load_staged_index


class StagingTests(unittest.TestCase):
    def test_merge_order_null_arrays_and_missing_documents(self):
        with tempfile.TemporaryDirectory() as root:
            store = DocumentStore(Path(root) / 'test.sqlite')
            try:
                store.add_page([{'id': 'a', 'obsolete': 1}, {'id': 'a', 'obj': {'x': 1, 'y': 2},
                                'array': [1, 2], 'nullable': 1}, {'id': 'b'}], 'id')
                affected, invalid = store.add_page([
                    {'id': 'a', 'obj': {'x': 3}, 'array': [9], 'nullable': None},
                    {'id': 'a', 'obj': {'z': 4}}, {'id': 'missing', 'v': 1}, {'v': 2}],
                    'id', update=True)
                self.assertEqual((affected, invalid), (2, 1))
                docs = {d['id']: d for batch in store.batches(count=1) for d in batch}
                self.assertEqual(docs['a'], {'id': 'a', 'obj': {'x': 3, 'y': 2, 'z': 4},
                                            'array': [9], 'nullable': None})
                self.assertEqual(store.count(), 2)
                self.assertEqual(len(list(store.batches(max_bytes=1))), 2)
            finally:
                store.close()

    def loader(self, fail=False):
        loader = SimpleNamespace(config=Mock(), memgraph=Mock(), opensearch=Mock(),
                                 query_timings=defaultdict(list), _validate_query_fields=Mock(return_value=True))
        loader.config.get_clear_existing_indices.return_value = False
        loader.config.get_allow_index_creation.return_value = True
        loader.config.get_test_mode.return_value = False
        loader.opensearch.client.transport.serializer.dumps = json.dumps
        loader.memgraph.last_query_time = 0.01
        def pages(query, **kwargs):
            loader.opensearch.bulk_upsert.assert_not_called()
            loader.opensearch.create_index.assert_not_called()
            if query == 'initial':
                yield [{'id': 'a', 'file': 'one'}, {'id': 'b', 'file': 'two'}]
            else:
                yield [{'id': 'a', 'program': ['P']}]
                if fail:
                    raise RuntimeError('query failed')
                yield [{'id': 'b', 'program': ['Q']}]
        loader.memgraph.execute_paginated_query.side_effect = pages
        return loader

    def spec(self):
        return {'index_name': 'dashboard_file', 'id_field': 'id',
                'initial_query': {'query': 'initial'},
                'update_queries': [{'name': 'Program', 'query': 'update'}]}

    def test_all_reads_finish_before_single_final_write_and_cleanup(self):
        loader = self.loader()
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'stage'
            path.mkdir()
            with patch('opensearch_loader.staging.tempfile.mkdtemp', return_value=str(path)):
                self.assertEqual(load_staged_index(loader, self.spec(), {}), 2)
            self.assertFalse(path.exists())
        loader.opensearch.bulk_update.assert_not_called()
        loader.opensearch.bulk_upsert.assert_called_once()
        self.assertEqual(loader.opensearch.bulk_upsert.call_args.args[1], [
            {'id': 'a', 'file': 'one', 'program': ['P']},
            {'id': 'b', 'file': 'two', 'program': ['Q']}])

    def test_read_failure_preserves_remote_and_retains_local_file(self):
        loader = self.loader(fail=True)
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'stage'
            path.mkdir()
            with patch('opensearch_loader.staging.tempfile.mkdtemp', return_value=str(path)):
                with self.assertRaisesRegex(RuntimeError, 'query failed'):
                    load_staged_index(loader, self.spec(), {})
            self.assertTrue((path / 'documents.sqlite').exists())
        loader.opensearch.create_index.assert_not_called()
        loader.opensearch.delete_index.assert_not_called()
        loader.opensearch.bulk_upsert.assert_not_called()

    def test_write_failure_waits_for_retry_and_retains_staging(self):
        loader = self.loader()
        loader.opensearch.bulk_upsert.side_effect = RuntimeError('write failed')
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'stage'
            path.mkdir()
            with patch('opensearch_loader.staging.tempfile.mkdtemp', return_value=str(path)):
                with self.assertRaisesRegex(RuntimeError, 'write failed'):
                    load_staged_index(loader, self.spec(), {})
            self.assertTrue((path / 'documents.sqlite').exists())
        self.assertEqual(loader.opensearch.bulk_upsert.call_count, 2)
        loader.opensearch.refresh_index.assert_not_called()
