import threading
import unittest
from collections import defaultdict
from unittest.mock import Mock, patch
from opensearch_loader.config import Config
from opensearch_loader.loader import Loader


class LoaderStreamingTests(unittest.TestCase):
    def make_loader(self):
        loader = Loader.__new__(Loader)
        loader.config = Config()
        loader.memgraph = Mock(last_query_time=0.25)
        loader.opensearch = Mock()
        loader.query_timings = defaultdict(list)
        loader._write_seconds = defaultdict(float)
        loader._parse_mapping = Mock(return_value={})
        loader._validate_query_fields = Mock(return_value=True)
        return loader

    def spec(self):
        return {'index_name': 'test', 'id_field': 'id', 'mapping': {'keyword': ['id']},
                'initial_query': {'query': 'MATCH (n) RETURN n.id AS id'},
                'update_queries': [{'name': 'Details', 'query': 'MATCH (n) RETURN n.id AS id'}]}

    def test_serial_writes_initial_barrier_and_query_logs(self):
        loader = self.make_loader()
        events = []
        def read(query, **kwargs):
            phase = kwargs['query_name']
            events.append(('read', phase))
            yield [{'id': 'a', 'value': 1}]
            yield [{'id': 'b', 'value': 2}]
        loader.memgraph.execute_streaming_query.side_effect = read
        loader.opensearch.bulk_upsert.side_effect = lambda *a, **k: events.append(('create', threading.get_ident()))
        loader.opensearch.bulk_update.side_effect = lambda *a, **k: events.append(('update', threading.get_ident()))
        with patch('opensearch_loader.loader.OPENSEARCH_BATCH_BYTE_LIMIT', 0), self.assertLogs('OpenSearchLoader', 'INFO') as logs:
            self.assertEqual(loader._process_query_index(self.spec()), 2)
        self.assertEqual(events, [('read', 'Initial Query'), ('create', threading.get_ident()),
                                 ('create', threading.get_ident()), ('read', 'Details'),
                                 ('update', threading.get_ident()), ('update', threading.get_ident())])
        self.assertEqual(sum('QUERY_METRICS status=complete' in line for line in logs.output), 2)
        self.assertTrue(all('read_fetch_s=' in line and 'write_s=' in line for line in logs.output if 'QUERY_METRICS' in line))
        loader.memgraph.execute_paginated_query.assert_not_called()

    def test_write_failure_closes_reader_and_blocks_updates(self):
        loader = self.make_loader()
        closed = []
        def read(*a, **k):
            try:
                yield [{'id': 'a'}]
                yield [{'id': 'b'}]
            finally:
                closed.append(True)
        loader.memgraph.execute_streaming_query.side_effect = read
        loader.opensearch.bulk_upsert.side_effect = RuntimeError('write failed')
        with patch('opensearch_loader.loader.OPENSEARCH_BATCH_BYTE_LIMIT', 0), self.assertLogs('OpenSearchLoader', 'INFO') as logs:
            with self.assertRaisesRegex(RuntimeError, 'write failed'):
                loader._process_query_index(self.spec())
        self.assertEqual(closed, [True])
        loader.opensearch.bulk_update.assert_not_called()
        self.assertTrue(any('QUERY_METRICS status=failed' in line for line in logs.output))

    def test_skip_mode_can_be_selected(self):
        loader = self.make_loader()
        loader.config.config['streaming'] = False
        loader._query_batches('q', {}, 42, query_name='test')
        loader.memgraph.execute_paginated_query.assert_called_once_with('q', parameters={}, page_size=42, query_name='test')
        loader.memgraph.execute_streaming_query.assert_not_called()

    def test_queue_capacity_one_env_stays_integer(self):
        with patch.dict('os.environ', {'OS_LOADER_STREAM_QUEUE_BATCHES': '1', 'OS_LOADER_STREAM_BATCH_SIZE': '1000'}):
            cfg = Config()
        self.assertIs(type(cfg.get('stream_queue_batches')), int)
        self.assertEqual(cfg.get('stream_queue_batches'), 1)
