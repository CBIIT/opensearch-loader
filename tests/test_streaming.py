import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, MagicMock, patch
from opensearch_loader.streaming import queued_batches, without_pagination
from opensearch_loader.memgraph_client import MemgraphClient


class StreamingTests(unittest.TestCase):
    def test_pagination_is_removed_but_literals_are_preserved(self):
        q = "MATCH (f:file) WITH f SKIP $skip LIMIT $limit RETURN 'SKIP $skip LIMIT $limit' AS x"
        self.assertEqual(without_pagination(q), "MATCH (f:file) WITH f   RETURN 'SKIP $skip LIMIT $limit' AS x")
        for q in ['RETURN $limit', 'MATCH (f) RETURN f LIMIT 1',
                  'MATCH (f) WITH f SKIP $skip LIMIT $limit WITH f SKIP $skip LIMIT $limit RETURN f']:
            with self.assertRaises(ValueError):
                without_pagination(q)

    def test_fifo_and_producer_failure_propagation(self):
        def read():
            yield [1]
            yield [2]
            raise RuntimeError('read failed')
        it = queued_batches(read, 1)
        self.assertEqual(next(it), [1])
        self.assertEqual(next(it), [2])
        with self.assertRaisesRegex(RuntimeError, 'read failed'):
            next(it)

    def test_backpressure_and_consumer_cancellation_close_source(self):
        produced = []
        closed = threading.Event()
        def read():
            try:
                for n in range(10000):
                    produced.append(n)
                    yield [n]
            finally:
                closed.set()
        it = queued_batches(read, 2)
        self.assertEqual(next(it), [0])
        time.sleep(0.05)
        self.assertLessEqual(len(produced), 4)
        it.close()
        self.assertTrue(closed.is_set())
        self.assertFalse(any(t.name == 'memgraph-stream-producer' for t in threading.enumerate()))

    def test_bolt_batch_stream_executes_once_and_fetches_until_empty(self):
        client = MemgraphClient.__new__(MemgraphClient)
        client.connect_kwargs = {'host': 'localhost', 'port': 7687}
        result = Mock()
        result.fetch.side_effect = [[{'id': 1}, {'id': 2}], [{'id': 3}], []]
        driver = MagicMock()
        driver.__enter__.return_value = driver
        session = driver.session.return_value.__enter__.return_value
        session.run.return_value = result
        with patch('opensearch_loader.memgraph_client.GraphDatabase.driver', return_value=driver) as connect:
            rows = list(client.execute_streaming_query('MATCH (f) WITH f SKIP $skip LIMIT $limit RETURN f.id AS id', batch_size=2))
        connect.assert_called_once_with('bolt://localhost:7687', auth=None)
        driver.session.assert_called_once_with(fetch_size=2)
        self.assertEqual(rows, [[{'id': 1}, {'id': 2}], [{'id': 3}]])
        session.run.assert_called_once()
        self.assertNotIn('SKIP', session.run.call_args.args[0])
        driver.__exit__.assert_called_once()

    def test_invalid_queue_capacity(self):
        for n in [0, -1, True]:
            with self.assertRaises(ValueError):
                list(queued_batches(lambda: iter([]), n))
