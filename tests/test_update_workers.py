import threading
import unittest
import zlib
from unittest.mock import patch
from opensearch_loader.opensearch_client import OpenSearchClient


class UpdateWorkerTests(unittest.TestCase):
    def test_parallel_lanes_preserve_id_order_and_join(self):
        client = OpenSearchClient.__new__(OpenSearchClient)
        ids = [next(str(i) for i in range(100) if zlib.crc32(str(i).encode()) % 2 == n)
               for n in range(2)]
        barrier = threading.Barrier(2)
        seen = {key: [] for key in ids}
        threads = {key: set() for key in ids}
        def write(index, rows, field, query_name=None):
            key = str(rows[0][field])
            if not seen[key]:
                barrier.wait(timeout=2)  # Both workers must actually run concurrently.
            for row in rows:
                seen[key].append(row['value'])
                threads[key].add(threading.get_ident())
        client._process_update_batch = write
        rows = [{'id': key, 'value': n} for n in range(7) for key in ids]
        with patch('opensearch_loader.opensearch_client.BULK_CHUNK_SIZE', 2):
            client.bulk_update('test', rows, 'id', 'Details')
        self.assertEqual(seen, {key: list(range(7)) for key in ids})
        self.assertTrue(threads[ids[0]].isdisjoint(threads[ids[1]]))
        self.assertFalse(any(t.name.startswith('opensearch-update') for t in threading.enumerate()))

    def test_failure_propagates_after_workers_finish(self):
        client = OpenSearchClient.__new__(OpenSearchClient)
        barrier = threading.Barrier(2)
        completed = threading.Event()
        def write(index, rows, field, query_name=None):
            barrier.wait(timeout=2)
            if zlib.crc32(str(rows[0][field]).encode()) % 2 == 0:
                raise RuntimeError('write failed')
            completed.set()
        client._process_update_batch = write
        ids = [next(str(i) for i in range(100) if zlib.crc32(str(i).encode()) % 2 == n)
               for n in range(2)]
        with self.assertRaisesRegex(RuntimeError, 'write failed'):
            client.bulk_update('test', [{'id': key} for key in ids], 'id')
        self.assertTrue(completed.is_set())
        self.assertFalse(any(t.name.startswith('opensearch-update') for t in threading.enumerate()))
