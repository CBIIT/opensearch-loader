import threading
import time
import unittest
from unittest.mock import Mock, patch
from opensearch_loader.process_pipeline import ProcessWrites, QueryPool, positive_workers


class FakeWorker:
    def __init__(self, *args):
        self.process = Mock(pid=id(self))
        self.calls = []
        self.closed = False

    def call(self, method, *args, **kwargs):
        if method == 'execute_query':
            time.sleep(.02)
            return []
        self.calls.append((method, args))
        if any(d.get('fail') for d in args[1]):
            raise RuntimeError('failed lane')
        time.sleep(.02)

    def close(self):
        self.closed = True


class ProcessPipelineTests(unittest.TestCase):
    @patch('opensearch_loader.process_pipeline.Worker', FakeWorker)
    def test_id_order_barrier_and_failure_drains_other_lanes(self):
        writer = ProcessWrites(Mock(), {}, 4)
        try:
            writer.bulk_upsert('a', [{'id':str(i)} for i in range(8)], 'id')
            writer.bulk_update('a', [{'id':str(i), 'v':1} for i in range(8)], 'id')
            for lane, child in enumerate(writer.children):
                self.assertEqual([m for m, _ in child.calls], ['bulk_upsert','bulk_update'])
                self.assertEqual([d['id'] for d in child.calls[0][1][1]], [str(lane),str(lane+4)])
            with self.assertRaisesRegex(RuntimeError, 'failed lane'):
                writer.bulk_update('a', [{'id':'0','fail':True},{'id':'1'}], 'id')
            # A later retry is submitted only after ALL previous lanes completed.
            self.assertEqual(len(writer.children[1].calls), 3)
        finally:
            writer.close()
        self.assertTrue(all(c.closed for c in writer.children))

    @patch('opensearch_loader.process_pipeline.Worker', FakeWorker)
    def test_query_pool_shared_and_closed(self):
        pool=QueryPool({},4)
        try:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=8) as threads:
                results=list(threads.map(lambda _:pool.execute('RETURN 1',{}),range(8)))
            self.assertEqual(results,[[]]*8)
            self.assertEqual(pool.available.qsize(),4)
        finally:pool.close()
        self.assertTrue(all(c.closed for c in pool.workers))

    def test_counts(self):
        for value in [0,-1,True,1.5]:
            with self.assertRaises(ValueError):positive_workers(value)
