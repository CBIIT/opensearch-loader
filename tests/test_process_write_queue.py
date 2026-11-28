import threading
import unittest
from unittest.mock import Mock, patch
from opensearch_loader.process_pipeline import QueuedProcessWrites

class BlockingWorker:
    gate = None
    entered = None
    calls = None
    def __init__(self,*args):pass
    def call(self,method,index,docs,key,**kwargs):
        self.entered.set()
        self.gate.wait(3)
        self.calls.append((method,docs))
        if any(d.get('fail') for d in docs):raise RuntimeError('write rejected')
    def close(self):pass

class QueueTests(unittest.TestCase):
    def setUp(self):
        BlockingWorker.gate=threading.Event();BlockingWorker.entered=threading.Event();BlockingWorker.calls=[]
        self.patch=patch('opensearch_loader.process_pipeline.Worker',BlockingWorker);self.patch.start()
        self.delegate=Mock();self.writer=QueuedProcessWrites(self.delegate,{},workers=1,queue_batches=1)
    def tearDown(self):
        BlockingWorker.gate.set()
        try:self.writer.close()
        except RuntimeError:pass
        self.patch.stop()
    def test_async_backpressure_order_and_refresh_barrier(self):
        self.writer.bulk_upsert('x',[{'id':'1'}],'id')
        self.assertTrue(BlockingWorker.entered.wait(1))
        submitted=threading.Event()
        def second():
            self.writer.bulk_update('x',[{'id':'1','value':2}],'id');submitted.set()
        thread=threading.Thread(target=second);thread.start()
        self.assertFalse(submitted.wait(.05))
        BlockingWorker.gate.set();thread.join(2);self.assertTrue(submitted.is_set())
        self.writer.refresh_index('x')
        self.assertEqual([c[0] for c in BlockingWorker.calls],['bulk_upsert','bulk_update'])
        self.delegate.refresh_index.assert_called_once_with('x')
        self.assertFalse(self.writer.pending)
    def test_failure_cannot_be_hidden_by_retry_or_refresh(self):
        BlockingWorker.gate.set()
        self.writer.bulk_update('x',[{'id':'1','fail':True}],'id')
        with self.assertRaisesRegex(RuntimeError,'Queued process write failed'):self.writer.refresh_index('x')
        self.delegate.refresh_index.assert_not_called()
        with self.assertRaises(RuntimeError):self.writer.bulk_update('x',[{'id':'1'}],'id')
    def test_message_size_is_bounded(self):
        with self.assertRaisesRegex(ValueError,'5 MiB'):
            self.writer.bulk_upsert('x',[{'id':'1','value':'x'*(6*1024*1024)}],'id')

if __name__=='__main__':unittest.main()
