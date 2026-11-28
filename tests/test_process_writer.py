import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock
from opensearch_loader.process_writer import ProcessWrites

FIXTURE=Path(__file__).parent/'fixtures/write_consumer.py'

class SubprocessWriterTests(unittest.TestCase):
    def make(self,config,**kw):
        return ProcessWrites(Mock(),config,workers=2,_command=[sys.executable,str(FIXTURE.resolve())],**kw)

    def test_order_errors_retry_and_six_thread_startup(self):
        def one(_):
            with tempfile.TemporaryDirectory() as d:
                p=Path(d)/'writes';w=self.make({'path':str(p)})
                try:
                    w.bulk_upsert('test',[{'id':'1'}],'id')
                    w.bulk_update('test',[{'id':'1','v':2}],'id')
                    with self.assertRaisesRegex(RuntimeError,'deliberate write failure'):
                        w.bulk_update('test',[{'id':'1','fail':True}],'id')
                    w.bulk_update('test',[{'id':'1','v':3}],'id')
                    w.refresh_index('test');w.delegate.refresh_index.assert_called_once_with('test')
                    rows=[json.loads(s) for s in p.read_text().splitlines()]
                    self.assertEqual([x[1] for x in rows],['create','update','update'])
                    self.assertTrue(all(x[0]==w.process.pid!=os.getpid() for x in rows))
                finally:w.shutdown()
                self.assertIsNotNone(w.process.poll())
        with ThreadPoolExecutor(max_workers=6) as pool:list(pool.map(one,range(6)))

    def test_child_exit(self):
        with tempfile.TemporaryDirectory() as d:
            w=self.make({'path':d+'/writes'})
            try:
                with self.assertRaisesRegex(RuntimeError,'exitcode=7'):
                    w.bulk_update('test',[{'id':'1','crash':True}],'id')
                self.assertTrue(w.closed)
            finally:w.shutdown()

    def test_timeout_kills_child_before_retry(self):
        with tempfile.TemporaryDirectory() as d:
            w=self.make({'path':d+'/writes'},request_timeout=.2)
            try:
                with self.assertRaisesRegex(RuntimeError,'transport failed'):
                    w.bulk_update('test',[{'id':'1','hang':True}],'id')
                self.assertIsNotNone(w.process.poll())
                with self.assertRaisesRegex(RuntimeError,'closed'):
                    w.bulk_update('test',[{'id':'1'}],'id')
                self.assertFalse(Path(d+'/writes').exists())
            finally:w.shutdown()

    def test_initialization_error(self):
        with self.assertRaisesRegex(RuntimeError,'deliberate initialization failure'):
            self.make({'fail_init':True})

    def test_bootstrap_error_stderr_visible(self):
        with self.assertRaisesRegex(RuntimeError,'No module named'):
            ProcessWrites(Mock(),{},_command=[sys.executable,'-m','nonexistent_writer_bootstrap'])

    def test_startup_timeout(self):
        with self.assertRaisesRegex(RuntimeError,'Writer startup failed'):
            ProcessWrites(Mock(),{},startup_timeout=.2,_command=[sys.executable,'-c','import time;time.sleep(30)'])

    def test_loader_selects_subprocess_and_restores_client_on_failure(self):
        from unittest.mock import patch
        from opensearch_loader.partitioned import load_partitioned
        loader=Mock()
        original=loader.opensearch
        loader.partition_mode=False
        config={'separate_write_process':True,'write_workers':4}
        loader.config.get.side_effect=lambda key,default=None:config.get(key,default)
        loader.config.get_opensearch_config.return_value={'host':'localhost','password':'secret'}
        loader._process_index_serial.side_effect=ValueError('write failure')
        with patch('opensearch_loader.process_writer.ProcessWrites') as constructor:
            with self.assertRaisesRegex(ValueError,'write failure'):
                load_partitioned(loader,{'index_name':'test'})
            constructor.assert_called_once_with(original,{'host':'localhost','password':'secret'},4)
            constructor.return_value.shutdown.assert_called_once_with()
        self.assertIs(loader.opensearch,original)
        self.assertFalse(loader.partition_mode)
