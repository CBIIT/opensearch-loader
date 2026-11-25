import unittest
from collections import defaultdict
from contextvars import ContextVar
from threading import Barrier, Lock
from unittest.mock import Mock, patch
from opensearch_loader.loader import Loader
from opensearch_loader.config import Config


class IndexWorkerTests(unittest.TestCase):
    def test_run_context_reaches_index_and_nested_write_workers(self):
        from opensearch_loader.partitioned import ParallelWrites
        run_id = ContextVar('test_flow_run', default=None)
        token = run_id.set('flow-123')
        seen = []
        barrier = Barrier(2, timeout=3)
        def factory(config):
            self.assertEqual(run_id.get(), 'flow-123')
            child = Mock(query_timings={})
            def process(spec):
                barrier.wait()
                delegate = Mock()
                delegate.bulk_update.side_effect = lambda *a, **k: seen.append(run_id.get())
                writes = ParallelWrites(delegate)
                try:
                    writes.bulk_update(spec['index_name'], [{'id': '0'}, {'id': '1'}], 'id')
                finally:
                    writes.shutdown()
                return 2
            child._process_index.side_effect = process
            return child
        try:
            with patch('opensearch_loader.loader.Loader', side_effect=factory):
                self.parent()._load_indices_parallel([{'index_name': 'a'}, {'index_name': 'b'}], 2)
            self.assertEqual(seen, ['flow-123'] * 4)
        finally:
            run_id.reset(token)

    def parent(self):
        p = Loader.__new__(Loader)
        p.config = Config()
        p.config.config = {'index_spec_file': '/tmp/test-indices.yaml', 'selected_indices': [],
                           'index_workers': 4}
        p.model = None
        p.index_stats = []
        p.query_timings = defaultdict(list)
        p._print_summary = Mock()
        p._save_query_timings = Mock()
        return p

    def test_four_indexes_overlap_and_close_separate_loaders(self):
        p = self.parent()
        barrier = Barrier(4, timeout=3)
        lock = Lock()
        active = 0
        peak = 0
        children = []
        def factory(config):
            child = Mock(query_timings=defaultdict(list))
            self.assertIsNot(config, p.config)
            def process(spec):
                nonlocal active, peak
                with lock:
                    active += 1
                    peak = max(peak, active)
                barrier.wait()
                child.query_timings[spec['index_name']].append(1.0)
                with lock:
                    active -= 1
                return 10
            child._process_index.side_effect = process
            children.append(child)
            return child
        with patch('opensearch_loader.loader.Loader', side_effect=factory):
            p._load_indices_parallel([{'index_name': str(i)} for i in range(8)], 4)
        self.assertEqual(peak, 4)
        self.assertEqual([s['index_name'] for s in p.index_stats], [str(i) for i in range(8)])
        self.assertEqual(len(p.query_timings), 8)
        self.assertEqual(len(children), 8)
        for child in children:
            child.close.assert_called_once()
        p._print_summary.assert_called_once()
        p._save_query_timings.assert_called_once()

    def test_failure_preserves_other_results_and_fails_flow(self):
        p = self.parent()
        children = []
        def factory(config):
            child = Mock(query_timings={})
            def process(spec):
                if spec['index_name'] == 'bad':
                    raise ValueError('write failure')
                return 4
            child._process_index.side_effect = process
            children.append(child)
            return child
        with patch('opensearch_loader.loader.Loader', side_effect=factory):
            with self.assertRaisesRegex(RuntimeError, 'bad'):
                p._load_indices_parallel([{'index_name': 'bad'}, {'index_name': 'good'}], 2)
        self.assertTrue(p.index_stats[0]['error'])
        self.assertEqual(p.index_stats[1]['document_count'], 4)
        for child in children:
            child.close.assert_called_once()
        p._print_summary.assert_called_once()

    def test_duplicate_targets_fail_before_opening_connections(self):
        p = self.parent()
        with patch('opensearch_loader.loader.Loader') as ctor:
            with self.assertRaises(ValueError):
                p._load_indices_parallel([{'index_name': 'same'}, {'index_name': 'same'}], 4)
        ctor.assert_not_called()

    def test_all_indexes_use_pool_but_explicit_selection_and_one_worker_do_not(self):
        for selection, workers, parallel in [([], 4, True), (['a'], 4, False), ([], 1, False)]:
            with self.subTest(selection=selection, workers=workers):
                p = self.parent()
                p.config.config.update(selected_indices=selection, index_workers=workers)
                p._load_indices_parallel = Mock()
                p._process_index = Mock(return_value=1)
                with patch('opensearch_loader.loader.Path.exists', return_value=True), patch(
                        'opensearch_loader.loader.load_index_spec', return_value={'indices': [
                            {'index_name': 'a'}, {'index_name': 'b'}]}):
                    p.load()
                self.assertEqual(p._load_indices_parallel.called, parallel)
                self.assertEqual(p._process_index.call_count, 0 if parallel else (1 if selection else 2))

if __name__ == '__main__':
    unittest.main()
