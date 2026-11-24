import unittest
from collections import defaultdict
from threading import Barrier, Event, Lock
from unittest.mock import Mock

from opensearch_loader.partitioned import ParallelWrites, load_partitioned, partition_for_id
from opensearch_loader.loader import Loader

FILE_QUERY = 'MATCH (f:file) WITH DISTINCT f SKIP $skip LIMIT $limit RETURN f.file_id AS file_id'
STUDY_QUERY = 'MATCH (s:study) WITH s SKIP $skip LIMIT $limit RETURN s.id AS file_id'


class PartitionTests(unittest.TestCase):
    def test_stable_disjoint_numeric_and_uuid_ids(self):
        for number in range(100):
            self.assertEqual(partition_for_id(str(number)), number % 4)
        for digit in '0123456789abcdef':
            self.assertEqual(partition_for_id('ABC-123' + digit), int(digit, 16) % 4)
        with self.assertRaises(ValueError):
            partition_for_id(None)

    def parent(self):
        loader = Loader.__new__(Loader)
        loader.config = Mock()
        loader.config.get.side_effect = lambda key, default=None: default
        loader.config.get_test_mode.return_value = False
        loader.config.get_allow_index_creation.return_value = True
        loader.config.get_clear_existing_indices.return_value = False
        loader.memgraph = Mock(last_query_time=1)
        loader.opensearch = Mock()
        api = loader.opensearch.client.indices
        api.exists.return_value = True
        api.exists_alias.return_value = True
        api.get_alias.return_value = {'old-even': {}, 'old-odd': {}}
        loader.query_timings = defaultdict(list)
        return loader

    def spec(self):
        return dict(index_name='dashboard_file', id_field='file_id',
                    mapping={'keyword': ['file_id']},
                    initial_query={'query': FILE_QUERY},
                    update_queries=[{'name': 'Studies', 'query': STUDY_QUERY},
                                    {'name': 'Later', 'query': 'later query'}])

    def test_one_reader_concurrent_writes_and_ordered_updates(self):
        loader = self.parent()
        original = loader.opensearch
        overlap = Barrier(4, timeout=3)
        lock = Lock()
        seen = defaultdict(list)
        targets = set()
        def read(query, **kwargs):
            if query == STUDY_QUERY:
                self.assertEqual(len(seen['initial']), 10)
            if query == 'later query':
                self.assertEqual(len(seen['Studies']), 10)
            yield [{'file_id': str(i)} for i in range(5)]
            yield [{'file_id': str(i)} for i in range(5, 10)]
        loader.memgraph.execute_paginated_query.side_effect = read
        def write(name, docs, id_field, query_name):
            overlap.wait()  # All four workers must be active, not sequential.
            self.assertEqual(len({partition_for_id(d[id_field]) for d in docs}), 1)
            with lock:
                targets.add(name)
                seen['initial' if query_name == 'Initial Query' else query_name].extend(
                    d[id_field] for d in docs)
        original.bulk_upsert.side_effect = write
        original.bulk_update.side_effect = write
        self.assertEqual(load_partitioned(loader, self.spec()), 10)
        calls = loader.memgraph.execute_paginated_query.call_args_list
        self.assertEqual([c.args[0] for c in calls], [FILE_QUERY, STUDY_QUERY, 'later query'])
        self.assertEqual(len(targets), 1)
        for ids in seen.values():
            self.assertEqual(sorted(ids), [str(i) for i in range(10)])
        original.create_index.assert_called_once()
        self.assertEqual(original.create_index.call_args.kwargs['number_of_shards'], 2)
        self.assertEqual(original.refresh_index.call_count, 2)
        self.assertEqual(targets, {'dashboard_file'})
        self.assertEqual(original.create_index.call_args.args[0], 'dashboard_file')
        original.client.indices.update_aliases.assert_not_called()
        original.client.indices.get_alias.assert_not_called()
        self.assertIs(loader.opensearch, original)
        self.assertFalse(loader.partition_mode)

    def test_failure_drains_other_worker_before_raising(self):
        client = Mock()
        overlap = Barrier(2, timeout=3)
        completed = Event()
        def write(name, docs, *args, **kwargs):
            overlap.wait()
            if docs[0]['file_id'] == '0':
                raise RuntimeError('failure')
            completed.set()
        client.bulk_upsert.side_effect = write
        writes = ParallelWrites(client)
        try:
            with self.assertRaisesRegex(RuntimeError, 'failure'):
                writes.bulk_upsert('index', [{'file_id': '0'}, {'file_id': '1'}], 'file_id')
            self.assertTrue(completed.is_set())
        finally:
            writes.shutdown()

    def test_failed_initial_write_retries_but_never_reads_updates_or_publishes(self):
        loader = self.parent()
        original = loader.opensearch
        loader.memgraph.execute_paginated_query.side_effect = lambda *a, **k: iter([
            [{'file_id': '0'}, {'file_id': '1'}]])
        original.bulk_upsert.side_effect = RuntimeError('write failed')
        with self.assertRaisesRegex(RuntimeError, 'write failed'):
            load_partitioned(loader, self.spec())
        self.assertEqual(original.bulk_upsert.call_count, 4)  # Two writers, two attempts.
        loader.memgraph.execute_paginated_query.assert_called_once()
        original.bulk_update.assert_not_called()
        original.client.indices.update_aliases.assert_not_called()
        self.assertIs(loader.opensearch, original)

    def test_same_id_order_and_empty_partition(self):
        client = Mock()
        writes = ParallelWrites(client)
        try:
            rows = [{'file_id': '2', 'v': 1}, {'file_id': '2', 'v': 2}]
            writes.bulk_update('index', rows, 'file_id')
            writes.bulk_update('index', [{'file_id': '2', 'v': 3}], 'file_id')
            self.assertEqual([c.args[1] for c in client.bulk_update.call_args_list],
                             [rows, [{'file_id': '2', 'v': 3}]])
        finally:
            writes.shutdown()

    def test_update_validation_failure_never_publishes(self):
        loader = self.parent()
        original = loader.opensearch
        loader.memgraph.execute_paginated_query.side_effect = [
            iter([[{'file_id': '0'}]]), iter([[{'file_id': '0', 'unknown': 'value'}]])]
        with self.assertRaises(ValueError):
            load_partitioned(loader, self.spec())
        original.client.indices.update_aliases.assert_not_called()
        self.assertIs(loader.opensearch, original)

    def test_initial_retry_does_not_hide_failure_or_reset_count(self):
        loader = self.parent()
        args = ('test', 'Initial Query', 1, [{'file_id': '1'}], 'file_id', FILE_QUERY, 10)
        self.assertEqual(loader._upsert_documents_with_retry(*args)[0], 11)
        loader.opensearch.bulk_upsert.side_effect = RuntimeError('failure')
        with self.assertRaises(RuntimeError):
            loader._upsert_documents_with_retry(*args)


if __name__ == '__main__':
    unittest.main()
