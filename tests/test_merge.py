import os
import unittest
from datetime import date, datetime

from opensearch_loader.merge import MergeStore, deep_merge


class DeepMergeTests(unittest.TestCase):
    def test_objects_merge_and_other_values_replace(self):
        doc = {'a': 1, 'obj': {'x': 1, 'y': 2}, 'tags': ['a', 'b']}
        deep_merge(doc, {'obj': {'y': 3, 'z': 4}, 'tags': ['c'], 'b': None})
        self.assertEqual(doc, {'a': 1, 'obj': {'x': 1, 'y': 3, 'z': 4}, 'tags': ['c'], 'b': None})

    def test_object_replaces_scalar(self):
        self.assertEqual(deep_merge({'a': 1}, {'a': {'b': 2}}), {'a': {'b': 2}})


class MergeStoreTests(unittest.TestCase):
    def setUp(self):
        self.store = MergeStore(name='test')

    def tearDown(self):
        self.store.close()

    def docs(self):
        import json
        return {i: json.loads(d) for i, d in self.store.documents()}

    def test_initial_skips_missing_ids_and_last_row_wins(self):
        stored, skipped = self.store.put_initial(
            [{'id': 'a', 'v': 1}, {'id': None, 'v': 2}, {'id': '', 'v': 3}, {'id': 'a', 'v': 4}], 'id')
        self.assertEqual((stored, skipped), (2, 2))
        self.assertEqual(self.docs(), {'a': {'id': 'a', 'v': 4}})

    def test_update_merges_known_ids_and_drops_others(self):
        self.store.put_initial([{'id': 'a', 'v': 1, 'o': {'x': 1}}, {'id': 'b', 'v': 2}], 'id')
        merged, dropped = self.store.apply_update(
            [{'id': 'a', 'o': {'y': 2}}, {'id': 'zzz', 'v': 9}, {'id': None, 'v': 9}, {'id': 'b'}], 'id')
        self.assertEqual((merged, dropped), (1, 3))
        self.assertEqual(self.docs(), {'a': {'id': 'a', 'v': 1, 'o': {'x': 1, 'y': 2}}, 'b': {'id': 'b', 'v': 2}})

    def test_update_does_not_change_id_field_and_later_pass_wins(self):
        self.store.put_initial([{'id': 1, 'v': 'init'}], 'id')
        self.store.apply_update([{'id': 1, 'v': 'pass1', 'w': 1}], 'id')
        self.store.apply_update([{'id': 1, 'v': 'pass2'}], 'id')
        self.assertEqual(self.docs(), {'1': {'id': 1, 'v': 'pass2', 'w': 1}})

    def test_dates_serialize_like_opensearch_py(self):
        self.store.put_initial([{'id': 'a', 'd': date(2026, 1, 2), 't': datetime(2026, 1, 2, 3, 4, 5)}], 'id')
        self.assertEqual(self.docs()['a'], {'id': 'a', 'd': '2026-01-02', 't': '2026-01-02T03:04:05'})

    def test_file_removed_on_close(self):
        store = MergeStore(name='gone')
        path = store.path
        store.close()
        self.assertFalse(os.path.exists(path))


if __name__ == '__main__':
    unittest.main()
