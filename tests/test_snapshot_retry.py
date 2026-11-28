import unittest
from unittest.mock import Mock, patch

from opensearchpy.exceptions import RequestError
from opensearch_loader.opensearch_client import OpenSearchClient


class SnapshotRetryTests(unittest.TestCase):
    def client(self):
        client = OpenSearchClient.__new__(OpenSearchClient)
        client.client = Mock()
        client.client.indices.exists.return_value = True
        return client

    def snapshot_error(self):
        return RequestError(400, 'snapshot_in_progress_exception', {})

    def test_waits_for_snapshot_before_recreating(self):
        client = self.client()
        client.client.indices.exists.side_effect = [True, True, False]
        client.client.indices.delete.side_effect = [self.snapshot_error(), {}]
        with patch('opensearch_loader.opensearch_client.time.sleep') as sleep:
            client.create_index('test', force=True)
        sleep.assert_called_once()
        self.assertEqual(client.client.indices.delete.call_count, 2)
        client.client.indices.create.assert_called_once()
        calls = [call[0] for call in client.client.indices.method_calls]
        self.assertLess(max(i for i, name in enumerate(calls) if name == 'delete'),
                        calls.index('create'))

    def test_timeout_preserves_error_and_does_not_create(self):
        client = self.client()
        error = self.snapshot_error()
        client.client.indices.delete.side_effect = error
        with patch('opensearch_loader.opensearch_client.time.monotonic',
                   side_effect=[0, 0, 30, 60]), \
             patch('opensearch_loader.opensearch_client.time.sleep') as sleep:
            with self.assertRaises(RequestError) as raised:
                client.delete_index('test', snapshot_wait_seconds=60)
        self.assertIs(raised.exception, error)
        self.assertEqual(sleep.call_count, 2)
        client.client.indices.create.assert_not_called()

    def test_unrelated_errors_are_not_retried(self):
        client = self.client()
        client.client.indices.delete.side_effect = RequestError(400, 'other_error', {})
        with patch('opensearch_loader.opensearch_client.time.sleep') as sleep:
            with self.assertRaises(RequestError):
                client.create_index('test', force=True)
        sleep.assert_not_called()
        client.client.indices.create.assert_not_called()

    def test_absent_index_is_not_deleted(self):
        client = self.client()
        client.client.indices.exists.return_value = False
        client.delete_index('test')
        client.client.indices.delete.assert_not_called()
