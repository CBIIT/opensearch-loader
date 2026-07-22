"""Tests for snapshot-aware OpenSearch index deletion."""

import unittest
from unittest.mock import MagicMock, call, patch

from opensearchpy import RequestError

from opensearch_loader.opensearch_client import OpenSearchClient


class SnapshotAwareIndexDeletionTests(unittest.TestCase):
    def setUp(self):
        self.opensearch = object.__new__(OpenSearchClient)
        self.opensearch.client = MagicMock()
        self.opensearch.snapshot_poll_interval_seconds = 10
        self.opensearch.snapshot_wait_timeout_seconds = 3600

    @patch('opensearch_loader.opensearch_client.time.sleep')
    def test_waits_for_active_snapshot_before_deleting(self, sleep):
        self.opensearch.client.indices.exists.return_value = True
        self.opensearch.client.snapshot.status.side_effect = [
            {
                'snapshots': [
                    {
                        'repository': 'cs-automated',
                        'snapshot': '2026-07-22t13-27-16',
                        'state': 'IN_PROGRESS',
                    }
                ]
            },
            {'snapshots': []},
        ]

        self.opensearch.delete_index('diagnoses_table')

        sleep.assert_called_once_with(10)
        self.assertEqual(self.opensearch.client.snapshot.status.call_count, 2)
        self.opensearch.client.indices.delete.assert_called_once_with(
            index='diagnoses_table'
        )
        self.assertEqual(
            self.opensearch.client.mock_calls,
            [
                call.indices.exists(index='diagnoses_table'),
                call.snapshot.status(),
                call.snapshot.status(),
                call.indices.exists(index='diagnoses_table'),
                call.indices.delete(index='diagnoses_table'),
            ],
        )

    @patch('opensearch_loader.opensearch_client.time.sleep')
    def test_deletes_immediately_when_no_snapshot_is_active(self, sleep):
        self.opensearch.client.indices.exists.return_value = True
        self.opensearch.client.snapshot.status.return_value = {'snapshots': []}

        self.opensearch.delete_index('files_table')

        sleep.assert_not_called()
        self.opensearch.client.indices.delete.assert_called_once_with(
            index='files_table'
        )

    @patch('opensearch_loader.opensearch_client.time.sleep')
    def test_retries_if_snapshot_starts_between_check_and_delete(self, sleep):
        self.opensearch.client.indices.exists.return_value = True
        self.opensearch.client.snapshot.status.side_effect = [
            {'snapshots': []},
            {
                'snapshots': [
                    {
                        'repository': 'cs-automated',
                        'snapshot': '2026-07-22t13-27-16',
                        'state': 'IN_PROGRESS',
                    }
                ]
            },
            {'snapshots': []},
        ]
        self.opensearch.client.indices.delete.side_effect = [
            RequestError(
                400,
                'snapshot_in_progress_exception',
                {'error': 'Cannot delete indices that are being snapshotted'},
            ),
            {'acknowledged': True},
        ]

        self.opensearch.delete_index('samples_table')

        sleep.assert_called_once_with(10)
        self.assertEqual(self.opensearch.client.indices.delete.call_count, 2)

    def test_does_not_retry_unrelated_delete_error(self):
        self.opensearch.client.indices.exists.return_value = True
        self.opensearch.client.snapshot.status.return_value = {'snapshots': []}
        error = RequestError(400, 'illegal_argument_exception', {'error': 'bad request'})
        self.opensearch.client.indices.delete.side_effect = error

        with self.assertRaises(RequestError) as raised:
            self.opensearch.delete_index('studies_table')

        self.assertIs(raised.exception, error)
        self.opensearch.client.indices.delete.assert_called_once()

    @patch('opensearch_loader.opensearch_client.time.sleep')
    @patch('opensearch_loader.opensearch_client.time.monotonic')
    def test_times_out_when_snapshot_does_not_finish(self, monotonic, sleep):
        self.opensearch.snapshot_poll_interval_seconds = 10
        self.opensearch.snapshot_wait_timeout_seconds = 15
        self.opensearch.client.snapshot.status.return_value = {
            'snapshots': [
                {
                    'repository': 'cs-automated',
                    'snapshot': 'stuck-snapshot',
                    'state': 'IN_PROGRESS',
                }
            ]
        }
        monotonic.side_effect = [100, 100, 115]

        with self.assertRaisesRegex(
            TimeoutError,
            'cs-automated/stuck-snapshot',
        ):
            self.opensearch.wait_for_snapshots_to_finish('genetic_analyses_table')

        sleep.assert_called_once_with(10)


if __name__ == '__main__':
    unittest.main()
