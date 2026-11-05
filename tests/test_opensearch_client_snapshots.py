"""Tests for OpenSearch authentication and snapshot-aware index deletion."""

from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import MagicMock, call, patch

from opensearchpy import RequestError

from opensearch_loader.opensearch_client import OpenSearchClient, _assumed_role_credentials


class OpenSearchAuthenticationTests(unittest.TestCase):
    def test_assumed_credentials_can_refresh(self):
        role = 'arn:aws:iam::123456789012:role/operations'
        expiration = datetime.now(timezone.utc) + timedelta(hours=1)
        with patch('opensearch_loader.opensearch_client.boto3.client') as client:
            client.return_value.assume_role.side_effect = [
                {'Credentials': {'AccessKeyId': 'first-key', 'SecretAccessKey': 'first-secret',
                                 'SessionToken': 'first-token', 'Expiration': expiration}},
                {'Credentials': {'AccessKeyId': 'next-key', 'SecretAccessKey': 'next-secret',
                                 'SessionToken': 'next-token', 'Expiration': expiration}},
            ]
            credentials = _assumed_role_credentials(role)
            self.assertEqual(credentials.get_frozen_credentials().access_key, 'first-key')
            credentials._expiry_time = datetime.now(timezone.utc) - timedelta(seconds=1)
            refreshed = credentials.get_frozen_credentials()
        client.assert_called_once_with('sts')
        self.assertEqual(client.return_value.assume_role.call_args_list, [
            call(RoleArn=role, RoleSessionName='PrefectOpenSearchLoader'),
            call(RoleArn=role, RoleSessionName='PrefectOpenSearchLoader'),
        ])
        self.assertEqual(refreshed.access_key, 'next-key')
        self.assertEqual(refreshed.secret_key, 'next-secret')
        self.assertEqual(refreshed.token, 'next-token')

    def test_aws_signing_uses_selected_credentials(self):
        for role in [None, 'arn:aws:iam::123456789012:role/operations']:
            with self.subTest(role=role), \
                    patch('opensearch_loader.opensearch_client.OpenSearch'), \
                    patch('opensearch_loader.opensearch_client.AWS4Auth') as auth, \
                    patch('opensearch_loader.opensearch_client.Session') as session, \
                    patch('opensearch_loader.opensearch_client._assumed_role_credentials') as assume:
                OpenSearchClient('https://search-example.us-east-1.es.amazonaws.com',
                                 operations_role_arn=role)
                credentials = (assume.return_value if role
                               else session.return_value.get_credentials.return_value)
                auth.assert_called_once_with(refreshable_credentials=credentials,
                                             region='us-east-1', service='es')
                if role:
                    assume.assert_called_once_with(role)
                    session.assert_not_called()
                else:
                    assume.assert_not_called()

    def test_local_opensearch_does_not_assume_role(self):
        with patch('opensearch_loader.opensearch_client.OpenSearch'), \
                patch('opensearch_loader.opensearch_client._assumed_role_credentials') as assume:
            OpenSearchClient('http://localhost:9200', operations_role_arn='unused-role')
        assume.assert_not_called()


class SnapshotAwareIndexDeletionTests(unittest.TestCase):
    def setUp(self):
        self.opensearch = object.__new__(OpenSearchClient)
        self.opensearch.client = MagicMock()
        self.opensearch.snapshot_poll_interval_seconds = 30
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

        sleep.assert_called_once_with(30)
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

        sleep.assert_called_once_with(30)
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
