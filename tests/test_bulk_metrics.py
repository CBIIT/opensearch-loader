import json
import unittest
from unittest.mock import Mock, patch
from opensearch_loader.bulk_metrics import MeasuredBulkClient


class BulkMetricsTests(unittest.TestCase):
    def test_response_and_options_preserved_and_contents_not_logged(self):
        client = Mock()
        response = {'took': 20, 'errors': True, 'items': [
            {'update': {'status': 200}}, {'update': {'status': 429}}]}
        client.bulk.return_value = response
        proxy = MeasuredBulkClient(client, 'test', 'Program', 'update')
        with patch('opensearch_loader.bulk_metrics.logger') as log:
            self.assertIs(proxy.bulk(body='private-value\n', refresh=False), response)
        client.bulk.assert_called_once_with(body='private-value\n', refresh=False)
        data = json.loads(log.info.call_args.args[1])
        self.assertEqual(data['item_statuses'], {'200': 1, '429': 1})
        self.assertEqual(data['server_took_ms'], 20)
        self.assertNotIn('private-value', str(log.mock_calls))
        self.assertIs(proxy.transport, client.transport)

    def test_failure_propagates_without_logging_response_body(self):
        client = Mock()
        client.bulk.side_effect = ValueError('sensitive response')
        with patch('opensearch_loader.bulk_metrics.logger') as log:
            with self.assertRaises(ValueError):
                MeasuredBulkClient(client, 'test', 'Q', 'index').bulk(body=b'x')
        self.assertNotIn('sensitive response', str(log.mock_calls))
        self.assertEqual(json.loads(log.info.call_args.args[1])['error_type'], 'ValueError')
