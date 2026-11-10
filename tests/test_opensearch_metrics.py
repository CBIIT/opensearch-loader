import threading
import json
import unittest
from unittest.mock import Mock
from opensearch_loader.opensearch_metrics import opensearch_monitor, log_index_settings


class MetricsTests(unittest.TestCase):
    def test_settings_defaults_override_and_allowlist(self):
        client, logger = Mock(), Mock()
        client.transport.perform_request.return_value = {'dashboard_file': {
            'settings': {'index.refresh_interval': '30s', 'private': 'secret'},
            'defaults': {'index.refresh_interval': '1s', 'index.translog.durability': 'request'}}}
        log_index_settings(client, logger, 'dashboard_file')
        settings = json.loads(logger.info.call_args.args[1])['settings']
        self.assertEqual(settings['index.refresh_interval'], {'value': '30s', 'source': 'settings'})
        self.assertEqual(settings['index.translog.durability'], {'value': 'request', 'source': 'defaults'})
        self.assertEqual(settings['index.number_of_replicas']['source'], 'unavailable')
        self.assertNotIn('secret', str(logger.mock_calls))
        self.assertEqual(client.transport.perform_request.call_args.args,
                         ('GET', '/dashboard_file/_settings'))

    def test_settings_failure_is_nonfatal_and_sanitized(self):
        client, logger = Mock(), Mock()
        client.transport.perform_request.side_effect = RuntimeError('secret response')
        log_index_settings(client, logger, 'dashboard_file')
        logger.warning.assert_called_once()
        self.assertNotIn('secret response', str(logger.mock_calls))

    def test_sampling_and_cleanup_on_load_failure(self):
        client, logger = Mock(), Mock()
        client.transport.perform_request.return_value = {'nodes': {}}
        sampled = threading.Event()
        logger.info.side_effect = lambda *args: sampled.set() if '"event": "sample"' in args[-1] else None
        with self.assertRaisesRegex(ValueError, 'load failed'):
            with opensearch_monitor(client, logger, interval=.01):
                self.assertTrue(sampled.wait(1))
                raise ValueError('load failed')
        self.assertFalse(any(t.name == 'opensearch-metrics' for t in threading.enumerate()))
        self.assertIn('"event": "end"', logger.info.call_args.args[-1])
        for call in client.transport.perform_request.call_args_list:
            self.assertEqual(call.args[0], 'GET')
            self.assertEqual(call.kwargs['timeout'], 7)

    def test_denied_does_not_abort_load_or_repeat_requests_or_log_secret(self):
        client, logger = Mock(), Mock()
        error = RuntimeError('secret response body')
        error.status_code = 403
        client.transport.perform_request.side_effect = error
        with opensearch_monitor(client, logger):
            pass
        self.assertEqual(client.transport.perform_request.call_count, 2)
        self.assertNotIn('secret response body', str(logger.mock_calls))
