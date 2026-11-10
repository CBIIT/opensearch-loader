import threading
import unittest
from unittest.mock import Mock
from opensearch_loader.opensearch_metrics import opensearch_monitor


class MetricsTests(unittest.TestCase):
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
