import unittest
from unittest.mock import Mock
from opensearch_loader.kafka_transport import KafkaWrites, MAX_PAYLOAD_BYTES


class KafkaTransportTests(unittest.TestCase):
    def writer(self):
        w = KafkaWrites.__new__(KafkaWrites)
        w.delegate = Mock()
        w.consumers = 4
        w.max_payload_bytes = MAX_PAYLOAD_BYTES
        w.send = Mock()
        return w

    def test_same_document_keeps_partition_and_bounded_batches(self):
        w = self.writer()
        rows = [{'id': str(i)} for i in range(25000)]
        w.bulk_upsert('test', rows, 'id')
        first = {r['id']: c.args[4] for c in w.send.call_args_list for r in c.args[2]}
        self.assertEqual(len(first), 25000)
        self.assertTrue(all(len(c.args[2]) <= 5000 for c in w.send.call_args_list))
        w.send.reset_mock()
        w.bulk_update('test', rows, 'id')
        second = {r['id']: c.args[4] for c in w.send.call_args_list for r in c.args[2]}
        self.assertEqual(first, second)

    def test_large_document_rejected(self):
        w = self.writer()
        with self.assertRaises(ValueError):
            w.bulk_upsert('test', [{'id': '1', 'data': 'x' * MAX_PAYLOAD_BYTES}], 'id')
        w.send.assert_not_called()

    def test_refresh_only_after_ack_barrier(self):
        w = self.writer()
        w.drain = Mock(side_effect=RuntimeError('consumer write failed'))
        with self.assertRaises(RuntimeError):
            w.refresh_index('test')
        w.delegate.refresh_index.assert_not_called()

    def test_shutdown_on_drain_failure(self):
        w = self.writer()
        w.drain = Mock(side_effect=RuntimeError('consumer died'))
        w.shutdown = Mock()
        with self.assertRaises(RuntimeError): w.close()
        w.shutdown.assert_called_once()

if __name__ == '__main__': unittest.main()
