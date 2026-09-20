import base64
import gzip
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from common.bronze_batch import BronzeBatcher
from sinks.minio_bronze import MinioBronzeSink


def message(offset, partition=0, value=b'{"raw":{"value":3}}'):
    return Mock(**{'topic.return_value': 'earthquake', 'partition.return_value': partition,
                   'offset.return_value': offset, 'value.return_value': value,
                   'key.return_value': b'key', 'headers.return_value': [],
                   'timestamp.return_value': (1, 1000)})


class BronzeTests(unittest.TestCase):
    def setUp(self):
        self.sink = Mock()
        self.commit = Mock()
        self.now = 0
        self.batcher = BronzeBatcher(self.sink, self.commit, max_records=2,
                                    max_seconds=10, clock=lambda: self.now)

    def test_upload_precedes_commit_and_uses_next_offset(self):
        calls = []
        self.sink.write.side_effect = lambda *a: calls.append('upload')
        self.commit.side_effect = lambda *a: calls.append('commit')
        self.batcher.add(message(8))
        self.commit.assert_not_called()
        self.batcher.add(message(9))
        self.assertEqual(calls, ['upload', 'commit'])
        self.commit.assert_called_once_with('earthquake', 0, 10)

    def test_upload_failure_retains_batch_without_commit(self):
        self.batcher.add(message(8))
        self.sink.write.side_effect = OSError('unavailable')
        with self.assertRaises(OSError):
            self.batcher.flush_all()
        self.commit.assert_not_called()
        self.assertEqual(len(self.batcher.batches), 1)

    def test_commit_failure_retains_batch(self):
        self.batcher.add(message(8))
        self.commit.side_effect = OSError('broker down')
        with self.assertRaises(OSError):
            self.batcher.flush_all()
        self.sink.write.assert_called_once()
        self.assertEqual(len(self.batcher.batches), 1)

    def test_time_threshold_and_partition_isolation(self):
        self.batcher.add(message(2, 0))
        self.now = 9
        self.batcher.add(message(20, 1))
        self.now = 10
        self.batcher.flush_due()
        self.commit.assert_called_once_with('earthquake', 0, 3)
        self.assertIn(('earthquake', 1), self.batcher.batches)

    def test_revoke_discards_without_commit(self):
        self.batcher.add(message(0))
        self.batcher.discard([SimpleNamespace(topic='earthquake', partition=0)])
        self.batcher.flush_all()
        self.sink.write.assert_not_called()
        self.commit.assert_not_called()

    def test_invalid_json_preserved(self):
        self.batcher.add(message(0, value=b'\xffbroken'))
        self.batcher.flush_all()
        line = self.sink.write.call_args.args[-1][0]
        record = json.loads(line)
        self.assertEqual(base64.b64decode(record['value_base64']), b'\xffbroken')
        self.assertEqual(record['decode_error'], 'invalid_json')

    def test_byte_limit_flushes_single_oversized_record(self):
        batcher = BronzeBatcher(self.sink, self.commit, max_bytes=1)
        batcher.add(message(0))
        self.commit.assert_called_once_with('earthquake', 0, 1)

    def test_object_key_and_content_reproducible(self):
        sink = MinioBronzeSink.__new__(MinioBronzeSink)
        sink.client = Mock()
        sink.bucket, sink.prefix = 'bronze', 'environment-v1'
        bodies = []
        sink.client.put_object.side_effect = lambda bucket, key, data, length, **kw: bodies.append(data.read())
        key = sink.write('earthquake', 0, 5, 6, [b'{"a":1}\n'])
        sink.write('earthquake', 0, 5, 6, [b'{"a":1}\n'])
        self.assertEqual(key, 'environment-v1/topic=earthquake/partition=0/offsets=5-6.jsonl.gz')
        self.assertEqual(bodies[0], bodies[1])
        self.assertEqual(gzip.decompress(bodies[0]), b'{"a":1}\n')


if __name__ == '__main__':
    unittest.main()
