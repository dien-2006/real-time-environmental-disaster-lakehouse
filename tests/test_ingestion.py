import tempfile
import unittest
from pathlib import Path
from common.events import envelope
from common.ingestion import DeliveryState, ingest_once


class Client:
    TOPIC = 'earthquake'
    def __init__(self, events):
        self.events = events
    def fetch(self):
        yield from self.events


class Producer:
    def __init__(self, fail=False):
        self.events = []
        self.fail = fail
        self.callbacks = []
        self.flush_count = 0
    def send(self, topic, key, value, *, on_delivery=None):
        self.events.append(value)
        self.callbacks.append(on_delivery)
    def flush(self):
        self.flush_count += 1
        callbacks, self.callbacks = self.callbacks, []
        for callback in callbacks:
            callback('delivery failed' if self.fail else None, None)
        if self.fail:
            raise RuntimeError('delivery failed')


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / 'state.sqlite3'
        self.state = DeliveryState(self.path)
    def tearDown(self):
        self.state.close()
        self.directory.cleanup()
    def event(self, magnitude=3):
        return envelope('usgs', 'earthquake', 'quake-1', {'mag': magnitude})
    def test_stable_identity_and_raw(self):
        raw = {'latitude': '21.0', 'unknown': None}
        a = envelope('test', 'weather', 'id', raw)
        b = envelope('test', 'weather', 'id', raw)
        self.assertEqual(a['event_id'], b['event_id'])
        self.assertEqual(a['payload_hash'], b['payload_hash'])
        self.assertEqual(a['raw'], raw)
    def test_dedup_survives_restart(self):
        client = Client([self.event(), self.event()])
        self.assertEqual(ingest_once(client, Producer(), self.state), 1)
        self.state.close()
        self.state = DeliveryState(self.path)
        self.assertEqual(ingest_once(client, Producer(), self.state), 0)
    def test_changed_payload_is_delivered(self):
        ingest_once(Client([self.event()]), Producer(), self.state)
        self.assertEqual(ingest_once(Client([self.event(4)]), Producer(), self.state), 1)
    def test_failure_does_not_checkpoint(self):
        event = self.event()
        with self.assertRaises(RuntimeError):
            ingest_once(Client([event]), Producer(fail=True), self.state)
        self.assertFalse(self.state.seen(Client.TOPIC, event))
        self.assertEqual(ingest_once(Client([event]), Producer(), self.state), 1)
    def test_partial_fetch_preserves_acknowledged_event(self):
        event = self.event()
        class BrokenClient:
            TOPIC = Client.TOPIC
            def fetch(self):
                yield event
                raise ValueError('bad response')
        with self.assertRaises(ValueError):
            ingest_once(BrokenClient(), Producer(), self.state)
        self.assertTrue(self.state.seen(Client.TOPIC, event))
    def test_events_are_queued_before_acknowledgement(self):
        first, second = self.event(), self.event(4)
        state = self.state
        test = self
        class SequentialClient:
            TOPIC = Client.TOPIC
            def fetch(self):
                yield first
                test.assertFalse(state.seen(self.TOPIC, first))
                yield second
        producer = Producer()
        self.assertEqual(ingest_once(SequentialClient(), producer, state), 2)
        self.assertEqual(producer.events, [first, second])
        self.assertEqual(producer.flush_count, 1)
        self.assertTrue(state.seen(Client.TOPIC, second))
    def test_topic_isolation(self):
        event = self.event()
        self.state.mark('other', [event])
        self.assertFalse(self.state.seen(Client.TOPIC, event))


if __name__ == '__main__':
    unittest.main()
