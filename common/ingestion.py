"""Persist acknowledged revisions; replay is at-least-once across crashes."""
import sqlite3
from pathlib import Path


class DeliveryState:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("CREATE TABLE IF NOT EXISTS delivered (topic TEXT, event_id TEXT, revision TEXT, PRIMARY KEY(topic, event_id))")

    def seen(self, topic, event):
        row = self.db.execute("SELECT revision FROM delivered WHERE topic=? AND event_id=?",
                              (topic, event['event_id'])).fetchone()
        return row == (event['payload_hash'],)

    def mark(self, topic, events):
        with self.db:
            self.db.executemany("INSERT OR REPLACE INTO delivered VALUES (?, ?, ?)",
                [(topic, e['event_id'], e['payload_hash']) for e in events])

    def close(self):
        self.db.close()


def ingest_once(client, producer, state):
    """Queue each event immediately; checkpoint only successful delivery callbacks."""
    acknowledged = 0
    pending = set()

    def callback_for(event, identity):
        def delivered(err, msg):
            nonlocal acknowledged
            try:
                if err is None:
                    state.mark(client.TOPIC, [event])
                    acknowledged += 1
            finally:
                pending.discard(identity)
        return delivered

    try:
        for event in client.fetch():
            identity = (event['event_id'], event['payload_hash'])
            if identity in pending or state.seen(client.TOPIC, event):
                continue
            pending.add(identity)
            try:
                producer.send(client.TOPIC, event['event_id'], event,
                              on_delivery=callback_for(event, identity))
            except Exception:
                pending.discard(identity)
                raise
            # Bound memory and periodically surface asynchronous delivery errors.
            if len(pending) >= 100:
                producer.flush()
    except BaseException:
        # Drain accepted events even when fetching the next API record fails.
        # Preserve the original exception; unacknowledged events can be replayed.
        try:
            producer.flush()
        except Exception:
            pass
        raise
    else:
        producer.flush()
    return acknowledged
