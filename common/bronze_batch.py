"""Batch state, independent of Kafka/MinIO SDKs for failure-path testing."""
import base64
import json
import time
from dataclasses import dataclass, field


def encode_record(message):
    value = message.value()
    record = {
        'kafka': {
            'topic': message.topic(), 'partition': message.partition(),
            'offset': message.offset(), 'timestamp': message.timestamp(),
            'key_base64': base64.b64encode(message.key()).decode() if message.key() is not None else None,
            'headers': [(name, base64.b64encode(data).decode() if data is not None else None)
                        for name, data in (message.headers() or [])],
        },
        # Preserve bytes even for invalid JSON, tombstones, or future source schemas.
        'value_base64': base64.b64encode(value).decode() if value is not None else None,
    }
    try:
        record['event'] = json.loads(value) if value is not None else None
    except (ValueError, UnicodeError):
        record['event'] = None
        record['decode_error'] = 'invalid_json'
    return (json.dumps(record, ensure_ascii=False) + '\n').encode()


@dataclass
class Batch:
    first_offset: int
    last_offset: int
    started: float
    lines: list = field(default_factory=list)
    size: int = 0


class BronzeBatcher:
    def __init__(self, sink, commit, max_records=500, max_bytes=5_000_000,
                 max_seconds=10, clock=time.monotonic):
        if min(max_records, max_bytes, max_seconds) <= 0:
            raise ValueError('Batch limits must be positive')
        self.sink, self.commit = sink, commit
        self.max_records, self.max_bytes, self.max_seconds = max_records, max_bytes, max_seconds
        self.clock = clock
        self.batches = {}

    def add(self, message):
        key = (message.topic(), message.partition())
        line = encode_record(message)
        batch = self.batches.get(key)
        if batch and batch.size + len(line) > self.max_bytes:
            self.flush(key)
            batch = None
        if batch is None:
            batch = self.batches[key] = Batch(message.offset(), message.offset(), self.clock())
        batch.lines.append(line)
        batch.size += len(line)
        batch.last_offset = message.offset()
        if len(batch.lines) >= self.max_records or batch.size >= self.max_bytes:
            self.flush(key)

    def flush(self, key):
        batch = self.batches.get(key)
        if batch is None:
            return
        self.sink.write(*key, batch.first_offset, batch.last_offset, batch.lines)
        # Kafka stores the NEXT offset. A failed upload or commit retains the batch.
        self.commit(*key, batch.last_offset + 1)
        del self.batches[key]

    def flush_due(self):
        for key, batch in list(self.batches.items()):
            if self.clock() - batch.started >= self.max_seconds:
                self.flush(key)

    def flush_all(self):
        for key in list(self.batches):
            self.flush(key)

    def discard(self, partitions):
        # Do not commit revoked/lost ownership. The next owner replays these offsets.
        for p in partitions:
            self.batches.pop((p.topic, p.partition), None)
