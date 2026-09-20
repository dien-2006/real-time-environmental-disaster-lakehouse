"""Minimal ingestion envelope; source fields remain unchanged in raw."""
import hashlib
import json
from datetime import datetime, timezone


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode()).hexdigest()


def envelope(source, event_type, identity, raw, *, context=None, revision=None):
    return {
        "schema_version": "1.0",
        "event_id": source + ":" + digest(identity),
        "source": source,
        "event_type": event_type,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "payload_hash": digest(raw if revision is None else revision),
        "context": context or {},
        "raw": raw,
    }
