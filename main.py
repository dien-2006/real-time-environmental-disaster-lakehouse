"""Run one independent polling worker per selected API source."""
import argparse
import logging
import signal
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from api.usgs import USGSClient
from api.open_meteo import OpenMeteoClient
from api.openaq import OpenAQClient
from api.nasa_firms import NASAFirmsClient
from common.ingestion import DeliveryState, ingest_once
from kafka.producer import KafkaEventProducer
from kafka.kafka_admin import ensure_topics

SOURCES = {
    "usgs": (USGSClient, 60),
    "open_meteo": (OpenMeteoClient, 900),
    "openaq": (OpenAQClient, 900),
    "nasa_firms": (NASAFirmsClient, 1800),
}
log = logging.getLogger(__name__)


def worker(name, client, args, stop):
    state = DeliveryState(Path(args.state_dir) / f"{name}.sqlite3")
    producer = KafkaEventProducer()
    failed = False
    try:
        while not stop.is_set():
            try:
                count = ingest_once(client, producer, state)
                log.info("%s: acknowledged %d events", name, count)
            except Exception as exc:
                # Do not log HTTP exception URLs: NASA embeds its API key there.
                log.error("%s: ingestion failed (%s); unconfirmed events will be retried",
                          name, type(exc).__name__)
                failed = True
            if args.once:
                break
            stop.wait(args.interval or SOURCES[name][1])
    finally:
        try:
            producer.flush()
        finally:
            state.close()
            client.session.close()
    return not failed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", nargs="+", choices=SOURCES, default=["usgs"])
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval", type=float, help="Override polling seconds for selected sources")
    parser.add_argument("--state-dir", default=".state")
    parser.add_argument("--replication-factor", type=int, default=3)
    args = parser.parse_args()
    if args.interval is not None and args.interval <= 0:
        parser.error("--interval must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    clients = {}
    try:
        for name in dict.fromkeys(args.sources):
            clients[name] = SOURCES[name][0]()
        ensure_topics([c.TOPIC for c in clients.values()], replication_factor=args.replication_factor)
    except Exception as exc:
        for client in clients.values():
            client.session.close()
        log.error("Startup failed (%s). Check API keys and Kafka connectivity/configuration.", type(exc).__name__)
        return 1
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    with ThreadPoolExecutor(max_workers=len(clients)) as pool:
        futures = [pool.submit(worker, name, client, args, stop) for name, client in clients.items()]
        results = [future.result() for future in futures]
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
