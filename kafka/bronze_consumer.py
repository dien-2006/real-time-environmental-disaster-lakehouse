"""Continuous Kafka → MinIO Bronze worker, separate from the inspection consumer."""
import argparse
import logging
import signal
import threading

from common.bronze_batch import BronzeBatcher

log = logging.getLogger(__name__)


class BronzeConsumer:
    def __init__(self, sink, group_id='minio-bronze-v1', **batch_options):
        from confluent_kafka import Consumer, KafkaException, TopicPartition
        from config.settings import setting

        self.consumer = Consumer({
            'bootstrap.servers': setting.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'enable.auto.offset.store': False,
            'max.poll.interval.ms': 900000,
            'queued.max.messages.kbytes': 16384,
        })

        def commit(topic, partition, next_offset):
            result = self.consumer.commit(
                offsets=[TopicPartition(topic, partition, next_offset)], asynchronous=False)
            for item in result or []:
                if item.error:
                    raise KafkaException(item.error)
            log.info('Stored and committed %s partition=%d next_offset=%d',
                     topic, partition, next_offset)

        self.batcher = BronzeBatcher(sink, commit, **batch_options)

    def _discard_revoked(self, consumer, partitions):
        self.batcher.discard(partitions)
        log.info('Discarded uncommitted buffers for %d revoked/lost partitions; Kafka will replay',
                 len(partitions))

    def consume(self, topics, stop):
        from confluent_kafka import KafkaError, KafkaException
        try:
            self.consumer.subscribe(topics, on_revoke=self._discard_revoked,
                                    on_lost=self._discard_revoked)
            while not stop.is_set():
                message = self.consumer.poll(1.0)
                if message is not None:
                    if message.error():
                        if message.error().code() != KafkaError._PARTITION_EOF:
                            raise KafkaException(message.error())
                    else:
                        self.batcher.add(message)
                self.batcher.flush_due()
            self.batcher.flush_all()
        finally:
            # On failure, do not flush further partitions or auto-commit. Restart replays.
            self.consumer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--topics', nargs='+', default=['earthquake', 'weather', 'air_quality', 'nasa_firms'])
    parser.add_argument('--group-id', default='minio-bronze-v1')
    parser.add_argument('--batch-size', type=int, default=500)
    parser.add_argument('--batch-bytes', type=int, default=5_000_000)
    parser.add_argument('--batch-seconds', type=float, default=10)
    args = parser.parse_args()
    if min(args.batch_size, args.batch_bytes, args.batch_seconds) <= 0:
        parser.error('Batch limits must be positive')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        # Load .env through existing configuration before constructing the sink.
        from config.settings import setting
        from sinks.minio_bronze import MinioBronzeSink
        consumer = BronzeConsumer(MinioBronzeSink(), args.group_id,
            max_records=args.batch_size, max_bytes=args.batch_bytes, max_seconds=args.batch_seconds)
        consumer.consume(args.topics, stop)
    except Exception as exc:
        log.error('Bronze consumer stopped (%s); check Kafka, MinIO and bucket configuration. '
                  'Uncommitted records will be replayed on restart.', type(exc).__name__)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
