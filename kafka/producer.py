import json
import time
from confluent_kafka import Producer
from config.settings import setting


class KafkaEventProducer:
    def __init__(self):
        self.errors = []
        self.producer = Producer({
            "bootstrap.servers": setting.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "environment-api-producer",
            "acks": "all", "enable.idempotence": True,
            "delivery.timeout.ms": 30000,
            "batch.size": 100_000, "linger.ms": 100,
        })

    def _delivery_report(self, err, msg):
        if err:
            self.errors.append(str(err))

    def send(self, topic: str, key: str, value: dict, *, on_delivery=None) -> None:
        def delivery_report(err, msg):
            self._delivery_report(err, msg)
            if on_delivery is not None:
                try:
                    on_delivery(err, msg)
                except Exception as exc:
                    # Surface checkpoint failures through flush, not librdkafka callbacks.
                    self.errors.append(f"Delivery callback failed: {type(exc).__name__}")

        deadline = time.monotonic() + 30
        while True:
            try:
                self.producer.produce(topic=topic, key=key.encode(),
                    value=json.dumps(value, ensure_ascii=False).encode(),
                    callback=delivery_report)
                break
            except BufferError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Kafka producer queue remained full")
                self.producer.poll(0.5)
        self.producer.poll(0)

    def flush(self) -> None:
        remaining = self.producer.flush(35)
        errors, self.errors = self.errors, []
        if remaining or errors:
            raise RuntimeError(f"Kafka delivery unsuccessful: pending={remaining}, errors={errors}")
