import json

from config.settings import setting
from confluent_kafka import Producer
from api.nasa_firms import NASAFirmsClient


class KafkaEventProducer:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": setting.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "environment-api-producer",

            "acks": "all",
            "retries": 5,
            "enable.idempotence": True,

            "batch.size": 100_000,
            "linger.ms": 100,
        })

    @staticmethod
    def _delivery_report(err, msg):
        if err:
            print(f"[ERROR] Kafka delivery failed: {err}")
        else:
            print(
                f"[OK] topic={msg.topic()} "
                f"partition={msg.partition()} "
                f"offset={msg.offset()}"
            )

    def send(
        self,
        topic: str,
        key: str,
        value: dict
    ) -> None:

        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(
                value,
                ensure_ascii=False,
                default=str,
            ).encode("utf-8"),

  
            callback=self._delivery_report
        )

        # xử lý callback + network events
        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush()


if __name__ == "__main__":

    client = NASAFirmsClient()
    producer = KafkaEventProducer()

    events = client.fetch()

    for event in events:
        producer.send(
            topic="nasa_firms",
            key=event["event_id"],
            value=event,
        )

    producer.flush()