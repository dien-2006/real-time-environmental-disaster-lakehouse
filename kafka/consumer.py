import json
from confluent_kafka import Consumer
from config.settings import setting

class KafkaEventConsumer:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": setting.KAFKA_BOOTSTRAP_SERVERS,
            "group.id" : "bronze-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })

    def consume(self, topic: str):

        self.consumer.subscribe([topic])
        print(f"Listening topic: {topic}")

        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    print(f"[ERROR] {msg.error()}")
                    continue

                event = json.loads(
                    msg.value().decode("utf-8")
                )

                print(
                    f"partition={msg.partition()} "
                    f"offset={msg.offset()} "
                    f"event={event}"
                )

        except KeyboardInterrupt:
            print("Stopping consumer...")

        finally:
            self.consumer.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Inspect raw Kafka events")
    parser.add_argument("--topic", default="nasa_firms")
    args = parser.parse_args()
    consumer = KafkaEventConsumer()
    consumer.consume(args.topic)