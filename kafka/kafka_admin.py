from confluent_kafka import KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from config.settings import setting


def ensure_topics(names, partitions=3, replication_factor=3):
    admin = AdminClient({"bootstrap.servers": setting.KAFKA_BOOTSTRAP_SERVERS})
    futures = admin.create_topics([
        NewTopic(name, num_partitions=partitions, replication_factor=replication_factor)
        for name in names], request_timeout=20)
    for future in futures.values():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def create_topic(name_topic, num_partitions, replication_factor):
    ensure_topics([name_topic], num_partitions, replication_factor)


if __name__ == "__main__":
    ensure_topics(["earthquake", "weather", "air_quality", "nasa_firms"])
