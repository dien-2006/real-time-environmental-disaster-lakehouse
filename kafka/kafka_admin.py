
from pathlib import Path
from confluent_kafka.admin import AdminClient, NewTopic
from config.settings import setting

def create_topic(name_topic: str, num_partitions: int, replication_factor: int):

    admin = AdminClient({
        "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094"
    })
    
    topic = NewTopic(topic = name_topic, num_partitions = num_partitions, replication_factor= replication_factor)

    futures=admin.create_topics([topic])

    try :
        futures[name_topic].result()
        print(f"Created topic: {name_topic}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_topic(name_topic= "nasa_firms", num_partitions= 3, replication_factor=3)
