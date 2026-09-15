from airflow.sdk import dag, task
from datetime import datetime

from src.api.nasa_firms import NASAFirmsClient
from src.kafka_client.producer import KafkaProducerClient


@dag(
    schedule="*/5 * * * *",
    start_date=datetime(2026, 9, 14),
    catchup=False,
    tags=["nasa", "firms", "kafka"]
)
def nasa_firms_ingestion():

    @task
    def fetch_and_send():
        client = NASAFirmsClient()
        producer = KafkaProducerClient()

        for event in client.fetch():
            producer.send(
                topic="nasa_firms",
                key=event["event_id"],
                value=event
            )

        producer.flush()

    fetch_and_send()


nasa_firms_ingestion()