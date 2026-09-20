"""One ingestion attempt, independent of Airflow scheduling."""
from environment_ingestion.sources import SOURCES


def run_source_once(source):
    """Run inside a task, never during scheduler DAG discovery."""
    import logging
    import os
    from contextlib import ExitStack
    from importlib import import_module
    from pathlib import Path

    try:
        from common.ingestion import DeliveryState, ingest_once
        from kafka.kafka_admin import ensure_topics
        from kafka.producer import KafkaEventProducer

        # Explicit persistent local storage; never silently use an ephemeral task cwd.
        state_dir = Path(os.environ["API_KAFKA_STATE_DIR"])
        if not state_dir.is_absolute():
            raise ValueError("API_KAFKA_STATE_DIR must be absolute")
        module_name, class_name, _ = SOURCES[source]
        client_class = getattr(import_module(module_name), class_name)

        with ExitStack() as cleanup:
            client = client_class()
            cleanup.callback(client.session.close)
            ensure_topics([client.TOPIC], replication_factor=int(
                os.getenv("API_KAFKA_REPLICATION_FACTOR", "3")))
            state = DeliveryState(state_dir / f"{source}.sqlite3")
            cleanup.callback(state.close)
            producer = KafkaEventProducer()
            # LIFO: drain callbacks before closing the SQLite checkpoint connection.
            cleanup.callback(producer.flush)
            acknowledged = ingest_once(client, producer, state)
        logging.getLogger(__name__).info(
            "%s: acknowledged %d events", source, acknowledged)
        return acknowledged
    except Exception as exc:
        # HTTP exception URLs can contain NASA's key. Do not expose the original
        # exception text/chain in task logs; failure still triggers Airflow retries.
        raise RuntimeError(
            f"{source}: ingestion failed ({type(exc).__name__}); "
            "check API keys, state directory and Kafka connectivity"
        ) from None

