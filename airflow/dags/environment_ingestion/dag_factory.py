"""Common schedule, retry and concurrency policy for all source DAGs."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task


def build_source_dag(source, schedule):
    @dag(
        dag_id=f"api_kafka_{source}",
        description=f"Fetch {source} once and send raw events to Kafka",
        schedule=schedule,
        start_date=datetime(2026, 9, 14, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        max_active_tasks=1,
        is_paused_upon_creation=True,
        tags=["api", "kafka", source],
    )
    def source_ingestion():
        @task(
            task_id="fetch_and_publish",
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=10),
            execution_timeout=timedelta(minutes=30),
            do_xcom_push=False,
        )
        def fetch_and_publish():
            # Import application dependencies only when the task executes.
            from environment_ingestion.runner import run_source_once

            run_source_once(source)

        fetch_and_publish()

    return source_ingestion()

