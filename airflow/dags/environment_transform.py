"""Read durable Bronze independently of API ingestion and the continuous consumer."""

from datetime import datetime, timedelta, timezone
from airflow.sdk import dag, task


@dag(
    dag_id="environment_bronze_silver_gold",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 9, 14, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    tags=["minio", "bronze", "silver", "gold"],
)
def environment_transform():
    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
    def transform_silver():
        from transforms.storage import LakeStorage
        from transforms.pipeline import bronze_to_silver

        return bronze_to_silver(LakeStorage())

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
        do_xcom_push=False,
    )
    def transform_gold(manifest_key):
        from transforms.storage import LakeStorage
        from transforms.pipeline import silver_to_gold

        silver_to_gold(LakeStorage(), manifest_key)

    transform_gold(transform_silver())


environment_bronze_silver_gold = environment_transform()
