"""Airflow discovery entry point: register one DAG per configured API source."""
from environment_ingestion.dag_factory import build_source_dag
from environment_ingestion.sources import SOURCES


for source_name, (_, _, cron_schedule) in SOURCES.items():
    globals()[f"api_kafka_{source_name}"] = build_source_dag(source_name, cron_schedule)
