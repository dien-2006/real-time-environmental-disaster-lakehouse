"""Snapshot orchestration: executors handle datasets, driver handles manifests."""

from datetime import datetime, timezone
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import functions as F

from transforms.model import SOURCE_FACT
from transforms.spark.gold import build_tables, validate_tables
from transforms.spark.silver import parse_bronze, silver_frames


def data_uri(storage, bucket, key):
    # Local file storage adapters can supply uri() for integration tests.
    if hasattr(storage, "uri"):
        return storage.uri(bucket, key)
    return f"s3a://{bucket}/{key}"


def write_table(frame, storage, bucket, key):
    frame.write.mode("overwrite").parquet(data_uri(storage, bucket, key))
    return {"key": key, "format": "parquet"}


def bronze_to_silver(spark, storage):
    inputs = storage.bronze_objects()
    if not inputs:
        raise ValueError("No Bronze objects found")
    snapshot = uuid4().hex
    base = f"{storage.prefix}/snapshots/{snapshot}"
    paths = [data_uri(storage, storage.bronze_bucket, key) for key in inputs]
    raw = spark.read.text(paths).withColumn("bronze_object", F.input_file_name())
    parsed = parse_bronze(raw).persist(StorageLevel.MEMORY_AND_DISK)
    silver, rejects = silver_frames(parsed)
    silver = silver.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        rejects_key = f"{base}/rejects"
        write_table(rejects, storage, storage.silver_bucket, rejects_key)
        count = silver.count()
        if not count:
            raise ValueError("No valid Silver rows; rejects saved without publishing")
        tables = {}
        for source, fact_name in SOURCE_FACT.items():
            name = fact_name.replace("fact_", "silver_", 1)
            tables[name] = write_table(
                silver.filter(F.col("source") == source),
                storage,
                storage.silver_bucket,
                f"{base}/{name}",
            )
        manifest = {
            "schema_version": "3.0",
            "engine": "pyspark",
            "format": "parquet",
            "snapshot_id": snapshot,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "bronze_bucket": storage.bronze_bucket,
            "bronze_objects": inputs,
            "silver_bucket": storage.silver_bucket,
            "silver_tables": tables,
            "rejects_key": rejects_key,
            "silver_count": count,
            "reject_count": rejects.count(),
        }
        manifest_key = f"{base}/manifest.json"
        storage.put_json(storage.silver_bucket, manifest_key, manifest)
        return manifest_key
    finally:
        silver.unpersist()
        parsed.unpersist()


def silver_to_gold(spark, storage, manifest_key):
    manifest = storage.get_json(storage.silver_bucket, manifest_key)
    if manifest.get("schema_version") != "3.0":
        raise ValueError("Rebuild Silver with Spark schema v3 before building Gold")
    paths = [
        data_uri(storage, manifest["silver_bucket"], table["key"])
        for table in manifest["silver_tables"].values()
    ]
    silver = spark.read.parquet(*paths).persist(StorageLevel.MEMORY_AND_DISK)
    tables = build_tables(silver)
    base = f"{storage.prefix}/snapshots/{manifest['snapshot_id']}/gold-attempts/{uuid4().hex}"
    try:
        gold_tables = {}
        # Materialize each table once. Validate Parquet readbacks to keep plans small
        # and verify exactly the data that will be published.
        materialized = {}
        for name, frame in tables.items():
            gold_tables[name] = write_table(
                frame, storage, storage.gold_bucket, f"{base}/{name}"
            )
            materialized[name] = spark.read.parquet(
                data_uri(storage, storage.gold_bucket, gold_tables[name]["key"])
            )
        validate_tables(materialized)
        published = {
            **manifest,
            "gold_bucket": storage.gold_bucket,
            "gold_tables": gold_tables,
            "silver_manifest_key": manifest_key,
        }
        storage.put_json(storage.gold_bucket, f"{base}/manifest.json", published)
        storage.put_json(
            storage.gold_bucket, f"{storage.prefix}/latest.json", published
        )
        return published
    finally:
        silver.unpersist()
