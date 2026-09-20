"""Normalize on executors, then deduplicate with distributed window operations."""

import json

from pyspark.sql import Window, functions as F

from transforms.silver import normalize
from transforms.spark.schemas import PARSED_SCHEMA


def parse_bronze_line(line):
    try:
        row = normalize(json.loads(line))
        row["schema_version"] = "3.0"
        # Spark's explicit schema retains only the Kafka lineage used by the model.
        for name in ("partition", "offset"):
            value = row["kafka"][name]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("Invalid Kafka coordinate")
        if not row["kafka"]["topic"]:
            raise ValueError("Missing Kafka topic")
        return {"row": row, "error": None}
    except (
        ValueError,
        TypeError,
        KeyError,
        IndexError,
        AttributeError,
        OverflowError,
    ) as exc:
        return {"row": None, "error": type(exc).__name__}


def parse_bronze(frame):
    parser = F.udf(parse_bronze_line, PARSED_SCHEMA)
    return frame.withColumn("parsed", parser("value"))


def silver_frames(parsed):
    rejected = parsed.filter("parsed.error IS NOT NULL").select(
        "bronze_object",
        F.col("value").alias("raw_line"),
        F.col("parsed.error").alias("reason"),
    )
    valid = parsed.filter("parsed.error IS NULL").select("parsed.row.*")
    ordering = [
        F.col(c).desc() for c in ("source_updated_at", "ingested_at", "payload_hash")
    ]
    transport = Window.partitionBy(
        "kafka.topic", "kafka.partition", "kafka.offset"
    ).orderBy(*ordering)
    business = Window.partitionBy("source", "event_id").orderBy(*ordering)
    latest = (
        valid.withColumn("_transport_rank", F.row_number().over(transport))
        .filter("_transport_rank = 1")
        .drop("_transport_rank")
        .withColumn("_event_rank", F.row_number().over(business))
        .filter("_event_rank = 1")
        .drop("_event_rank")
    )
    # Keep version ordering strings above; persist analytical columns as actual types.
    for name in ("observed_at", "ingested_at", "source_updated_at", "period_end"):
        latest = latest.withColumn(name, F.to_timestamp(name))
    return latest.withColumn("observed_date", F.to_date("observed_date")), rejected
