"""Explicit schemas avoid inference drift when a source or metric is absent."""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def field(name, kind=StringType()):
    return StructField(name, kind, True)


KAFKA_SCHEMA = StructType(
    [
        field("topic"),
        field("partition", LongType()),
        field("offset", LongType()),
    ]
)
METRIC_SCHEMA = StructType(
    [
        field("name"),
        field("value", DoubleType()),
        field("unit"),
    ]
)
DIMENSIONS_SCHEMA = StructType(
    [
        field("status"),
        field("satellite"),
        field("instrument"),
        field("confidence"),
        field("sensor_id", LongType()),
    ]
)
SILVER_SCHEMA = StructType(
    [
        *[
            field(name)
            for name in (
                "schema_version",
                "event_id",
                "source",
                "event_type",
                "observed_at",
                "observed_date",
                "ingested_at",
                "period_end",
                "source_updated_at",
                "payload_hash",
                "location_id",
                "location_name",
            )
        ],
        field("latitude", DoubleType()),
        field("longitude", DoubleType()),
        field("metrics", ArrayType(METRIC_SCHEMA)),
        field("dimensions", DIMENSIONS_SCHEMA),
        field("kafka", KAFKA_SCHEMA),
    ]
)
PARSED_SCHEMA = StructType(
    [
        field("row", SILVER_SCHEMA),
        field("error"),
    ]
)
