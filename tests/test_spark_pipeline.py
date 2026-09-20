"""Real local Spark integration tests; no Kafka/MinIO credentials required."""

import gzip
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_data_model import fixtures
from transforms.model import build_star_schema
from transforms.silver import build_silver

HAS_SPARK = importlib.util.find_spec("pyspark") is not None


class LocalLake:
    bronze_bucket = "bronze"
    silver_bucket = "silver"
    gold_bucket = "gold"
    prefix = "test"

    def __init__(self, root):
        self.root = Path(root)
        path = self.root / "bronze" / "input.jsonl.gz"
        path.parent.mkdir()
        records = fixtures()
        # Simulate overlapping Bronze files / replay plus malformed input.
        lines = [json.dumps(row) for row in [*records, records[0]]]
        path.write_bytes(
            gzip.compress(("\n".join([*lines, "{broken"]) + "\n").encode())
        )

    def uri(self, bucket, key):
        return (self.root / bucket / key).as_uri()

    def bronze_objects(self):
        return ["input.jsonl.gz"]

    def put_json(self, bucket, key, value):
        path = self.root / bucket / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def get_json(self, bucket, key):
        return json.loads((self.root / bucket / key).read_text())


@unittest.skipUnless(
    HAS_SPARK, "Install transforms/requirements.txt to run real Spark tests"
)
class SparkPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
        from transforms.spark.session import spark_session

        cls.context = spark_session("transform-tests", use_minio=False)
        cls.spark = cls.context.__enter__()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.context.__exit__(None, None, None)

    def test_real_spark_parquet_model_and_failed_publish(self):
        from transforms.spark.jobs import bronze_to_silver, silver_to_gold

        with tempfile.TemporaryDirectory() as directory:
            lake = LocalLake(directory)
            manifest_key = bronze_to_silver(self.spark, lake)
            manifest = lake.get_json("silver", manifest_key)
            self.assertEqual(manifest["silver_count"], 4)
            self.assertEqual(manifest["reject_count"], 1)
            published = silver_to_gold(self.spark, lake, manifest_key)
            self.assertEqual(published["schema_version"], "3.0")
            self.assertEqual(len(published["gold_tables"]), 12)
            reference_rows, _ = build_silver(fixtures())
            expected = build_star_schema(reference_rows)
            for name, expected_rows in expected.items():
                frame = self.spark.read.parquet(
                    lake.uri("gold", published["gold_tables"][name]["key"])
                )
                # Tiny fixture only: production code never collects the datasets.
                rows = [r.asDict(recursive=True) for r in frame.collect()]
                self.assertEqual(len(rows), len(expected_rows), name)
                pk = next(
                    c
                    for c in (
                        "measurement_key",
                        "event_key",
                        "sensor_key",
                        "parameter_key",
                        "location_key",
                        "date_key",
                        "source_key",
                    )
                    if c in frame.columns and c in expected_rows[0]
                )
                self.assertEqual(
                    {r[pk] for r in rows}, {r[pk] for r in expected_rows}, name
                )
            metric_frame = self.spark.read.parquet(
                lake.uri("gold", published["gold_tables"]["daily_metrics"]["key"])
            )
            wind = metric_frame.filter("metric = 'wind_speed_10m'").first()
            self.assertEqual(
                (wind.sample_count, wind.sample_mean, wind.unit), (1, 10.0, "m/s")
            )
            weather_values = self.spark.read.parquet(
                lake.uri(
                    "gold", published["gold_tables"]["fact_weather_measurement"]["key"]
                )
            )
            self.assertEqual({r.value for r in weather_values.collect()}, {10.0, 30.0})
            with patch(
                "transforms.spark.jobs.write_table",
                side_effect=OSError("upload unavailable"),
            ):
                with self.assertRaises(OSError):
                    silver_to_gold(self.spark, lake, manifest_key)
            self.assertEqual(lake.get_json("gold", "test/latest.json"), published)

    def test_single_source_keeps_empty_table_schemas(self):
        from test_transforms import quake
        from transforms.spark.silver import parse_bronze, silver_frames
        from transforms.spark.gold import build_tables

        raw = self.spark.createDataFrame(
            [(json.dumps(quake()), "fixture")],
            "value string, bronze_object string",
        )
        silver, _ = silver_frames(parse_bronze(raw))
        tables = build_tables(silver)
        with tempfile.TemporaryDirectory() as directory:
            tables["dim_sensor"].write.parquet(directory + "/sensors")
            sensors = self.spark.read.parquet(directory + "/sensors")
            self.assertEqual(sensors.count(), 0)
            self.assertIn("sensor_key", sensors.columns)

    def test_distributed_latest_version_and_units(self):
        from test_transforms import quake
        from transforms.spark.silver import parse_bronze, silver_frames

        records = [quake(), quake(1, mag=5, updated=5000), *fixtures()[1:]]
        frame = self.spark.createDataFrame(
            [(json.dumps(r), "fixture") for r in records],
            "value string, bronze_object string",
        )
        silver, rejects = silver_frames(parse_bronze(frame))
        self.assertEqual(rejects.count(), 0)
        self.assertEqual(silver.count(), 4)
        earthquake = silver.filter("source = 'usgs'").first()
        self.assertEqual(earthquake.metrics[0].value, 5)
        weather = silver.filter("source = 'open_meteo'").first()
        wind = next(m for m in weather.metrics if m.name == "wind_speed_10m")
        self.assertEqual((wind.value, wind.unit), (10, "m/s"))


if __name__ == "__main__":
    unittest.main()
