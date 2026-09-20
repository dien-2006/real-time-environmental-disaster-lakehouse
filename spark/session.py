"""Spark session lifecycle and MinIO S3A configuration."""

import os
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from pyspark.sql import SparkSession


@contextmanager
def spark_session(app_name="environment-transform", use_minio=True):
    if use_minio:
        from config.settings import setting  # Load .env before reading Spark settings.

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")
        )
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    if use_minio:
        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
        access_key = os.environ["MINIO_ACCESS_KEY"]
        secret_key = os.environ["MINIO_SECRET_KEY"]
        packages = os.getenv("SPARK_S3_PACKAGES", "org.apache.hadoop:hadoop-aws:3.4.1")
        builder = (
            builder.config("spark.jars.packages", packages)
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                ("https://" if secure else "http://") + endpoint,
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint.region",
                os.getenv("MINIO_REGION", "us-east-1"),
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(secure).lower())
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )
    spark = builder.getOrCreate()
    try:
        # Ship only pure transformation packages, never .env or credentials.
        with TemporaryDirectory(prefix="spark-transform-") as temporary:
            archive = Path(temporary) / "environment_transform.zip"
            root = Path(__file__).resolve().parents[2]
            with ZipFile(archive, "w") as bundle:
                for package in ("transforms", "common"):
                    for path in (root / package).rglob("*.py"):
                        bundle.write(path, path.relative_to(root))
            spark.sparkContext.addPyFile(str(archive))
            yield spark
    finally:
        spark.stop()
