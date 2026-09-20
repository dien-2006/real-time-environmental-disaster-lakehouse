"""MinIO snapshot I/O; latest.json is published only after all tables succeed."""

import io
import json
import os


class LakeStorage:
    def __init__(self):
        # Reuse existing credentials/HTTP configuration; no change to Bronze writer.
        from config.settings import setting
        from sinks.minio_bronze import MinioBronzeSink

        bronze = MinioBronzeSink()
        self.client, self.bronze_bucket = bronze.client, bronze.bucket
        self.bronze_prefix = bronze.prefix
        self.silver_bucket = os.getenv("MINIO_SILVER_BUCKET", "silver")
        self.gold_bucket = os.getenv("MINIO_GOLD_BUCKET", "gold")
        self.prefix = os.getenv("MINIO_TRANSFORM_PREFIX", "environment-v1").strip("/")
        for bucket in (self.silver_bucket, self.gold_bucket):
            if not self.client.bucket_exists(bucket):
                raise ValueError(f"Missing output bucket: {bucket}")

    def bronze_objects(self):
        prefix = self.bronze_prefix + "/" if self.bronze_prefix else ""
        return sorted(
            o.object_name
            for o in self.client.list_objects(
                self.bronze_bucket, prefix=prefix, recursive=True
            )
            if o.object_name.endswith(".jsonl.gz")
        )

    def read_bytes(self, bucket, key):
        response = self.client.get_object(bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def put_json(self, bucket, key, value):
        body = json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
        self.client.put_object(
            bucket, key, io.BytesIO(body), len(body), content_type="application/json"
        )

    def get_json(self, bucket, key):
        return json.loads(self.read_bytes(bucket, key))
