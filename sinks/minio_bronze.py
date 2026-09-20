"""Write one Kafka partition batch as a reproducible gzip JSONL object."""
import gzip
import io
import os


class MinioBronzeSink:
    def __init__(self):
        from minio import Minio
        import urllib3

        access_key = os.environ.get('MINIO_ACCESS_KEY')
        secret_key = os.environ.get('MINIO_SECRET_KEY')
        if not access_key or not secret_key:
            raise ValueError('MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required')
        self.bucket = os.getenv('MINIO_BRONZE_BUCKET', 'bronze')
        self.prefix = os.getenv('MINIO_BRONZE_PREFIX', 'environment-v1').strip('/')
        self.client = Minio(
            os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            access_key=access_key, secret_key=secret_key,
            secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true',
            http_client=urllib3.PoolManager(
                timeout=urllib3.Timeout(connect=5, read=30),
                retries=urllib3.Retry(total=2, backoff_factor=1,
                                      status_forcelist=[500, 502, 503, 504])),
        )
        # Provision the bucket separately; startup fails clearly if it is absent.
        if not self.client.bucket_exists(self.bucket):
            raise ValueError('MinIO Bronze bucket does not exist; provision it before starting')

    def write(self, topic, partition, first_offset, last_offset, lines):
        key = '/'.join(filter(None, [self.prefix, f'topic={topic}',
            f'partition={partition}', f'offsets={first_offset}-{last_offset}.jsonl.gz']))
        body = gzip.compress(b''.join(lines), mtime=0)
        self.client.put_object(self.bucket, key, io.BytesIO(body), len(body),
                               content_type='application/gzip')
        return key
