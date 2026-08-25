from minio import Minio

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def ensure_bucket(bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
