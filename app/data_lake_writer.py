import json
from io import BytesIO
from datetime import datetime
from app.minio_client import client, ensure_bucket

def save_json_to_datalake(bucket: str, prefix: str, data: dict) -> str:
    ensure_bucket(bucket)

    now = datetime.utcnow()
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%H%M%S%f")

    object_name = f"{prefix}/{date_path}/{timestamp}.json"

    payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
    payload_stream = BytesIO(payload)

    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=payload_stream,
        length=len(payload),
        content_type="application/json"
    )

    return object_name
