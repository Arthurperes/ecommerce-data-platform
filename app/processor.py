import json
from minio import Minio
from datetime import datetime
from io import BytesIO

# conexão MinIO
client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

RAW_BUCKET = "raw"
PROCESSED_BUCKET = "processed"

def list_objects(bucket, prefix):
    return client.list_objects(bucket, prefix=prefix, recursive=True)

def read_json(bucket, object_name):
    response = client.get_object(bucket, object_name)
    data = json.loads(response.read().decode("utf-8"))
    return data

def save_processed(prefix, data):
    now = datetime.utcnow()
    date_path = now.strftime("%Y/%m/%d")
    filename = now.strftime("%H%M%S%f") + ".json"

    object_name = f"{prefix}/{date_path}/{filename}"

    payload = json.dumps(data).encode("utf-8")

    client.put_object(
        PROCESSED_BUCKET,
        object_name,
        BytesIO(payload),
        length=len(payload),
        content_type="application/json"
    )

def process_price(data):
    return {
        "ticker": data["ticker"],
        "price": float(data["price"]),
        "volume": int(data["volume"]),
        "event_time": data["event_time"],
        "processed_at": datetime.utcnow().isoformat()
    }

def process_order(data):
    return {
        "order_id": data["order_id"],
        "ticker": data["ticker"],
        "quantity": int(data["quantity"]),
        "price": float(data["price"]),
        "total_value": float(data["total_value"]),
        "event_time": data["event_time"],
        "processed_at": datetime.utcnow().isoformat()
    }

def run():
    print("🚀 Iniciando processamento...")

    # PRICES
    for obj in list_objects(RAW_BUCKET, "prices/"):
        data = read_json(RAW_BUCKET, obj.object_name)
        processed = process_price(data)
        save_processed("prices", processed)
        print(f"✅ Processed PRICE: {obj.object_name}")

    # ORDERS
    for obj in list_objects(RAW_BUCKET, "orders/"):
        data = read_json(RAW_BUCKET, obj.object_name)
        processed = process_order(data)
        save_processed("orders", processed)
        print(f"✅ Processed ORDER: {obj.object_name}")

if __name__ == "__main__":
    run()
