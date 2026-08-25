import os

from minio import Minio


CSV_PATH = "data/input/2019-Nov.csv"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

BUCKET_NAME = "bronze"

OBJECT_NAME = (
    "ecommerce_events/"
    "year=2019/"
    "month=11/"
    "2019-Nov.csv"
)


def create_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def create_bucket_if_needed(client):
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' criado.")

    else:
        print(f"Bucket '{BUCKET_NAME}' já existe.")


def upload_dataset(client):
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(
            f"Dataset não encontrado: {CSV_PATH}"
        )

    print("Iniciando upload do dataset...")

    client.fput_object(
        BUCKET_NAME,
        OBJECT_NAME,
        CSV_PATH,
        content_type="text/csv",
    )

    print("Upload concluído.")
    print(f"Destino: {BUCKET_NAME}/{OBJECT_NAME}")


def main():
    client = create_client()

    create_bucket_if_needed(client)
    upload_dataset(client)


if __name__ == "__main__":
    main()