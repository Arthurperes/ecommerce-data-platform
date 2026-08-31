import boto3
from pathlib import Path

BUCKET_NAME = "ecommerce-data-platform-mack-lab"
AWS_REGION = "us-east-1"

LOCAL_FILE = "data/input/2019-Nov.csv"

S3_KEY = (
    "bronze/ecommerce_events/"
    "year=2019/month=11/"
    "2019-Nov.csv"
)


def upload_dataset():

    file_path = Path(LOCAL_FILE)

    if not file_path.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {file_path.resolve()}"
        )

    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION
    )

    print("Iniciando ingestão do dataset...")
    print(f"Arquivo local: {file_path}")
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Destino: s3://{BUCKET_NAME}/{S3_KEY}")

    try:
        s3.upload_file(
            str(file_path),
            BUCKET_NAME,
            S3_KEY
        )

        print("Upload concluído com sucesso!")

    except Exception as error:
        print("Erro durante o upload:")
        print(error)
        raise


if __name__ == "__main__":
    upload_dataset()