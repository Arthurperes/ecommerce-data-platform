import os
import shutil
import tempfile

from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, to_timestamp, trim

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"

BRONZE_OBJECT = (
    "ecommerce_events/"
    "year=2019/"
    "month=11/"
    "2019-Nov.csv"
)

SILVER_PREFIX = "ecommerce_events/year=2019/month=11/"


def create_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def ensure_bucket(client, bucket_name):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' criado.")
    else:
        print(f"Bucket '{bucket_name}' já existe.")


def main():
    client = create_minio_client()
    ensure_bucket(client, SILVER_BUCKET)

    temp_dir = tempfile.mkdtemp()
    bronze_local_file = os.path.join(temp_dir, "2019-Nov.csv")
    silver_local_dir = os.path.join(temp_dir, "silver_output")

    print("Baixando arquivo da Bronze...")

    client.fget_object(
        BRONZE_BUCKET,
        BRONZE_OBJECT,
        bronze_local_file,
    )

    print("Arquivo Bronze baixado.")

    spark = (
        SparkSession.builder
        .appName("EcommerceBronzeToSilver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Lendo dataset com Spark...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(bronze_local_file)
    )

    bronze_count = df.count()
    print(f"Registros Bronze: {bronze_count}")

    df.printSchema()

    df_clean = (
        df
        .withColumn(
            "event_time",
            to_timestamp(col("event_time"))
        )
        .withColumn(
            "event_type",
            lower(trim(col("event_type")))
        )
        .withColumn(
            "product_id",
            col("product_id").cast("long")
        )
        .withColumn(
            "category_id",
            col("category_id").cast("long")
        )
        .withColumn(
            "price",
            col("price").cast("double")
        )
        .withColumn(
            "user_id",
            col("user_id").cast("long")
        )
    )

    df_clean = (
        df_clean
        .filter(col("event_time").isNotNull())
        .filter(
            col("event_type").isin(
                "view",
                "cart",
                "purchase"
            )
        )
        .filter(col("product_id").isNotNull())
        .filter(col("user_id").isNotNull())
        .filter(col("price") >= 0)
        .dropDuplicates()
    )

    silver_count = df_clean.count()

    print(f"Registros Silver: {silver_count}")
    print(f"Registros removidos: {bronze_count - silver_count}")

    print("Amostra dos dados tratados:")
    df_clean.show(10, truncate=False)

    print("Gravando Silver em Parquet...")

    (
        df_clean
        .write
        .mode("overwrite")
        .parquet(silver_local_dir)
    )

    print("Enviando Silver para o MinIO...")

    for root, _, files in os.walk(silver_local_dir):
        for file_name in files:

            if file_name.startswith("."):
                continue

            local_path = os.path.join(root, file_name)

            relative_path = os.path.relpath(
                local_path,
                silver_local_dir,
            )

            object_name = (
                SILVER_PREFIX
                + relative_path.replace("\\", "/")
            )

            client.fput_object(
                SILVER_BUCKET,
                object_name,
                local_path,
            )

    print("Silver criada com sucesso.")

    spark.stop()

    shutil.rmtree(
        temp_dir,
        ignore_errors=True,
    )


if __name__ == "__main__":
    main()