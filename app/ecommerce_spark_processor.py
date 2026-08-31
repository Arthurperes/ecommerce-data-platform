import os
import shutil
import tempfile

import boto3

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    to_timestamp,
    trim,
)


AWS_REGION = "us-east-1"

BUCKET_NAME = "ecommerce-data-platform-mack-lab"

BRONZE_OBJECT = (
    "bronze/ecommerce_events/"
    "year=2019/"
    "month=11/"
    "2019-Nov.csv"
)

SILVER_PREFIX = (
    "silver/ecommerce_events/"
    "year=2019/"
    "month=11/"
)


def create_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION
    )


def upload_directory_to_s3(
    client,
    local_directory,
    bucket,
    prefix
):
    for root, _, files in os.walk(local_directory):

        for file_name in files:

            if file_name.startswith("."):
                continue

            local_path = os.path.join(
                root,
                file_name
            )

            relative_path = os.path.relpath(
                local_path,
                local_directory
            )

            s3_key = (
                prefix
                + relative_path.replace("\\", "/")
            )

            print(f"Enviando: {s3_key}")

            client.upload_file(
                local_path,
                bucket,
                s3_key
            )


def main():

    s3 = create_s3_client()

    temp_dir = tempfile.mkdtemp()

    bronze_local_file = os.path.join(
        temp_dir,
        "2019-Nov.csv"
    )

    silver_local_dir = os.path.join(
        temp_dir,
        "silver_output"
    )

    try:

        # -----------------------------
        # Download Bronze
        # -----------------------------

        print("Baixando Bronze do Amazon S3...")

        print(
            f"s3://{BUCKET_NAME}/{BRONZE_OBJECT}"
        )

        s3.download_file(
            BUCKET_NAME,
            BRONZE_OBJECT,
            bronze_local_file
        )

        print("Arquivo Bronze baixado.")

        # -----------------------------
        # Spark
        # -----------------------------

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

        print(
            f"Registros Bronze: {bronze_count}"
        )

        df.printSchema()

        # -----------------------------
        # Tratamento Silver
        # -----------------------------

        df_clean = (
            df
            .withColumn(
                "event_time",
                to_timestamp(
                    col("event_time")
                )
            )
            .withColumn(
                "event_type",
                lower(
                    trim(
                        col("event_type")
                    )
                )
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
            .filter(
                col("event_time").isNotNull()
            )
            .filter(
                col("event_type").isin(
                    "view",
                    "cart",
                    "purchase"
                )
            )
            .filter(
                col("product_id").isNotNull()
            )
            .filter(
                col("user_id").isNotNull()
            )
            .filter(
                col("price") >= 0
            )
            .dropDuplicates()
        )

        # -----------------------------
        # Contagem
        # -----------------------------

        silver_count = df_clean.count()

        removed_count = (
            bronze_count - silver_count
        )

        print(
            f"Registros Silver: {silver_count}"
        )

        print(
            f"Registros removidos: {removed_count}"
        )

        print(
            "Amostra dos dados tratados:"
        )

        df_clean.show(
            10,
            truncate=False
        )

        # -----------------------------
        # Parquet
        # -----------------------------

        print(
            "Gravando Silver em Parquet..."
        )

        (
            df_clean
            .write
            .mode("overwrite")
            .parquet(silver_local_dir)
        )

        # -----------------------------
        # Upload Silver
        # -----------------------------

        print(
            "Enviando Silver para o S3..."
        )

        upload_directory_to_s3(
            s3,
            silver_local_dir,
            BUCKET_NAME,
            SILVER_PREFIX
        )

        print(
            "Silver criada com sucesso."
        )

        print(
            "Destino:"
        )

        print(
            f"s3://{BUCKET_NAME}/{SILVER_PREFIX}"
        )

        spark.stop()

    finally:

        shutil.rmtree(
            temp_dir,
            ignore_errors=True
        )


if __name__ == "__main__":
    main()