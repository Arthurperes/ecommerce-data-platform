import os
import shutil
import tempfile
import boto3

from pyspark.sql import SparkSession


AWS_REGION = "us-east-1"
BUCKET = "ecommerce-data-platform-mack-lab"

GOLD_PREFIX = "gold/session_features/year=2019/month=11/"
ML_SAMPLE_PREFIX = "gold/ml_sample/"


def main():

    spark = (
        SparkSession.builder
        .appName("ExportMLSample")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION
    )

    temp_root = tempfile.mkdtemp()

    gold_local = os.path.join(
        temp_root,
        "gold"
    )

    sample_local = os.path.join(
        temp_root,
        "ml_sample"
    )

    os.makedirs(
        gold_local,
        exist_ok=True
    )

    try:

        print("Baixando arquivos da Gold...")

        paginator = s3.get_paginator(
            "list_objects_v2"
        )

        count = 0

        for page in paginator.paginate(
            Bucket=BUCKET,
            Prefix=GOLD_PREFIX
        ):

            for obj in page.get("Contents", []):

                key = obj["Key"]

                if not key.endswith(".parquet"):
                    continue

                filename = os.path.basename(key)

                local_file = os.path.join(
                    gold_local,
                    filename
                )

                s3.download_file(
                    BUCKET,
                    key,
                    local_file
                )

                count += 1

        print(f"Arquivos Gold baixados: {count}")

        df = spark.read.parquet(
            gold_local
        )

        print(f"Sessoes disponiveis: {df.count()}")

        # Amostra reprodutivel de 100 mil sessoes
        sample = (
            df
            .orderBy("session_id")
            .limit(100000)
        )

        print(
            f"Sessoes da amostra ML: {sample.count()}"
        )

        print("\n=== AMOSTRA ML ===")

        sample.select(
            "session_id",
            "user_id",
            "total_cart_value",
            "num_cart_items",
            "num_views_before_cart",
            "view_to_cart_ratio",
            "session_duration_sec",
            "hour_of_day",
            "is_night",
            "is_abandoned"
        ).show(
            10,
            truncate=False
        )

        print("\n=== DISTRIBUICAO IS_ABANDONED ===")

        sample.groupBy(
            "is_abandoned"
        ).count().show()

        sample.write.mode(
            "overwrite"
        ).parquet(
            sample_local
        )

        print("Enviando amostra para S3...")

        for root, _, files in os.walk(
            sample_local
        ):

            for filename in files:

                if not filename.endswith(".parquet"):
                    continue

                local_file = os.path.join(
                    root,
                    filename
                )

                key = ML_SAMPLE_PREFIX + filename

                s3.upload_file(
                    local_file,
                    BUCKET,
                    key
                )

        print()
        print("AMOSTRA ML CRIADA COM SUCESSO")
        print(
            f"s3://{BUCKET}/{ML_SAMPLE_PREFIX}"
        )

    finally:

        spark.stop()

        shutil.rmtree(
            temp_root,
            ignore_errors=True
        )


if __name__ == "__main__":
    main()