import os
import tempfile
import shutil
import boto3

from pyspark.sql import SparkSession

BUCKET = "ecommerce-data-platform-mack-lab"
PREFIX = "gold/session_features/year=2019/month=11/"


spark = (
    SparkSession.builder
    .appName("InspectGold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

s3 = boto3.client("s3", region_name="us-east-1")

temp_dir = tempfile.mkdtemp()

try:
    # Para inspecionar o schema não precisamos baixar a Gold inteira.
    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=PREFIX
    )

    parquet_files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not parquet_files:
        raise RuntimeError("Nenhum Parquet encontrado na Gold.")

    # Baixa somente um part-file
    key = parquet_files[0]
    local_file = os.path.join(temp_dir, "sample.parquet")

    print(f"Baixando amostra: {key}")

    s3.download_file(
        BUCKET,
        key,
        local_file
    )

    df = spark.read.parquet(local_file)

    print("\n=== SCHEMA DA SESSION_FEATURES ===")
    df.printSchema()

    print("\n=== AMOSTRA ===")
    df.show(10, truncate=False)

finally:
    spark.stop()
    shutil.rmtree(temp_dir, ignore_errors=True)