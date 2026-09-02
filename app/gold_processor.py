import os
import shutil
import tempfile
import boto3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


AWS_REGION = "us-east-1"
BUCKET_NAME = "ecommerce-data-platform-mack-lab"

SILVER_PREFIX = "silver/ecommerce_events/year=2019/month=11/"
GOLD_SESSION_PREFIX = "gold/session_features/year=2019/month=11/"
GOLD_FUNNEL_PREFIX = "gold/funnel_metrics/year=2019/month=11/"


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def download_silver_files(s3, local_dir):
    print("Baixando arquivos Silver do S3...")

    paginator = s3.get_paginator("list_objects_v2")

    count = 0

    for page in paginator.paginate(
        Bucket=BUCKET_NAME,
        Prefix=SILVER_PREFIX
    ):
        for obj in page.get("Contents", []):

            key = obj["Key"]

            if not key.endswith(".parquet"):
                continue

            filename = os.path.basename(key)
            local_file = os.path.join(local_dir, filename)

            s3.download_file(
                BUCKET_NAME,
                key,
                local_file
            )

            count += 1

    print(f"Arquivos Silver baixados: {count}")


def upload_directory(s3, local_dir, prefix):
    for root, _, files in os.walk(local_dir):

        for filename in files:

            if not filename.endswith(".parquet"):
                continue

            local_file = os.path.join(root, filename)

            relative_path = os.path.relpath(
                local_file,
                local_dir
            ).replace("\\", "/")

            s3_key = f"{prefix}{relative_path}"

            print(f"Upload: {s3_key}")

            s3.upload_file(
                local_file,
                BUCKET_NAME,
                s3_key
            )


def main():

    spark = (
        SparkSession.builder
        .appName("EcommerceGoldProcessor")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    s3 = get_s3_client()

    temp_root = tempfile.mkdtemp()

    silver_local = os.path.join(
        temp_root,
        "silver"
    )

    gold_session_local = os.path.join(
        temp_root,
        "gold_session_features"
    )

    gold_funnel_local = os.path.join(
        temp_root,
        "gold_funnel"
    )

    os.makedirs(silver_local, exist_ok=True)

    try:

        download_silver_files(
            s3,
            silver_local
        )

        print("Lendo Silver...")

        df = spark.read.parquet(
            silver_local
        )

        print(
            f"Eventos Silver: {df.count()}"
        )

        # =====================================================
        # PRIMEIRO EVENTO DE CARRINHO DA SESSAO
        # =====================================================

        print("Identificando primeiro evento de carrinho...")

        first_cart = (
            df
            .filter(
                F.col("event_type") == "cart"
            )
            .groupBy(
                "user_session",
                "user_id"
            )
            .agg(
                F.min(
                    "event_time"
                ).alias(
                    "first_cart_time"
                )
            )
        )

        df_with_cart_time = (
            df
            .join(
                first_cart,
                [
                    "user_session",
                    "user_id"
                ],
                "left"
            )
        )

        # =====================================================
        # SESSION FEATURES
        # =====================================================

        print("Criando session_features...")

        session_features = (
            df_with_cart_time
            .groupBy(
                "user_session",
                "user_id"
            )
            .agg(

                # Valor total adicionado ao carrinho
                F.sum(
                    F.when(
                        F.col("event_type") == "cart",
                        F.col("price")
                    ).otherwise(0)
                ).alias(
                    "total_cart_value"
                ),

                # Quantidade de eventos de carrinho
                F.sum(
                    F.when(
                        F.col("event_type") == "cart",
                        1
                    ).otherwise(0)
                ).alias(
                    "num_cart_items"
                ),

                # Views ocorridos antes do primeiro cart
                F.sum(
                    F.when(
                        (
                            F.col("event_type") == "view"
                        )
                        &
                        (
                            F.col("event_time")
                            <
                            F.col("first_cart_time")
                        ),
                        1
                    ).otherwise(0)
                ).alias(
                    "num_views_before_cart"
                ),

                # Total de compras observadas na sessão
                F.sum(
                    F.when(
                        F.col("event_type") == "purchase",
                        1
                    ).otherwise(0)
                ).alias(
                    "num_purchases"
                ),

                # Início da sessão
                F.min(
                    "event_time"
                ).alias(
                    "session_start"
                ),

                # Fim da sessão
                F.max(
                    "event_time"
                ).alias(
                    "session_end"
                ),

                # Horário do primeiro carrinho
                F.min(
                    "first_cart_time"
                ).alias(
                    "first_cart_time"
                )
            )
        )

        # =====================================================
        # FEATURES DERIVADAS
        # =====================================================

        session_features = (
            session_features

            # Duração total da sessão
            .withColumn(
                "session_duration_sec",
                F.col("session_end").cast("long")
                -
                F.col("session_start").cast("long")
            )

            # Hora do primeiro carrinho
            .withColumn(
                "hour_of_day",
                F.hour(
                    "first_cart_time"
                )
            )

            # Flag de período noturno
            .withColumn(
                "is_night",
                F.when(
                    (
                        F.col("hour_of_day") >= 22
                    )
                    |
                    (
                        F.col("hour_of_day") < 6
                    ),
                    1
                ).otherwise(0)
            )

            # Relação entre quantidade de views e itens no carrinho
            .withColumn(
                "view_to_cart_ratio",
                F.when(
                    F.col("num_cart_items") > 0,
                    F.col("num_views_before_cart")
                    /
                    F.col("num_cart_items")
                ).otherwise(0.0)
            )

            # Target de abandono
            .withColumn(
                "is_abandoned",
                F.when(
                    (
                        F.col("num_cart_items") > 0
                    )
                    &
                    (
                        F.col("num_purchases") == 0
                    ),
                    1
                ).otherwise(0)
            )

            .withColumnRenamed(
                "user_session",
                "session_id"
            )
        )

        # Mantém somente sessões que tiveram cart
        session_features = (
            session_features
            .filter(
                F.col("num_cart_items") > 0
            )
        )

        print(
            "Sessões com carrinho:",
            session_features.count()
        )

        print("\n=== AMOSTRA SESSION_FEATURES ===")

        session_features.select(
            "session_id",
            "user_id",
            "total_cart_value",
            "num_cart_items",
            "num_views_before_cart",
            "view_to_cart_ratio",
            "num_purchases",
            "session_duration_sec",
            "hour_of_day",
            "is_night",
            "is_abandoned"
        ).show(
            10,
            truncate=False
        )

        # =====================================================
        # FUNNEL METRICS
        # =====================================================

        print("Criando métricas do funil...")

        funnel = (
            df
            .groupBy(
                "event_type"
            )
            .count()
            .orderBy(
                "event_type"
            )
        )

        funnel.show()

        # =====================================================
        # GRAVAÇÃO LOCAL
        # =====================================================

        print("Gravando Gold local...")

        (
            session_features
            .write
            .mode("overwrite")
            .parquet(
                gold_session_local
            )
        )

        (
            funnel
            .write
            .mode("overwrite")
            .parquet(
                gold_funnel_local
            )
        )

        # =====================================================
        # UPLOAD PARA S3
        # =====================================================

        print(
            "Enviando session_features para S3..."
        )

        upload_directory(
            s3,
            gold_session_local,
            GOLD_SESSION_PREFIX
        )

        print(
            "Enviando funnel_metrics para S3..."
        )

        upload_directory(
            s3,
            gold_funnel_local,
            GOLD_FUNNEL_PREFIX
        )

        print()
        print("GOLD CRIADA COM SUCESSO")

        print(
            f"s3://{BUCKET_NAME}/{GOLD_SESSION_PREFIX}"
        )

        print(
            f"s3://{BUCKET_NAME}/{GOLD_FUNNEL_PREFIX}"
        )

    finally:

        spark.stop()

        shutil.rmtree(
            temp_root,
            ignore_errors=True
        )


if __name__ == "__main__":
    main()