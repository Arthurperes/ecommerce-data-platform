import os
import shutil
import tempfile
import boto3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


AWS_REGION = "us-east-1"
BUCKET_NAME = "ecommerce-data-platform-mack-lab"

SILVER_PREFIX = "silver/ecommerce_events/year=2019/month=11/"

# Gold analítica que já existia
GOLD_SESSION_PREFIX = "gold/session_features/year=2019/month=11/"
GOLD_FUNNEL_PREFIX = "gold/funnel_metrics/year=2019/month=11/"

# Nova Gold específica para Machine Learning
GOLD_ML_PREFIX = "gold/ml_features/year=2019/month=11/"


def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION
    )


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
            local_file = os.path.join(
                local_dir,
                filename
            )

            s3.download_file(
                BUCKET_NAME,
                key,
                local_file
            )

            count += 1

    print(
        f"Arquivos Silver baixados: {count}"
    )


def delete_s3_prefix(s3, prefix):

    print(
        f"Limpando prefixo antigo: s3://{BUCKET_NAME}/{prefix}"
    )

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    for page in paginator.paginate(
        Bucket=BUCKET_NAME,
        Prefix=prefix
    ):

        objects = page.get(
            "Contents",
            []
        )

        if not objects:
            continue

        delete_items = [
            {"Key": obj["Key"]}
            for obj in objects
        ]

        # S3 permite no máximo 1000 objetos por delete_objects
        for i in range(
            0,
            len(delete_items),
            1000
        ):

            batch = delete_items[
                i:i + 1000
            ]

            s3.delete_objects(
                Bucket=BUCKET_NAME,
                Delete={
                    "Objects": batch
                }
            )


def upload_directory(
    s3,
    local_dir,
    prefix
):

    for root, _, files in os.walk(
        local_dir
    ):

        for filename in files:

            if not filename.endswith(
                ".parquet"
            ):
                continue

            local_file = os.path.join(
                root,
                filename
            )

            relative_path = os.path.relpath(
                local_file,
                local_dir
            ).replace(
                "\\",
                "/"
            )

            s3_key = (
                f"{prefix}{relative_path}"
            )

            print(
                f"Upload: {s3_key}"
            )

            s3.upload_file(
                local_file,
                BUCKET_NAME,
                s3_key
            )


def main():

    spark = (
        SparkSession.builder
        .appName(
            "EcommerceGoldProcessorV2"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

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

    gold_ml_local = os.path.join(
        temp_root,
        "gold_ml_features"
    )

    os.makedirs(
        silver_local,
        exist_ok=True
    )

    try:

        # =====================================================
        # LEITURA DA SILVER
        # =====================================================

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
        # PRIMEIRO EVENTO DE CARRINHO
        # =====================================================

        print(
            "Identificando primeiro evento de carrinho..."
        )

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
        # GOLD ANALÍTICA
        # SESSION FEATURES
        # =====================================================

        print(
            "Criando session_features..."
        )

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
                        F.col(
                            "event_type"
                        ) == "cart",
                        F.col(
                            "price"
                        )
                    ).otherwise(0)
                ).alias(
                    "total_cart_value"
                ),

                # Quantidade total de eventos cart
                F.sum(
                    F.when(
                        F.col(
                            "event_type"
                        ) == "cart",
                        1
                    ).otherwise(0)
                ).alias(
                    "num_cart_items"
                ),

                # Views antes do primeiro cart
                F.sum(
                    F.when(
                        (
                            F.col(
                                "event_type"
                            ) == "view"
                        )
                        &
                        (
                            F.col(
                                "event_time"
                            )
                            <
                            F.col(
                                "first_cart_time"
                            )
                        ),
                        1
                    ).otherwise(0)
                ).alias(
                    "num_views_before_cart"
                ),

                # Compras observadas na sessão inteira
                F.sum(
                    F.when(
                        F.col(
                            "event_type"
                        ) == "purchase",
                        1
                    ).otherwise(0)
                ).alias(
                    "num_purchases"
                ),

                F.min(
                    "event_time"
                ).alias(
                    "session_start"
                ),

                F.max(
                    "event_time"
                ).alias(
                    "session_end"
                ),

                F.min(
                    "first_cart_time"
                ).alias(
                    "first_cart_time"
                )
            )
        )

        session_features = (
            session_features

            .withColumn(
                "session_duration_sec",
                F.col(
                    "session_end"
                ).cast("long")
                -
                F.col(
                    "session_start"
                ).cast("long")
            )

            .withColumn(
                "hour_of_day",
                F.hour(
                    "first_cart_time"
                )
            )

            .withColumn(
                "is_night",
                F.when(
                    (
                        F.col(
                            "hour_of_day"
                        ) >= 22
                    )
                    |
                    (
                        F.col(
                            "hour_of_day"
                        ) < 6
                    ),
                    1
                ).otherwise(0)
            )

            .withColumn(
                "view_to_cart_ratio",
                F.when(
                    F.col(
                        "num_cart_items"
                    ) > 0,

                    F.col(
                        "num_views_before_cart"
                    )
                    /
                    F.col(
                        "num_cart_items"
                    )

                ).otherwise(0.0)
            )

            # Target histórico
            .withColumn(
                "is_abandoned",
                F.when(
                    (
                        F.col(
                            "num_cart_items"
                        ) > 0
                    )
                    &
                    (
                        F.col(
                            "num_purchases"
                        ) == 0
                    ),
                    1
                ).otherwise(0)
            )

            # Target mais intuitivo para o ML
            .withColumn(
                "converted",
                1 - F.col(
                    "is_abandoned"
                )
            )

            .withColumnRenamed(
                "user_session",
                "session_id"
            )
        )

        session_features = (
            session_features
            .filter(
                F.col(
                    "num_cart_items"
                ) > 0
            )
        )

        print(
            "Sessões com carrinho:",
            session_features.count()
        )

        # =====================================================
        # GOLD ESPECÍFICA PARA MACHINE LEARNING
        #
        # IMPORTANTE:
        # somente eventos até o primeiro cart.
        #
        # Não usamos:
        # - purchase
        # - session_end
        # - duração depois do cart
        #
        # Isso evita DATA LEAKAGE.
        # =====================================================

        print(
            "Criando ML Features sem data leakage..."
        )

        ml_events = (
            df_with_cart_time

            .filter(
                F.col(
                    "first_cart_time"
                ).isNotNull()
            )

            .filter(
                F.col(
                    "event_time"
                )
                <=
                F.col(
                    "first_cart_time"
                )
            )
        )

        ml_features = (
            ml_events

            .groupBy(
                "user_session",
                "user_id"
            )

            .agg(

                # Primeiro evento observado da sessão
                F.min(
                    "event_time"
                ).alias(
                    "first_event_time"
                ),

                F.min(
                    "first_cart_time"
                ).alias(
                    "first_cart_time"
                ),

                # ---------------------------------------------
                # COMPORTAMENTO DE NAVEGAÇÃO
                # ---------------------------------------------

                F.sum(
                    F.when(
                        (
                            F.col(
                                "event_type"
                            ) == "view"
                        )
                        &
                        (
                            F.col(
                                "event_time"
                            )
                            <
                            F.col(
                                "first_cart_time"
                            )
                        ),
                        1
                    ).otherwise(0)
                ).alias(
                    "num_views_before_cart"
                ),

                F.countDistinct(
                    F.when(
                        (
                            F.col(
                                "event_type"
                            ) == "view"
                        )
                        &
                        (
                            F.col(
                                "event_time"
                            )
                            <
                            F.col(
                                "first_cart_time"
                            )
                        ),
                        F.col(
                            "product_id"
                        )
                    )
                ).alias(
                    "unique_products_viewed"
                ),

                F.countDistinct(
                    F.when(
                        (
                            F.col(
                                "event_type"
                            ) == "view"
                        )
                        &
                        (
                            F.col(
                                "event_time"
                            )
                            <
                            F.col(
                                "first_cart_time"
                            )
                        ),
                        F.col(
                            "category_id"
                        )
                    )
                ).alias(
                    "unique_categories_viewed"
                ),

                F.countDistinct(
                    F.when(
                        (
                            F.col(
                                "event_type"
                            ) == "view"
                        )
                        &
                        (
                            F.col(
                                "event_time"
                            )
                            <
                            F.col(
                                "first_cart_time"
                            )
                        ),
                        F.col(
                            "brand"
                        )
                    )
                ).alias(
                    "unique_brands_viewed"
                ),

                # ---------------------------------------------
                # PREÇO DOS PRODUTOS VISUALIZADOS
                # ---------------------------------------------

                F.avg(
                    F.when(
                        (
                            F.col(
                                "event_type"
                            ) == "view"
                        )
                        &
                        (
                            F.col(
                                "event_time"
                            )
                            <
                            F.col(
                                "first_cart_time"
                            )
                        ),
                        F.col(
                            "price"
                        )
                    )
                ).alias(
                    "avg_viewed_price"
                ),

                F.min(
                    F.when(
                        (
                            F.col(
                                "event_type"
                            ) == "view"
                        )
                        &
                        (
                            F.col(
                                "event_time"
                            )
                            <
                            F.col(
                                "first_cart_time"
                            )
                        ),
                        F.col(
                            "price"
                        )
                    )
                ).alias(
                    "min_viewed_price"
                ),

                F.max(
                    F.when(
                        (
                            F.col(
                                "event_type"
                            ) == "view"
                        )
                        &
                        (
                            F.col(
                                "event_time"
                            )
                            <
                            F.col(
                                "first_cart_time"
                            )
                        ),
                        F.col(
                            "price"
                        )
                    )
                ).alias(
                    "max_viewed_price"
                ),

                # ---------------------------------------------
                # ESTADO DO CARRINHO NO PRIMEIRO CART
                # ---------------------------------------------

                F.sum(
                    F.when(
                        (
                            F.col(
                                "event_type"
                            ) == "cart"
                        )
                        &
                        (
                            F.col(
                                "event_time"
                            )
                            ==
                            F.col(
                                "first_cart_time"
                            )
                        ),
                        F.col(
                            "price"
                        )
                    ).otherwise(0)
                ).alias(
                    "cart_value_at_first_cart"
                ),

                F.sum(
                    F.when(
                        (
                            F.col(
                                "event_type"
                            ) == "cart"
                        )
                        &
                        (
                            F.col(
                                "event_time"
                            )
                            ==
                            F.col(
                                "first_cart_time"
                            )
                        ),
                        1
                    ).otherwise(0)
                ).alias(
                    "num_cart_items_at_first_cart"
                ),

                F.avg(
                    F.when(
                        (
                            F.col(
                                "event_type"
                            ) == "cart"
                        )
                        &
                        (
                            F.col(
                                "event_time"
                            )
                            ==
                            F.col(
                                "first_cart_time"
                            )
                        ),
                        F.col(
                            "price"
                        )
                    )
                ).alias(
                    "avg_cart_item_price"
                ),

                F.max(
                    F.when(
                        (
                            F.col(
                                "event_type"
                            ) == "cart"
                        )
                        &
                        (
                            F.col(
                                "event_time"
                            )
                            ==
                            F.col(
                                "first_cart_time"
                            )
                        ),
                        F.col(
                            "price"
                        )
                    )
                ).alias(
                    "max_cart_item_price"
                )
            )
        )

        # =====================================================
        # FEATURES DERIVADAS PARA ML
        # =====================================================

        ml_features = (
            ml_features

            # Tempo entre início da sessão
            # e primeiro evento cart
            .withColumn(
                "time_to_first_cart_sec",
                F.col(
                    "first_cart_time"
                ).cast("long")
                -
                F.col(
                    "first_event_time"
                ).cast("long")
            )

            # Hora em que o usuário adicionou
            # o primeiro produto ao carrinho
            .withColumn(
                "hour_of_day",
                F.hour(
                    "first_cart_time"
                )
            )

            .withColumn(
                "is_night",
                F.when(
                    (
                        F.col(
                            "hour_of_day"
                        ) >= 22
                    )
                    |
                    (
                        F.col(
                            "hour_of_day"
                        ) < 6
                    ),
                    1
                ).otherwise(0)
            )

            # Quantas views ocorreram
            # para cada item adicionado no primeiro cart
            .withColumn(
                "view_to_cart_ratio",
                F.when(
                    F.col(
                        "num_cart_items_at_first_cart"
                    ) > 0,

                    F.col(
                        "num_views_before_cart"
                    )
                    /
                    F.col(
                        "num_cart_items_at_first_cart"
                    )

                ).otherwise(0.0)
            )

            # Amplitude dos preços pesquisados
            .withColumn(
                "viewed_price_range",
                F.when(
                    F.col(
                        "max_viewed_price"
                    ).isNotNull()
                    &
                    F.col(
                        "min_viewed_price"
                    ).isNotNull(),

                    F.col(
                        "max_viewed_price"
                    )
                    -
                    F.col(
                        "min_viewed_price"
                    )

                ).otherwise(0.0)
            )

            # Nulls de preço quando não houve view anterior
            .fillna(
                {
                    "avg_viewed_price": 0.0,
                    "min_viewed_price": 0.0,
                    "max_viewed_price": 0.0,
                    "avg_cart_item_price": 0.0,
                    "max_cart_item_price": 0.0
                }
            )

            .withColumnRenamed(
                "user_session",
                "session_id"
            )
        )

        # =====================================================
        # TARGET
        #
        # O target vem da sessão completa,
        # mas NÃO é usado como feature.
        #
        # converted = 1 -> terminou em compra
        # converted = 0 -> abandonou
        # =====================================================

        targets = (
            session_features

            .select(
                "session_id",
                "user_id",
                "is_abandoned",
                "converted"
            )
        )

        ml_features = (
            ml_features

            .join(
                targets,
                [
                    "session_id",
                    "user_id"
                ],
                "inner"
            )
        )

        # =====================================================
        # VISUALIZAÇÃO
        # =====================================================

        print()
        print(
            "=== AMOSTRA ML FEATURES ==="
        )

        ml_features.select(

            "session_id",
            "user_id",

            "num_views_before_cart",
            "unique_products_viewed",
            "unique_categories_viewed",
            "unique_brands_viewed",

            "avg_viewed_price",
            "viewed_price_range",

            "cart_value_at_first_cart",
            "num_cart_items_at_first_cart",
            "avg_cart_item_price",
            "max_cart_item_price",

            "time_to_first_cart_sec",

            "view_to_cart_ratio",

            "hour_of_day",
            "is_night",

            "is_abandoned",
            "converted"

        ).show(
            10,
            truncate=False
        )

        print()
        print(
            "=== DISTRIBUIÇÃO DO TARGET ==="
        )

        (
            ml_features
            .groupBy(
                "converted"
            )
            .count()
            .orderBy(
                "converted"
            )
            .show()
        )

        # =====================================================
        # FUNNEL
        # =====================================================

        print(
            "Criando métricas do funil..."
        )

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

        print(
            "Gravando Gold local..."
        )

        (
            session_features
            .write
            .mode("overwrite")
            .parquet(
                gold_session_local
            )
        )

        (
            ml_features
            .write
            .mode("overwrite")
            .parquet(
                gold_ml_local
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
        # LIMPEZA DOS PREFIXOS ANTIGOS
        # =====================================================

        delete_s3_prefix(
            s3,
            GOLD_SESSION_PREFIX
        )

        delete_s3_prefix(
            s3,
            GOLD_ML_PREFIX
        )

        delete_s3_prefix(
            s3,
            GOLD_FUNNEL_PREFIX
        )

        # =====================================================
        # UPLOAD S3
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
            "Enviando ml_features para S3..."
        )

        upload_directory(
            s3,
            gold_ml_local,
            GOLD_ML_PREFIX
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
        print(
            "GOLD V2 CRIADA COM SUCESSO"
        )

        print(
            f"s3://{BUCKET_NAME}/{GOLD_SESSION_PREFIX}"
        )

        print(
            f"s3://{BUCKET_NAME}/{GOLD_ML_PREFIX}"
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