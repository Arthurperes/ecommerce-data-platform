import os
import shutil
import tempfile
import boto3
import joblib
import pandas as pd

from pyspark.sql import SparkSession

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score


AWS_REGION = "us-east-1"
BUCKET_NAME = "ecommerce-data-platform-mack-lab"
ML_PREFIX = "gold/ml_features/year=2019/month=11/"

MAX_SAMPLE_ROWS = 50000
RANDOM_STATE = 42


# Features comportamentais.
# Não usamos converted nem is_abandoned.
PROFILE_FEATURES = [
    "num_views_before_cart",
    "unique_products_viewed",
    "unique_categories_viewed",
    "unique_brands_viewed",
    "avg_viewed_price",
    "viewed_price_range",
    "cart_value_at_first_cart",
    "num_cart_items_at_first_cart",
    "time_to_first_cart_sec",
    "view_to_cart_ratio",
    "hour_of_day",
    "is_night"
]


def get_s3_client():

    return boto3.client(
        "s3",
        region_name=AWS_REGION
    )


def download_ml_files(
    s3,
    local_dir
):

    print(
        "Baixando ML Features do S3..."
    )

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    count = 0

    for page in paginator.paginate(
        Bucket=BUCKET_NAME,
        Prefix=ML_PREFIX
    ):

        for obj in page.get(
            "Contents",
            []
        ):

            key = obj["Key"]

            if not key.endswith(
                ".parquet"
            ):
                continue

            filename = os.path.basename(
                key
            )

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
        f"Arquivos baixados: {count}"
    )


def main():

    # =========================================================
    # SPARK
    # =========================================================

    spark = (
        SparkSession.builder
        .appName(
            "CustomerProfileClustering"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    s3 = get_s3_client()

    temp_root = tempfile.mkdtemp()

    ml_local = os.path.join(
        temp_root,
        "ml_features"
    )

    os.makedirs(
        ml_local,
        exist_ok=True
    )

    try:

        # =====================================================
        # DOWNLOAD
        # =====================================================

        download_ml_files(
            s3,
            ml_local
        )

        print(
            "Lendo ML Features..."
        )

        df = spark.read.parquet(
            ml_local
        )

        total_rows = df.count()

        print(
            f"Total de sessões disponíveis: {total_rows}"
        )

        # =====================================================
        # AMOSTRA
        # =====================================================

        sample_rows = min(
            MAX_SAMPLE_ROWS,
            total_rows
        )

        print(
            f"Usando até {sample_rows} sessões..."
        )

        sample_df = (
            df
            .select(
                "session_id",
                "user_id",
                *PROFILE_FEATURES,
                "is_abandoned",
                "converted"
            )
            .limit(
                sample_rows
            )
        )

        print(
            "Convertendo amostra para Pandas..."
        )

        pdf = sample_df.toPandas()

        print(
            f"Linhas carregadas: {len(pdf)}"
        )

        # =====================================================
        # LIMPEZA
        # =====================================================

        for col in PROFILE_FEATURES:

            pdf[col] = pd.to_numeric(
                pdf[col],
                errors="coerce"
            )

        pdf[PROFILE_FEATURES] = (
            pdf[PROFILE_FEATURES]
            .replace(
                [
                    float("inf"),
                    float("-inf")
                ],
                0
            )
            .fillna(0)
        )

        # =====================================================
        # PADRONIZAÇÃO
        #
        # Necessária porque K-Means é sensível à escala.
        # Ex.: preço não pode dominar quantidade de views.
        # =====================================================

        print()
        print(
            "Padronizando features..."
        )

        scaler = StandardScaler()

        X_scaled = scaler.fit_transform(
            pdf[PROFILE_FEATURES]
        )

        # =====================================================
        # K-MEANS
        # =====================================================

        print(
            "Treinando K-Means com 3 clusters..."
        )

        kmeans = KMeans(
            n_clusters=3,
            random_state=RANDOM_STATE,
            n_init=20
        )

        pdf[
            "cluster"
        ] = kmeans.fit_predict(
            X_scaled
        )

        # =====================================================
        # TAMANHO DOS CLUSTERS
        # =====================================================

        print()
        print(
            "=== TAMANHO DOS CLUSTERS ==="
        )

        cluster_counts = (
            pdf[
                "cluster"
            ]
            .value_counts()
            .sort_index()
        )

        print(
            cluster_counts
        )

        # =====================================================
        # PERFIL MÉDIO DOS CLUSTERS
        # =====================================================

        cluster_summary = (
            pdf
            .groupby(
                "cluster"
            )[
                PROFILE_FEATURES
            ]
            .mean()
            .round(2)
        )

        print()
        print(
            "=== MÉDIAS POR CLUSTER ==="
        )

        print(
            cluster_summary.to_string()
        )

        # =====================================================
        # CONVERSÃO POR CLUSTER
        #
        # converted NÃO participou do K-Means.
        # Aqui usamos apenas depois para interpretar
        # os grupos encontrados.
        # =====================================================

        conversion_by_cluster = (
            pdf
            .groupby(
                "cluster"
            )
            .agg(
                sessions=(
                    "cluster",
                    "size"
                ),
                conversion_rate=(
                    "converted",
                    "mean"
                ),
                abandonment_rate=(
                    "is_abandoned",
                    "mean"
                )
            )
            .reset_index()
        )

        conversion_by_cluster[
            "conversion_rate"
        ] = (
            conversion_by_cluster[
                "conversion_rate"
            ] * 100
        ).round(2)

        conversion_by_cluster[
            "abandonment_rate"
        ] = (
            conversion_by_cluster[
                "abandonment_rate"
            ] * 100
        ).round(2)

        print()
        print(
            "=== CONVERSÃO POR CLUSTER ==="
        )

        print(
            conversion_by_cluster.to_string(
                index=False
            )
        )

        # =====================================================
        # SILHOUETTE
        #
        # Para ganhar tempo, calculamos em até 10 mil linhas.
        # =====================================================

        silhouette_rows = min(
            10000,
            len(pdf)
        )

        silhouette = silhouette_score(
            X_scaled[
                :silhouette_rows
            ],
            pdf[
                "cluster"
            ].iloc[
                :silhouette_rows
            ]
        )

        print()
        print(
            "=== QUALIDADE DO CLUSTERING ==="
        )

        print(
            f"Silhouette Score: {silhouette:.4f}"
        )

        # =====================================================
        # CENTROIDES NA ESCALA ORIGINAL
        # =====================================================

        centroids_original = (
            scaler.inverse_transform(
                kmeans.cluster_centers_
            )
        )

        centroids_df = pd.DataFrame(
            centroids_original,
            columns=PROFILE_FEATURES
        )

        centroids_df.insert(
            0,
            "cluster",
            range(3)
        )

        print()
        print(
            "=== CENTROIDES ==="
        )

        print(
            centroids_df
            .round(2)
            .to_string(
                index=False
            )
        )

        # =====================================================
        # SALVAR
        # =====================================================

        os.makedirs(
            "models",
            exist_ok=True
        )

        os.makedirs(
            "data/output",
            exist_ok=True
        )

        joblib.dump(
            {
                "kmeans": kmeans,
                "scaler": scaler,
                "features": PROFILE_FEATURES
            },
            "models/customer_profile_kmeans.joblib"
        )

        pdf.to_csv(
            "data/output/customer_clusters_sample.csv",
            index=False
        )

        cluster_summary.to_csv(
            "data/output/cluster_summary.csv"
        )

        conversion_by_cluster.to_csv(
            "data/output/cluster_conversion.csv",
            index=False
        )

        centroids_df.to_csv(
            "data/output/cluster_centroids.csv",
            index=False
        )

        pd.DataFrame(
            [
                {
                    "silhouette_score": silhouette
                }
            ]
        ).to_csv(
            "data/output/clustering_metrics.csv",
            index=False
        )

        print()
        print(
            "K-MEANS TREINADO COM SUCESSO"
        )

        print(
            "Modelo: models/customer_profile_kmeans.joblib"
        )

        print(
            "Clusters: data/output/customer_clusters_sample.csv"
        )

        print(
            "Resumo: data/output/cluster_summary.csv"
        )

        print(
            "Conversão: data/output/cluster_conversion.csv"
        )

        print(
            "Centroides: data/output/cluster_centroids.csv"
        )

    finally:

        spark.stop()

        shutil.rmtree(
            temp_root,
            ignore_errors=True
        )


if __name__ == "__main__":
    main()