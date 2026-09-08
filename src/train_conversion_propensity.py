import os
import shutil
import tempfile
import boto3
import joblib

import pandas as pd

from pyspark.sql import SparkSession

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix,
    classification_report
)


AWS_REGION = "us-east-1"
BUCKET_NAME = "ecommerce-data-platform-mack-lab"
ML_PREFIX = "gold/ml_features/year=2019/month=11/"

# Reduzido para evitar estouro de memória no container Spark
MAX_SAMPLE_ROWS = 50000

RANDOM_STATE = 42


FEATURES = [
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
    "is_night"
]

TARGET = "converted"


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
        "Baixando arquivos ML Features do S3..."
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
            "TrainConversionPropensity"
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
        # DOWNLOAD DA GOLD ML
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
        #
        # IMPORTANTE:
        # removemos orderBy porque ordenar 1,7 milhão
        # de sessões era desnecessariamente pesado.
        # =====================================================

        sample_rows = min(
            MAX_SAMPLE_ROWS,
            total_rows
        )

        print(
            f"Usando amostra de até {sample_rows} sessões..."
        )

        sample_df = (
            df
            .select(
                *FEATURES,
                TARGET,
                "session_id",
                "user_id"
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
            f"Linhas carregadas em Pandas: {len(pdf)}"
        )

        # =====================================================
        # LIMPEZA DOS DADOS
        # =====================================================

        for col in FEATURES:

            pdf[col] = pd.to_numeric(
                pdf[col],
                errors="coerce"
            )

        pdf[FEATURES] = (
            pdf[FEATURES]
            .replace(
                [
                    float("inf"),
                    float("-inf")
                ],
                0
            )
            .fillna(0)
        )

        pdf[TARGET] = (
            pd.to_numeric(
                pdf[TARGET],
                errors="coerce"
            )
            .fillna(0)
            .astype(int)
        )

        # =====================================================
        # DISTRIBUIÇÃO DO TARGET
        # =====================================================

        print()
        print(
            "=== DISTRIBUIÇÃO DO TARGET NA AMOSTRA ==="
        )

        target_distribution = (
            pdf[TARGET]
            .value_counts()
            .sort_index()
        )

        print(
            target_distribution
        )

        # =====================================================
        # DEFINIÇÃO DE X E Y
        # =====================================================

        X = pdf[
            FEATURES
        ]

        y = pdf[
            TARGET
        ]

        # =====================================================
        # TREINO / TESTE
        #
        # stratify mantém a proporção entre
        # converted = 0 e converted = 1.
        # =====================================================

        (
            X_train,
            X_test,
            y_train,
            y_test
        ) = train_test_split(
            X,
            y,
            test_size=0.20,
            random_state=RANDOM_STATE,
            stratify=y
        )

        print()
        print(
            f"Treino: {len(X_train)} linhas"
        )

        print(
            f"Teste: {len(X_test)} linhas"
        )

        # =====================================================
        # MODELO
        # =====================================================

        print()
        print(
            "Treinando Random Forest..."
        )

        model = RandomForestClassifier(
            n_estimators=120,
            max_depth=10,
            min_samples_leaf=10,
            class_weight="balanced",
            random_state=RANDOM_STATE,
            n_jobs=-1
        )

        model.fit(
            X_train,
            y_train
        )

        # =====================================================
        # PREDIÇÕES
        # =====================================================

        predictions = model.predict(
            X_test
        )

        probabilities = (
            model.predict_proba(
                X_test
            )[:, 1]
        )

        # =====================================================
        # MÉTRICAS
        # =====================================================

        accuracy = accuracy_score(
            y_test,
            predictions
        )

        precision = precision_score(
            y_test,
            predictions,
            zero_division=0
        )

        recall = recall_score(
            y_test,
            predictions,
            zero_division=0
        )

        f1 = f1_score(
            y_test,
            predictions,
            zero_division=0
        )

        roc_auc = roc_auc_score(
            y_test,
            probabilities
        )

        cm = confusion_matrix(
            y_test,
            predictions
        )

        print()
        print(
            "=== MÉTRICAS DO MODELO ==="
        )

        print(
            f"Accuracy : {accuracy:.4f}"
        )

        print(
            f"Precision: {precision:.4f}"
        )

        print(
            f"Recall   : {recall:.4f}"
        )

        print(
            f"F1 Score : {f1:.4f}"
        )

        print(
            f"ROC-AUC  : {roc_auc:.4f}"
        )

        # =====================================================
        # MATRIZ DE CONFUSÃO
        # =====================================================

        print()
        print(
            "=== MATRIZ DE CONFUSÃO ==="
        )

        print(
            cm
        )

        # =====================================================
        # CLASSIFICATION REPORT
        # =====================================================

        print()
        print(
            "=== CLASSIFICATION REPORT ==="
        )

        print(
            classification_report(
                y_test,
                predictions,
                digits=4,
                zero_division=0
            )
        )

        # =====================================================
        # FEATURE IMPORTANCE
        # =====================================================

        feature_importance = (
            pd.DataFrame(
                {
                    "feature": FEATURES,
                    "importance": (
                        model.feature_importances_
                    )
                }
            )
            .sort_values(
                "importance",
                ascending=False
            )
        )

        print()
        print(
            "=== IMPORTÂNCIA DAS FEATURES ==="
        )

        print(
            feature_importance.to_string(
                index=False
            )
        )

        # =====================================================
        # SCORE DE PROPENSÃO
        #
        # probability da classe converted = 1
        # =====================================================

        scored_test = X_test.copy()

        scored_test[
            "actual_converted"
        ] = y_test.values

        scored_test[
            "conversion_score"
        ] = probabilities

        # Faixa de score
        scored_test[
            "score_band"
        ] = pd.cut(
            scored_test[
                "conversion_score"
            ],
            bins=[
                -0.01,
                0.25,
                0.75,
                1.00
            ],
            labels=[
                "Baixa Propensao",
                "Media Propensao",
                "Alta Propensao"
            ]
        )

        print()
        print(
            "=== EXEMPLO DE SCORES ==="
        )

        print(
            scored_test[
                [
                    "actual_converted",
                    "conversion_score",
                    "score_band"
                ]
            ]
            .head(20)
            .to_string(
                index=False
            )
        )

        # =====================================================
        # DISTRIBUIÇÃO DOS SCORES
        # =====================================================

        print()
        print(
            "=== DISTRIBUIÇÃO DAS FAIXAS DE SCORE ==="
        )

        print(
            scored_test[
                "score_band"
            ]
            .value_counts()
            .sort_index()
        )

        # =====================================================
        # SALVAR RESULTADOS
        # =====================================================

        os.makedirs(
            "models",
            exist_ok=True
        )

        os.makedirs(
            "data/output",
            exist_ok=True
        )

        model_path = (
            "models/"
            "conversion_propensity_model.joblib"
        )

        joblib.dump(
            {
                "model": model,
                "features": FEATURES
            },
            model_path
        )

        score_output = (
            "data/output/"
            "conversion_scores_sample.csv"
        )

        scored_test.to_csv(
            score_output,
            index=False
        )

        importance_output = (
            "data/output/"
            "feature_importance.csv"
        )

        feature_importance.to_csv(
            importance_output,
            index=False
        )

        # =====================================================
        # MÉTRICAS EM CSV
        # =====================================================

        metrics_output = (
            "data/output/"
            "model_metrics.csv"
        )

        metrics_df = pd.DataFrame(
            [
                {
                    "accuracy": accuracy,
                    "precision": precision,
                    "recall": recall,
                    "f1_score": f1,
                    "roc_auc": roc_auc
                }
            ]
        )

        metrics_df.to_csv(
            metrics_output,
            index=False
        )

        # =====================================================
        # FINAL
        # =====================================================

        print()
        print(
            "MODELO TREINADO COM SUCESSO"
        )

        print(
            f"Modelo: {model_path}"
        )

        print(
            f"Scores: {score_output}"
        )

        print(
            f"Feature importance: {importance_output}"
        )

        print(
            f"Métricas: {metrics_output}"
        )

    finally:

        spark.stop()

        shutil.rmtree(
            temp_root,
            ignore_errors=True
        )


if __name__ == "__main__":
    main()