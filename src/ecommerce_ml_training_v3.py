import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import (
    RandomForestClassifier,
    GBTClassifier
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# CONFIGURAÇÕES
# ============================================================

BASE_PATH = (
    "s3://ecommerce-data-platform-mack-lab/"
    "ml/conversion_propensity_v3/"
)

TRAIN_PATH = BASE_PATH + "train/"
VALIDATION_PATH = BASE_PATH + "validation/"
TEST_PATH = BASE_PATH + "test/"

MODEL_BASE_PATH = BASE_PATH + "spark_models/"
METRICS_PATH = BASE_PATH + "metrics/"


# ============================================================
# FEATURES
# Mesma ordem usada no job de preparação
# ============================================================

FEATURES = [
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
    "is_night",

    "views_per_minute",
    "cart_value_per_item",
    "price_spread_ratio",
    "product_revisit_rate",
    "category_diversity_ratio",
    "brand_diversity_ratio",
    "cart_to_view_ratio",
    "cart_value_vs_avg_viewed_price",
    "seconds_per_view",
    "fast_cart_flag",
    "high_exploration_flag",
    "night_views_interaction",
    "log_time_to_first_cart",
    "log_cart_value",

    "user_prior_sessions",
    "user_prior_conversions",
    "user_prior_abandonments",
    "user_prior_conversion_rate",
    "user_prior_abandon_rate",
    "user_lifetime_hours"
]

TARGET = "converted"

COLUMNS = [TARGET] + FEATURES


# ============================================================
# LEITURA DOS CSVs
# ============================================================

def read_dataset(path):

    df = (
        spark.read
        .option("header", "false")
        .option("inferSchema", "true")
        .csv(path)
    )

    # Renomeia _c0, _c1... conforme a ordem usada na preparação
    for index, column_name in enumerate(COLUMNS):
        df = df.withColumnRenamed(
            f"_c{index}",
            column_name
        )

    df = df.select(
        *COLUMNS
    )

    df = df.withColumn(
        TARGET,
        F.col(TARGET).cast("double")
    )

    for feature in FEATURES:
        df = df.withColumn(
            feature,
            F.col(feature).cast("double")
        )

    return df


print("Lendo datasets...")

train_df = read_dataset(TRAIN_PATH)
validation_df = read_dataset(VALIDATION_PATH)
test_df = read_dataset(TEST_PATH)

print(f"Treino: {train_df.count()}")
print(f"Validação: {validation_df.count()}")
print(f"Teste: {test_df.count()}")


# ============================================================
# VECTOR ASSEMBLER
# ============================================================

assembler = VectorAssembler(
    inputCols=FEATURES,
    outputCol="features",
    handleInvalid="keep"
)

train_vector = assembler.transform(
    train_df
).select(
    TARGET,
    "features"
)

validation_vector = assembler.transform(
    validation_df
).select(
    TARGET,
    "features"
)

test_vector = assembler.transform(
    test_df
).select(
    TARGET,
    "features"
)


# ============================================================
# EVALUATOR
# ============================================================

roc_evaluator = BinaryClassificationEvaluator(
    labelCol=TARGET,
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

pr_evaluator = BinaryClassificationEvaluator(
    labelCol=TARGET,
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
)


# ============================================================
# RANDOM FOREST V3
# ============================================================

print()
print("========================================")
print("TREINANDO RANDOM FOREST V3")
print("========================================")

rf = RandomForestClassifier(
    labelCol=TARGET,
    featuresCol="features",
    numTrees=200,
    maxDepth=10,
    minInstancesPerNode=5,
    featureSubsetStrategy="sqrt",
    subsamplingRate=0.8,
    seed=42
)

rf_model = rf.fit(
    train_vector
)

rf_validation = rf_model.transform(
    validation_vector
)

rf_auc = roc_evaluator.evaluate(
    rf_validation
)

rf_pr_auc = pr_evaluator.evaluate(
    rf_validation
)

print(f"RF Validation ROC-AUC: {rf_auc:.6f}")
print(f"RF Validation PR-AUC : {rf_pr_auc:.6f}")


# ============================================================
# GRADIENT BOOSTED TREES V2
# ============================================================

print()
print("========================================")
print("TREINANDO GBT V3")
print("========================================")

gbt = GBTClassifier(
    labelCol=TARGET,
    featuresCol="features",
    maxIter=120,
    maxDepth=6,
    stepSize=0.05,
    minInstancesPerNode=5,
    subsamplingRate=0.8,
    seed=42
)

gbt_model = gbt.fit(
    train_vector
)

gbt_validation = gbt_model.transform(
    validation_vector
)

gbt_auc = roc_evaluator.evaluate(
    gbt_validation
)

gbt_pr_auc = pr_evaluator.evaluate(
    gbt_validation
)

print(f"GBT Validation ROC-AUC: {gbt_auc:.6f}")
print(f"GBT Validation PR-AUC : {gbt_pr_auc:.6f}")


# ============================================================
# ESCOLHER VENCEDOR USANDO APENAS VALIDATION
# ============================================================

if gbt_auc > rf_auc:

    winner_name = "GBTClassifier"
    winner_model = gbt_model

else:

    winner_name = "RandomForestClassifier"
    winner_model = rf_model


print()
print("========================================")
print(f"VENCEDOR NA VALIDAÇÃO: {winner_name}")
print("========================================")


# ============================================================
# AVALIAÇÃO FINAL NO TEST
# O TEST NÃO FOI USADO PARA ESCOLHER O MODELO
# ============================================================

test_predictions = winner_model.transform(
    test_vector
)

test_roc_auc = roc_evaluator.evaluate(
    test_predictions
)

test_pr_auc = pr_evaluator.evaluate(
    test_predictions
)


# ============================================================
# PRECISION / RECALL / F1 / ACCURACY
# ============================================================

metrics_counts = (
    test_predictions
    .select(
        F.col(TARGET).alias("label"),
        F.col("prediction")
    )
    .agg(
        F.sum(
            F.when(
                (F.col("label") == 1)
                & (F.col("prediction") == 1),
                1
            ).otherwise(0)
        ).alias("tp"),

        F.sum(
            F.when(
                (F.col("label") == 0)
                & (F.col("prediction") == 1),
                1
            ).otherwise(0)
        ).alias("fp"),

        F.sum(
            F.when(
                (F.col("label") == 1)
                & (F.col("prediction") == 0),
                1
            ).otherwise(0)
        ).alias("fn"),

        F.sum(
            F.when(
                (F.col("label") == 0)
                & (F.col("prediction") == 0),
                1
            ).otherwise(0)
        ).alias("tn")
    )
    .collect()[0]
)


tp = metrics_counts["tp"]
fp = metrics_counts["fp"]
fn = metrics_counts["fn"]
tn = metrics_counts["tn"]


accuracy = (
    (tp + tn)
    / (tp + tn + fp + fn)
)

precision = (
    tp / (tp + fp)
    if (tp + fp) > 0
    else 0.0
)

recall = (
    tp / (tp + fn)
    if (tp + fn) > 0
    else 0.0
)

f1 = (
    2 * precision * recall
    / (precision + recall)
    if (precision + recall) > 0
    else 0.0
)


print()
print("========================================")
print("RESULTADO FINAL - TEST")
print("========================================")

print(f"Modelo vencedor : {winner_name}")
print(f"Accuracy        : {accuracy:.6f}")
print(f"Precision       : {precision:.6f}")
print(f"Recall          : {recall:.6f}")
print(f"F1 Score        : {f1:.6f}")
print(f"ROC-AUC         : {test_roc_auc:.6f}")
print(f"PR-AUC          : {test_pr_auc:.6f}")

print()
print("Matriz de Confusão:")
print(f"TP: {tp}")
print(f"FP: {fp}")
print(f"FN: {fn}")
print(f"TN: {tn}")


# ============================================================
# SALVAR MODELO VENCEDOR NO S3
# ============================================================

winner_model.write().overwrite().save(
    MODEL_BASE_PATH + "best_model/"
)


# ============================================================
# SALVAR MÉTRICAS NO S3
# ============================================================

metrics_data = [
    (
        winner_name,
        float(rf_auc),
        float(rf_pr_auc),
        float(gbt_auc),
        float(gbt_pr_auc),
        float(accuracy),
        float(precision),
        float(recall),
        float(f1),
        float(test_roc_auc),
        float(test_pr_auc),
        int(tp),
        int(fp),
        int(fn),
        int(tn)
    )
]

metrics_columns = [
    "winner",
    "rf_validation_roc_auc",
    "rf_validation_pr_auc",
    "gbt_validation_roc_auc",
    "gbt_validation_pr_auc",
    "test_accuracy",
    "test_precision",
    "test_recall",
    "test_f1",
    "test_roc_auc",
    "test_pr_auc",
    "tp",
    "fp",
    "fn",
    "tn"
]

metrics_df = spark.createDataFrame(
    metrics_data,
    metrics_columns
)

(
    metrics_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(METRICS_PATH)
)


print()
print("========================================")
print("TREINAMENTO V2 FINALIZADO")
print("========================================")
print(f"Modelo: {MODEL_BASE_PATH}best_model/")
print(f"Métricas: {METRICS_PATH}")
print("========================================")


job.commit()