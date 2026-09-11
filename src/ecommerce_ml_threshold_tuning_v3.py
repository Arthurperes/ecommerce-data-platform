import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassificationModel
from pyspark.ml.functions import vector_to_array
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.sparkContext.setLogLevel("WARN")


BASE_PATH = "s3://ecommerce-data-platform-mack-lab/ml/conversion_propensity_v2/"

VALIDATION_PATH = BASE_PATH + "validation/"
TEST_PATH = BASE_PATH + "test/"
MODEL_PATH = BASE_PATH + "spark_models/best_model/"
OUTPUT_PATH = BASE_PATH + "threshold_tuning_v3/"


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
    "log_cart_value"
]

TARGET = "converted"
COLUMNS = [TARGET] + FEATURES


def read_dataset(path):
    df = (
        spark.read
        .option("header", "false")
        .option("inferSchema", "true")
        .csv(path)
    )

    for index, column_name in enumerate(COLUMNS):
        df = df.withColumnRenamed(f"_c{index}", column_name)

    df = df.select(*COLUMNS)

    df = df.withColumn(TARGET, F.col(TARGET).cast("double"))

    for feature in FEATURES:
        df = df.withColumn(feature, F.col(feature).cast("double"))

    return df


def calculate_metrics(df, threshold):
    scored = df.withColumn(
        "custom_prediction",
        F.when(F.col("probability_1") >= threshold, 1.0).otherwise(0.0)
    )

    counts = (
        scored
        .agg(
            F.sum(F.when(
                (F.col(TARGET) == 1) &
                (F.col("custom_prediction") == 1), 1
            ).otherwise(0)).alias("tp"),

            F.sum(F.when(
                (F.col(TARGET) == 0) &
                (F.col("custom_prediction") == 1), 1
            ).otherwise(0)).alias("fp"),

            F.sum(F.when(
                (F.col(TARGET) == 1) &
                (F.col("custom_prediction") == 0), 1
            ).otherwise(0)).alias("fn"),

            F.sum(F.when(
                (F.col(TARGET) == 0) &
                (F.col("custom_prediction") == 0), 1
            ).otherwise(0)).alias("tn")
        )
        .collect()[0]
    )

    tp = counts["tp"]
    fp = counts["fp"]
    fn = counts["fn"]
    tn = counts["tn"]

    accuracy = (tp + tn) / max(1, tp + tn + fp + fn)
    precision = tp / max(1, tp + fp)
    recall = tp / max(1, tp + fn)

    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall > 0
        else 0.0
    )

    return {
        "threshold": threshold,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn
    }


validation_df = read_dataset(VALIDATION_PATH)
test_df = read_dataset(TEST_PATH)

assembler = VectorAssembler(
    inputCols=FEATURES,
    outputCol="features",
    handleInvalid="keep"
)

validation_vector = assembler.transform(validation_df).select(
    TARGET, "features"
)

test_vector = assembler.transform(test_df).select(
    TARGET, "features"
)


print("Carregando GBT V2 salvo...")

model = GBTClassificationModel.load(MODEL_PATH)


validation_predictions = (
    model
    .transform(validation_vector)
    .withColumn(
        "probability_1",
        vector_to_array("probability")[1]
    )
    .select(TARGET, "probability_1", "rawPrediction")
    .cache()
)


THRESHOLDS = [
    0.20,
    0.25,
    0.30,
    0.35,
    0.40,
    0.45,
    0.50
]


print()
print("========================================")
print("THRESHOLD TUNING - VALIDATION")
print("========================================")

validation_results = []

for threshold in THRESHOLDS:
    metrics = calculate_metrics(validation_predictions, threshold)
    validation_results.append(metrics)

    print(
        f"Threshold={threshold:.2f} | "
        f"Accuracy={metrics['accuracy']:.4f} | "
        f"Precision={metrics['precision']:.4f} | "
        f"Recall={metrics['recall']:.4f} | "
        f"F1={metrics['f1']:.4f}"
    )


best = max(
    validation_results,
    key=lambda x: x["f1"]
)

BEST_THRESHOLD = best["threshold"]


print()
print("========================================")
print(f"MELHOR THRESHOLD: {BEST_THRESHOLD:.2f}")
print(f"Validation F1   : {best['f1']:.4f}")
print(f"Validation Recall: {best['recall']:.4f}")
print("========================================")


test_predictions = (
    model
    .transform(test_vector)
    .withColumn(
        "probability_1",
        vector_to_array("probability")[1]
    )
    .select(TARGET, "probability_1", "rawPrediction")
    .cache()
)


test_metrics = calculate_metrics(
    test_predictions,
    BEST_THRESHOLD
)


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


test_roc_auc = roc_evaluator.evaluate(test_predictions)
test_pr_auc = pr_evaluator.evaluate(test_predictions)


print()
print("========================================")
print("RESULTADO FINAL - TEST")
print("========================================")

print(f"Threshold       : {BEST_THRESHOLD:.2f}")
print(f"Accuracy        : {test_metrics['accuracy']:.6f}")
print(f"Precision       : {test_metrics['precision']:.6f}")
print(f"Recall          : {test_metrics['recall']:.6f}")
print(f"F1 Score        : {test_metrics['f1']:.6f}")
print(f"ROC-AUC         : {test_roc_auc:.6f}")
print(f"PR-AUC          : {test_pr_auc:.6f}")

print()
print("Matriz de Confusão:")
print(f"TP: {test_metrics['tp']}")
print(f"FP: {test_metrics['fp']}")
print(f"FN: {test_metrics['fn']}")
print(f"TN: {test_metrics['tn']}")


results_df = spark.createDataFrame(validation_results)

results_df.coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(
    OUTPUT_PATH + "validation_thresholds/"
)


final_df = spark.createDataFrame([{
    "best_threshold": float(BEST_THRESHOLD),
    "accuracy": float(test_metrics["accuracy"]),
    "precision": float(test_metrics["precision"]),
    "recall": float(test_metrics["recall"]),
    "f1": float(test_metrics["f1"]),
    "roc_auc": float(test_roc_auc),
    "pr_auc": float(test_pr_auc),
    "tp": int(test_metrics["tp"]),
    "fp": int(test_metrics["fp"]),
    "fn": int(test_metrics["fn"]),
    "tn": int(test_metrics["tn"])
}])

final_df.coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(
    OUTPUT_PATH + "final_test/"
)


validation_predictions.unpersist()
test_predictions.unpersist()

job.commit()
