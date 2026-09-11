import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.window import Window


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# CONFIG
# ============================================================

INPUT_PATH = (
    "s3://ecommerce-data-platform-mack-lab/"
    "gold_glue_test/ml_features/"
)

OUTPUT_BASE = (
    "s3://ecommerce-data-platform-mack-lab/"
    "ml/conversion_propensity_v3/"
)

TARGET = "converted"


BASE_FEATURES = [
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


# ============================================================
# READ GOLD
# ============================================================

print("Lendo Gold ML Features...")

df = spark.read.parquet(INPUT_PATH)

df = df.select(
    "session_id",
    "user_id",
    "first_event_time",
    "first_cart_time",
    "converted",
    "is_abandoned",
    *BASE_FEATURES
)

df = df.filter(
    F.col("converted").isin(0, 1)
    & F.col("user_id").isNotNull()
    & F.col("first_cart_time").isNotNull()
)


# ============================================================
# HISTORICO DO USUARIO SEM LEAKAGE
# somente sessoes ANTERIORES
# ============================================================

history_window = (
    Window
    .partitionBy("user_id")
    .orderBy(
        F.col("first_cart_time"),
        F.col("session_id")
    )
    .rowsBetween(
        Window.unboundedPreceding,
        -1
    )
)


df = df.withColumn(
    "user_prior_sessions",
    F.count("session_id").over(history_window)
)

df = df.withColumn(
    "user_prior_conversions",
    F.coalesce(
        F.sum("converted").over(history_window),
        F.lit(0)
    )
)

df = df.withColumn(
    "user_prior_abandonments",
    F.coalesce(
        F.sum("is_abandoned").over(history_window),
        F.lit(0)
    )
)


df = df.withColumn(
    "user_prior_conversion_rate",
    F.when(
        F.col("user_prior_sessions") > 0,
        F.col("user_prior_conversions")
        / F.col("user_prior_sessions")
    ).otherwise(0.0)
)


df = df.withColumn(
    "user_prior_abandon_rate",
    F.when(
        F.col("user_prior_sessions") > 0,
        F.col("user_prior_abandonments")
        / F.col("user_prior_sessions")
    ).otherwise(0.0)
)


# Primeira sessao anterior conhecida do usuario
df = df.withColumn(
    "user_first_prior_cart_time",
    F.min("first_cart_time").over(history_window)
)


df = df.withColumn(
    "user_lifetime_hours",
    F.when(
        F.col("user_first_prior_cart_time").isNotNull(),
        (
            F.col("first_cart_time").cast("long")
            - F.col("user_first_prior_cart_time").cast("long")
        ) / 3600.0
    ).otherwise(0.0)
)


HISTORY_FEATURES = [
    "user_prior_sessions",
    "user_prior_conversions",
    "user_prior_abandonments",
    "user_prior_conversion_rate",
    "user_prior_abandon_rate",
    "user_lifetime_hours"
]


# ============================================================
# V2 FEATURE ENGINEERING
# ============================================================

def safe_div(numerator, denominator):
    return (
        F.when(
            F.col(denominator) > 0,
            F.col(numerator) / F.col(denominator)
        )
        .otherwise(F.lit(0.0))
    )


for col_name in BASE_FEATURES:
    df = df.withColumn(
        col_name,
        F.col(col_name).cast("double")
    )

df = df.fillna(0, subset=BASE_FEATURES)


df = df.withColumn(
    "views_per_minute",
    F.when(
        F.col("time_to_first_cart_sec") > 0,
        F.col("num_views_before_cart")
        / (F.col("time_to_first_cart_sec") / 60.0)
    ).otherwise(0.0)
)

df = df.withColumn(
    "cart_value_per_item",
    safe_div(
        "cart_value_at_first_cart",
        "num_cart_items_at_first_cart"
    )
)

df = df.withColumn(
    "price_spread_ratio",
    safe_div(
        "viewed_price_range",
        "avg_viewed_price"
    )
)

df = df.withColumn(
    "product_revisit_rate",
    safe_div(
        "num_views_before_cart",
        "unique_products_viewed"
    )
)

df = df.withColumn(
    "category_diversity_ratio",
    safe_div(
        "unique_categories_viewed",
        "unique_products_viewed"
    )
)

df = df.withColumn(
    "brand_diversity_ratio",
    safe_div(
        "unique_brands_viewed",
        "unique_products_viewed"
    )
)

df = df.withColumn(
    "cart_to_view_ratio",
    safe_div(
        "num_cart_items_at_first_cart",
        "num_views_before_cart"
    )
)

df = df.withColumn(
    "cart_value_vs_avg_viewed_price",
    safe_div(
        "cart_value_at_first_cart",
        "avg_viewed_price"
    )
)

df = df.withColumn(
    "seconds_per_view",
    safe_div(
        "time_to_first_cart_sec",
        "num_views_before_cart"
    )
)

df = df.withColumn(
    "fast_cart_flag",
    F.when(
        (F.col("time_to_first_cart_sec") <= 120)
        & (F.col("num_views_before_cart") <= 5),
        1.0
    ).otherwise(0.0)
)

df = df.withColumn(
    "high_exploration_flag",
    F.when(
        (F.col("num_views_before_cart") >= 10)
        & (F.col("unique_products_viewed") >= 5),
        1.0
    ).otherwise(0.0)
)

df = df.withColumn(
    "night_views_interaction",
    F.col("is_night")
    * F.col("num_views_before_cart")
)

df = df.withColumn(
    "log_time_to_first_cart",
    F.log1p(
        F.greatest(
            F.col("time_to_first_cart_sec"),
            F.lit(0.0)
        )
    )
)

df = df.withColumn(
    "log_cart_value",
    F.log1p(
        F.greatest(
            F.col("cart_value_at_first_cart"),
            F.lit(0.0)
        )
    )
)


NEW_FEATURES = [
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


ALL_FEATURES = (
    BASE_FEATURES
    + NEW_FEATURES
    + HISTORY_FEATURES
)


# ============================================================
# FINAL CLEANING
# ============================================================

for col_name in ALL_FEATURES:

    df = df.withColumn(
        col_name,
        F.col(col_name).cast("double")
    )

    df = df.withColumn(
        col_name,
        F.when(
            F.isnan(F.col(col_name))
            | F.col(col_name).isNull(),
            F.lit(0.0)
        ).otherwise(F.col(col_name))
    )


df = df.select(
    F.col(TARGET).cast("int").alias(TARGET),
    *ALL_FEATURES
)


print("========================================")
print("ML PREPARATION V3")
print("========================================")

print(f"Total features V3: {len(ALL_FEATURES)}")

print("Features historicas:")
for feature in HISTORY_FEATURES:
    print(feature)

print("Distribuicao target:")
df.groupBy(TARGET).count().orderBy(TARGET).show()


# ============================================================
# SPLIT
# mesma seed do V2
# ============================================================

train_df, validation_df, test_df = df.randomSplit(
    [0.70, 0.15, 0.15],
    seed=42
)

print(f"Train: {train_df.count()}")
print(f"Validation: {validation_df.count()}")
print(f"Test: {test_df.count()}")


# CSV sem header para manter compatibilidade com training job
train_df.coalesce(20).write.mode("overwrite").option(
    "header", "false"
).csv(
    OUTPUT_BASE + "train/"
)

validation_df.coalesce(5).write.mode("overwrite").option(
    "header", "false"
).csv(
    OUTPUT_BASE + "validation/"
)

test_df.coalesce(5).write.mode("overwrite").option(
    "header", "false"
).csv(
    OUTPUT_BASE + "test/"
)


# Metadata simples para auditoria
metadata = spark.createDataFrame(
    [
        (
            "conversion_propensity_v3",
            len(ALL_FEATURES),
            len(BASE_FEATURES),
            len(NEW_FEATURES),
            len(HISTORY_FEATURES)
        )
    ],
    [
        "version",
        "total_features",
        "base_features",
        "derived_features",
        "history_features"
    ]
)

metadata.coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(
    OUTPUT_BASE + "metadata/"
)


job.commit()
