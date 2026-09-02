from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("TestGoldFeatures")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


data = [
    # Sessão 1:
    # 3 views antes do cart e depois compra
    ("s1", 101, "2026-09-02 10:00:00", "view", 100.0),
    ("s1", 101, "2026-09-02 10:01:00", "view", 100.0),
    ("s1", 101, "2026-09-02 10:02:00", "view", 100.0),
    ("s1", 101, "2026-09-02 10:05:00", "cart", 100.0),
    ("s1", 101, "2026-09-02 10:10:00", "purchase", 100.0),

    # Sessão 2:
    # 2 views antes do cart e não compra
    ("s2", 202, "2026-09-02 23:00:00", "view", 50.0),
    ("s2", 202, "2026-09-02 23:01:00", "view", 50.0),
    ("s2", 202, "2026-09-02 23:05:00", "cart", 50.0),
]


df = spark.createDataFrame(
    data,
    [
        "user_session",
        "user_id",
        "event_time",
        "event_type",
        "price"
    ]
)

df = df.withColumn(
    "event_time",
    F.to_timestamp("event_time")
)


first_cart = (
    df
    .filter(F.col("event_type") == "cart")
    .groupBy("user_session", "user_id")
    .agg(
        F.min("event_time").alias("first_cart_time")
    )
)


df = df.join(
    first_cart,
    ["user_session", "user_id"],
    "left"
)


result = (
    df
    .groupBy(
        "user_session",
        "user_id"
    )
    .agg(

        F.sum(
            F.when(
                F.col("event_type") == "cart",
                1
            ).otherwise(0)
        ).alias("num_cart_items"),

        F.sum(
            F.when(
                (F.col("event_type") == "view")
                &
                (F.col("event_time") < F.col("first_cart_time")),
                1
            ).otherwise(0)
        ).alias("num_views_before_cart"),

        F.sum(
            F.when(
                F.col("event_type") == "purchase",
                1
            ).otherwise(0)
        ).alias("num_purchases"),

        F.min("first_cart_time").alias("first_cart_time")
    )

    .withColumn(
        "view_to_cart_ratio",
        F.col("num_views_before_cart")
        /
        F.col("num_cart_items")
    )

    .withColumn(
        "hour_of_day",
        F.hour("first_cart_time")
    )

    .withColumn(
        "is_night",
        F.when(
            (F.col("hour_of_day") >= 22)
            |
            (F.col("hour_of_day") < 6),
            1
        ).otherwise(0)
    )

    .withColumn(
        "is_abandoned",
        F.when(
            (F.col("num_cart_items") > 0)
            &
            (F.col("num_purchases") == 0),
            1
        ).otherwise(0)
    )
)


print("\n=== TESTE DAS FEATURES ===")

result.select(
    "user_session",
    "num_cart_items",
    "num_views_before_cart",
    "view_to_cart_ratio",
    "hour_of_day",
    "is_night",
    "num_purchases",
    "is_abandoned"
).show(truncate=False)


spark.stop()