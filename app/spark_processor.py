from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.functions import avg, sum

# cria sessão Spark
spark = SparkSession.builder \
    .appName("StockPipeline") \
    .getOrCreate()

# caminho local (vamos mapear depois)
RAW_PATH = "data/raw/prices"

# lê JSON
df = spark.read.json("data/raw/prices")

# transformação
df_clean = df \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("volume", col("volume").cast("int")) \
    .withColumn("event_time", to_timestamp("event_time"))

df_agg = df_clean.groupBy("ticker").agg(
    avg("price").alias("avg_price"),
    sum("volume").alias("total_volume")
)

df_agg.show()

df_agg.write \
    .mode("overwrite") \
    .parquet("data/analytics/summary")

# mostra dados
df_clean.show()

# salva camada analytics
df_clean.write \
    .mode("overwrite") \
    .parquet("data/analytics/prices")

print("🔥 Dados processados com Spark!")





