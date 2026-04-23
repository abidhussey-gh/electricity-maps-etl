from src.utils.spark_session import get_spark

spark = get_spark()

bronze_path = "./data/bronze/electricity_mix"

# Step 1 — raw read
raw = (
    spark.read
    .option("multiline", "true")
    .option("recursiveFileLookup", "true")
    .json(bronze_path)
)
print("=== RAW SCHEMA ===")
raw.printSchema()
print("=== RAW COUNT ===", raw.count())

# Step 2 — after explode
from pyspark.sql import functions as F
step2 = raw.select(
    F.col("_meta.ingestion_timestamp").alias("ingestion_timestamp_str"),
    F.col("_meta.zone").alias("zone"),
    F.explode("history").alias("rec"),
)
print("=== AFTER EXPLODE COUNT ===", step2.count())
print("=== REC SCHEMA ===")
step2.select("rec").printSchema()

# Step 3 — check what rec.mix looks like
step3 = step2.select(
    "zone",
    "ingestion_timestamp_str",
    F.col("rec.datetime").alias("datetime_str"),
    F.to_json(F.col("rec.mix")).alias("mix_json"),
)
print("=== SAMPLE MIX JSON ===")
step3.show(2, truncate=False)

# Step 4 — check timestamp parsing
from pyspark.sql.types import MapType, StringType, DoubleType
step4 = step3.withColumn(
    "mix_map",
    F.from_json(F.col("mix_json"), MapType(StringType(), DoubleType()))
)
print("=== AFTER FROM_JSON ===")
step4.show(2, truncate=False)

# Step 5 — check timestamp parsing on a sample value
sample_dt = step3.select("datetime_str").first()["datetime_str"]
print("=== SAMPLE DATETIME STRING ===", sample_dt)

step5 = step3.withColumn(
    "parsed_ts_1", F.to_timestamp(F.col("datetime_str"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
).withColumn(
    "parsed_ts_2", F.to_timestamp(F.col("datetime_str"))
)
print("=== TIMESTAMP PARSING ===")
step5.select("datetime_str", "parsed_ts_1", "parsed_ts_2").show(2, truncate=False)

spark.stop()