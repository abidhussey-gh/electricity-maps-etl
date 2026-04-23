"""
Silver Layer — Electricity Mix
================================
Reads raw Bronze JSON files, flattens the nested power-breakdown structure,
enforces a strict schema, deduplicates, and writes out as Delta Lake tables
partitioned by data timestamp (year/month/day).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config import Config
from src.utils.spark_session import get_spark

logger = logging.getLogger(__name__)

STREAM = "electricity_mix"

# ── Canonical Silver schema ───────────────────────────────────────────────────
SILVER_SCHEMA = StructType([
    StructField("record_id",           StringType(),    nullable=False),
    StructField("zone",                StringType(),    nullable=False),
    StructField("data_timestamp",      TimestampType(), nullable=False),
    StructField("source_type",         StringType(),    nullable=False),
    StructField("power_mw",            DoubleType(),    nullable=True),
    StructField("is_estimated",        StringType(),    nullable=True),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    StructField("year",                StringType(),    nullable=False),
    StructField("month",               StringType(),    nullable=False),
    StructField("day",                 StringType(),    nullable=False),
])

def _read_bronze(spark: SparkSession, bronze_path: str) -> DataFrame:
    """Read all Bronze JSON files for the mix stream as a multi-line JSON DF."""
    return (
        spark.read.option("multiline", "true")
        .option("recursiveFileLookup", "true")
        .json(bronze_path)
    )


# Energy sources that are simple numeric fields
SIMPLE_SOURCES = ["nuclear", "biomass", "coal", "gas", "geothermal",
                  "hydro", "oil", "solar", "unknown", "wind"]

# Storage sources that have charge/discharge sub-fields
STORAGE_SOURCES = ["hydro storage", "battery storage"]


def _flatten(df: DataFrame) -> DataFrame:
    """
    Explode history array and pivot each energy source into its own row.
    Handles three field types:
      - Simple numeric: nuclear, wind, solar, etc.
      - Storage structs: hydro storage, battery storage (charge/discharge)
      - Ignored: flows (import/export totals — covered by electricity_flows stream)
    """
    # Step 1 — explode history array
    base = df.select(
        F.col("_meta.ingestion_timestamp").alias("ingestion_timestamp_str"),
        F.col("_meta.zone").alias("zone"),
        F.explode("history").alias("rec"),
    ).select(
        "zone",
        "ingestion_timestamp_str",
        F.col("rec.datetime").alias("datetime_str"),
        F.col("rec.isEstimated").cast(StringType()).alias("is_estimated"),
        F.col("rec.mix").alias("mix"),
    )

    # Step 2 — build one DataFrame per source then union all together
    source_dfs = []

    # Simple numeric sources
    for source in SIMPLE_SOURCES:
        col_name = f"mix.`{source}`"   
        source_df = base.select(
            "zone",
            "ingestion_timestamp_str",
            "datetime_str",
            "is_estimated",
            F.lit(source).alias("source_type"),
            F.col(col_name).cast(DoubleType()).alias("power_mw"),
        )
        source_dfs.append(source_df)

    # Storage sources — charge and discharge become separate rows
    for source in STORAGE_SOURCES:
        col_base = f"mix.`{source}`"
        for direction in ["charge", "discharge"]:
            label = f"{source}_{direction}"
            source_df = base.select(
                "zone",
                "ingestion_timestamp_str",
                "datetime_str",
                "is_estimated",
                F.lit(label).alias("source_type"),
                F.col(f"{col_base}.{direction}").cast(DoubleType()).alias("power_mw"),
            )
            source_dfs.append(source_df)

    # Union all source rows together
    result = source_dfs[0]
    for sdf in source_dfs[1:]:
        result = result.unionByName(sdf)

    return result


def _apply_schema(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "data_timestamp",
            F.to_timestamp(F.col("datetime_str"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        )
        .withColumn(
            "ingestion_timestamp",
            F.to_timestamp(F.col("ingestion_timestamp_str")),
        )
        .withColumn("power_mw", F.col("power_mw").cast(DoubleType()))
        .withColumn("year",  F.date_format("data_timestamp", "yyyy"))
        .withColumn("month", F.date_format("data_timestamp", "MM"))
        .withColumn("day",   F.date_format("data_timestamp", "dd"))
        .withColumn(
            "record_id",
            F.concat_ws("|", F.col("zone"), F.col("datetime_str"), F.col("source_type")),
        )
        .select(
            "record_id",
            "zone",
            "data_timestamp",
            "source_type",
            "power_mw",
            "is_estimated",
            "ingestion_timestamp",
            "year",
            "month",
            "day",
        )
    )


def _deduplicate(df: DataFrame) -> DataFrame:
    """Keep the most recently ingested record for each unique record_id."""
    window = (
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("record_id")
        .orderBy(F.col("ingestion_timestamp").desc())
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def _upsert_delta(spark: SparkSession, df: DataFrame, silver_path: str) -> None:
    """Merge new records into the Delta table (insert-or-replace on record_id)."""
    if DeltaTable.isDeltaTable(spark, silver_path):
        delta_table = DeltaTable.forPath(spark, silver_path)
        (
            delta_table.alias("target")
            .merge(df.alias("source"), "target.record_id = source.record_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Delta merge completed for %s.", silver_path)
    else:
        (
            df.write.format("delta")
            .partitionBy("year", "month", "day")
            .mode("overwrite")
            .save(silver_path)
        )
        logger.info("Delta table created at %s.", silver_path)


def transform(spark: SparkSession | None = None) -> DataFrame:
    """
    Full Bronze → Silver transformation for the electricity-mix stream.

    Returns the final deduplicated DataFrame (useful for testing).
    """
    spark = spark or get_spark()
    bronze_path = Config.bronze_path(STREAM)
    silver_path = Config.silver_path(STREAM)

    logger.info("Reading Bronze data from %s", bronze_path)
    raw_df = _read_bronze(spark, bronze_path)

    logger.info("Flattening nested structure …")
    flat_df = _flatten(raw_df)

    logger.info("Applying schema …")
    typed_df = _apply_schema(flat_df)

    logger.info("Deduplicating …")
    deduped_df = _deduplicate(typed_df)

    logger.info("Writing to Silver Delta table at %s", silver_path)
    _upsert_delta(spark, deduped_df, silver_path)

    # Also write Parquet snapshot 
    parquet_path = silver_path + "_parquet"
    (
        deduped_df.write.partitionBy("year", "month", "day")
        .mode("overwrite")
        .parquet(parquet_path)
    )
    logger.info("Silver Parquet snapshot written to %s", parquet_path)

    return deduped_df
