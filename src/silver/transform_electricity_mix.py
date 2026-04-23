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
# Each row = one hourly observation for one energy source in one zone.
SILVER_SCHEMA = StructType(
    [
        StructField("record_id", StringType(), nullable=False),     # dedup key
        StructField("zone", StringType(), nullable=False),
        StructField("data_timestamp", TimestampType(), nullable=False),
        StructField("source_type", StringType(), nullable=False),   # e.g. "nuclear"
        StructField("power_mw", DoubleType(), nullable=True),
        StructField("fossil_free_percentage", DoubleType(), nullable=True),
        StructField("renewable_percentage", DoubleType(), nullable=True),
        StructField("power_consumption_total_mw", DoubleType(), nullable=True),
        StructField("power_production_total_mw", DoubleType(), nullable=True),
        StructField("ingestion_timestamp", TimestampType(), nullable=False),
        # Partition columns 
        StructField("year", StringType(), nullable=False),
        StructField("month", StringType(), nullable=False),
        StructField("day", StringType(), nullable=False),
    ]
)


def _read_bronze(spark: SparkSession, bronze_path: str) -> DataFrame:
    """Read all Bronze JSON files for the mix stream as a multi-line JSON DF."""
    return (
        spark.read.option("multiline", "true")
        .option("recursiveFileLookup", "true")
        .json(bronze_path)
    )


def _flatten(df: DataFrame) -> DataFrame:
    """
    Explode the history array and pivot the nested powerConsumptionBreakdown /
    powerProductionBreakdown maps into individual rows (one per source_type).
    """
    # Explode history array
    df = df.select(
        F.col("_meta.ingestion_timestamp").alias("ingestion_timestamp_str"),
        F.col("_meta.zone").alias("zone"),
        F.explode("history").alias("rec"),
    )

    # Extract flat columns from each history record
    df = df.select(
        "zone",
        "ingestion_timestamp_str",
        F.col("rec.datetime").alias("datetime_str"),
        F.col("rec.fossilFreePercentage").cast(DoubleType()).alias(
            "fossil_free_percentage"
        ),
        F.col("rec.renewablePercentage").cast(DoubleType()).alias(
            "renewable_percentage"
        ),
        F.col("rec.powerConsumptionTotal").cast(DoubleType()).alias(
            "power_consumption_total_mw"
        ),
        F.col("rec.powerProductionTotal").cast(DoubleType()).alias(
            "power_production_total_mw"
        ),
        F.col("rec.powerConsumptionBreakdown").alias("consumption_breakdown"),
    )

    # Pivot consumption breakdown map → rows
    df = df.select(
        "zone",
        "ingestion_timestamp_str",
        "datetime_str",
        "fossil_free_percentage",
        "renewable_percentage",
        "power_consumption_total_mw",
        "power_production_total_mw",
        F.explode("consumption_breakdown").alias("source_type", "power_mw"),
    )

    return df


def _apply_schema(df: DataFrame) -> DataFrame:
    """Cast to proper types and add partition + dedup columns."""
    df = df.withColumn(
        "data_timestamp",
        F.to_timestamp(F.col("datetime_str"), "yyyy-MM-dd'T'HH:mm:ssX"),
    ).withColumn(
        "ingestion_timestamp",
        F.to_timestamp(F.col("ingestion_timestamp_str")),
    )

    df = df.withColumn("year", F.date_format("data_timestamp", "yyyy")) \
           .withColumn("month", F.date_format("data_timestamp", "MM")) \
           .withColumn("day", F.date_format("data_timestamp", "dd")) \
           .withColumn("power_mw", F.col("power_mw").cast(DoubleType()))

    # Deterministic dedup key: zone + datetime + source_type
    df = df.withColumn(
        "record_id",
        F.concat_ws("|", F.col("zone"), F.col("datetime_str"), F.col("source_type")),
    )

    return df.select(
        "record_id",
        "zone",
        "data_timestamp",
        "source_type",
        "power_mw",
        "fossil_free_percentage",
        "renewable_percentage",
        "power_consumption_total_mw",
        "power_production_total_mw",
        "ingestion_timestamp",
        "year",
        "month",
        "day",
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
