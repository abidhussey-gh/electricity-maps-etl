"""
Gold Layer — Data Product 1: Daily Relative Electricity Mix
============================================================
Aggregates hourly Silver mix data to daily level and computes each
energy source's percentage contribution to total consumption.

Schema (flat, no nesting)
-------------------------
date                        DATE
zone                        STRING
source_type                 STRING
daily_power_mwh             DOUBLE   — sum of hourly MW readings (proxy for MWh)
total_daily_consumption_mwh DOUBLE   — zone total for that day
pct_contribution            DOUBLE   — source_type share (0–100)
avg_fossil_free_pct         DOUBLE   — daily average fossil-free %
avg_renewable_pct           DOUBLE   — daily average renewable %
reference_timestamp         TIMESTAMP — midnight UTC of the date
pipeline_run_timestamp      TIMESTAMP — when this Gold record was produced
year                        STRING   — partition
month                       STRING   — partition
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from config import Config
from src.utils.spark_session import get_spark

logger = logging.getLogger(__name__)

PRODUCT = "daily_electricity_mix"


def build(spark: SparkSession | None = None) -> DataFrame:
    spark = spark or get_spark()
    silver_path = Config.silver_path("electricity_mix")
    gold_path   = Config.gold_path(PRODUCT)
    run_ts      = datetime.now(tz=timezone.utc)

    logger.info("Reading Silver mix from %s", silver_path)
    silver = spark.read.format("delta").load(silver_path)

    # ── Daily totals per source ───────────────────────────────────────────────
    daily_source = (
        silver.withColumn("date", F.to_date("data_timestamp"))
        .groupBy("zone", "date", "source_type")
        .agg(
            F.sum("power_mw").alias("daily_power_mwh"),
        )
    )

    # ── Zone daily total (for percentage calc) ────────────────────────────────
    zone_total = (
        daily_source.groupBy("zone", "date")
        .agg(F.sum("daily_power_mwh").alias("total_daily_consumption_mwh"))
    )

    # ── Join & compute percentage ─────────────────────────────────────────────
    gold = (
        daily_source.join(zone_total, on=["zone", "date"], how="left")
        .withColumn(
            "pct_contribution",
            (F.col("daily_power_mwh") / F.col("total_daily_consumption_mwh") * 100)
            .cast(DoubleType()),
        )
        .withColumn(
            "reference_timestamp",
            F.to_timestamp(F.col("date").cast("string"), "yyyy-MM-dd"),
        )
        .withColumn("pipeline_run_timestamp", F.lit(run_ts.isoformat()).cast("timestamp"))
        .withColumn("year",  F.date_format("date", "yyyy"))
        .withColumn("month", F.date_format("date", "MM"))
        .select(
            "date",
            "zone",
            "source_type",
            "daily_power_mwh",
            "total_daily_consumption_mwh",
            "pct_contribution",
            "reference_timestamp",
            "pipeline_run_timestamp",
            "year",
            "month",
        )
    )

    _write_gold(spark, gold, gold_path)
    return gold


def _write_gold(spark: SparkSession, df: DataFrame, gold_path: str) -> None:
    merge_key = "target.zone = source.zone AND target.date = source.date AND target.source_type = source.source_type"

    if DeltaTable.isDeltaTable(spark, gold_path):
        dt = DeltaTable.forPath(spark, gold_path)
        (
            dt.alias("target")
            .merge(df.alias("source"), merge_key)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Gold mix Delta merged at %s", gold_path)
    else:
        (
            df.write.format("delta")
            .partitionBy("year", "month")
            .mode("overwrite")
            .save(gold_path)
        )
        logger.info("Gold mix Delta created at %s", gold_path)

    parquet_path = gold_path + "_parquet"
    df.write.partitionBy("year", "month").mode("overwrite").parquet(parquet_path)
    logger.info("Gold mix Parquet written to %s", parquet_path)
