"""
Gold Layer — Data Product 2: Daily Net Import / Export (MWh)
=============================================================
Produces two separate Gold tables from the Silver flows stream:

1. electricity_imports
   Rows where France is the *destination* (net_import > 0 or flow_type=import).

2. electricity_exports
   Rows where France is the *source* (net_import < 0 or flow_type=export).

Schema — both tables (flat, no nesting)
---------------------------------------
date                   DATE
zone                   STRING     — always "FR"
counterpart_zone       STRING     — trading partner
net_mwh                DOUBLE     — absolute net MWh exchanged
flow_direction         STRING     — "import" | "export"
reference_timestamp    TIMESTAMP  — midnight UTC of the date
pipeline_run_timestamp TIMESTAMP
year                   STRING     — partition
month                  STRING     — partition
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

PRODUCT_IMPORTS = "electricity_imports"
PRODUCT_EXPORTS = "electricity_exports"


def build(spark: SparkSession | None = None) -> tuple[DataFrame, DataFrame]:
    spark  = spark or get_spark()
    silver = spark.read.format("delta").load(Config.silver_path("electricity_flows"))
    run_ts = datetime.now(tz=timezone.utc)

    # Silver flows now only has flow_type = "import" | "export"
    # No net_import — compute net MWh by aggregating daily totals directly

    daily = (
        silver
        .withColumn("date", F.to_date("data_timestamp"))
        .groupBy("zone", "counterpart_zone", "date", "flow_type")
        .agg(F.sum("power_mw").alias("net_mwh"))
    )

    imports_df = (
        daily.filter(F.col("flow_type") == "import")
        .withColumn("flow_direction", F.lit("import"))
        .withColumn("net_mwh", F.col("net_mwh").cast(DoubleType()))
    )

    exports_df = (
        daily.filter(F.col("flow_type") == "export")
        .withColumn("flow_direction", F.lit("export"))
        .withColumn("net_mwh", F.col("net_mwh").cast(DoubleType()))
    )

    for label, df in [("imports", imports_df), ("exports", exports_df)]:
        df = (
            df.withColumn(
                "reference_timestamp",
                F.to_timestamp(F.col("date").cast("string"), "yyyy-MM-dd"),
            )
            .withColumn(
                "pipeline_run_timestamp",
                F.lit(run_ts.isoformat()).cast("timestamp"),
            )
            .withColumn("year",  F.date_format("date", "yyyy"))
            .withColumn("month", F.date_format("date", "MM"))
            .select(
                "date",
                "zone",
                "counterpart_zone",
                "net_mwh",
                "flow_direction",
                "reference_timestamp",
                "pipeline_run_timestamp",
                "year",
                "month",
            )
        )

        product = PRODUCT_IMPORTS if label == "imports" else PRODUCT_EXPORTS
        _write_gold(spark, df, Config.gold_path(product), label)

        if label == "imports":
            imports_df = df
        else:
            exports_df = df

    return imports_df, exports_df


def _write_gold(
    spark: SparkSession,
    df: DataFrame,
    gold_path: str,
    label: str,
) -> None:
    merge_key = (
        "target.zone = source.zone "
        "AND target.date = source.date "
        "AND target.counterpart_zone = source.counterpart_zone"
    )

    if DeltaTable.isDeltaTable(spark, gold_path):
        dt = DeltaTable.forPath(spark, gold_path)
        (
            dt.alias("target")
            .merge(df.alias("source"), merge_key)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Gold %s Delta merged at %s", label, gold_path)
    else:
        (
            df.write.format("delta")
            .partitionBy("year", "month")
            .mode("overwrite")
            .save(gold_path)
        )
        logger.info("Gold %s Delta created at %s", label, gold_path)

    parquet_path = gold_path + "_parquet"
    df.write.partitionBy("year", "month").mode("overwrite").parquet(parquet_path)
    logger.info("Gold %s Parquet written to %s", label, parquet_path)
