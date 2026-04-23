"""
Silver Layer — Electricity Flows
=================================
Reads raw Bronze JSON for the flows stream, flattens the nested
powerImport / powerExport / powerNetImport maps, enforces schema,
deduplicates, and writes Delta + Parquet.
"""
from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

from config import Config
from src.utils.spark_session import get_spark

logger = logging.getLogger(__name__)

STREAM = "electricity_flows"

SILVER_SCHEMA = StructType(
    [
        StructField("record_id", StringType(), nullable=False),
        StructField("zone", StringType(), nullable=False),
        StructField("data_timestamp", TimestampType(), nullable=False),
        StructField("counterpart_zone", StringType(), nullable=False),
        StructField("flow_type", StringType(), nullable=False),      # import|export|net_import
        StructField("power_mw", DoubleType(), nullable=True),
        StructField("ingestion_timestamp", TimestampType(), nullable=False),
        StructField("year", StringType(), nullable=False),
        StructField("month", StringType(), nullable=False),
        StructField("day", StringType(), nullable=False),
    ]
)


def _read_bronze(spark: SparkSession, bronze_path: str) -> DataFrame:
    return (
        spark.read.option("multiline", "true")
        .option("recursiveFileLookup", "true")
        .json(bronze_path)
    )


def _flatten_flow_map(base: DataFrame, map_col: str, flow_type: str) -> DataFrame:
    """
    Explode a single flow map column into (counterpart_zone, power_mw) rows.
    Uses to_json → from_json to convert struct to MAP since Spark infers
    flat dicts as structs.
    """
    from pyspark.sql.types import MapType

    return (
        base.select(
            "zone",
            "ingestion_timestamp_str",
            "datetime_str",
            F.explode(
                F.from_json(
                    F.to_json(F.col(map_col)),
                    MapType(StringType(), DoubleType())
                )
            ).alias("counterpart_zone", "power_mw"),
        )
        .withColumn("flow_type", F.lit(flow_type))
    )


def _flatten(df: DataFrame) -> DataFrame:
    base = df.select(
        F.col("_meta.ingestion_timestamp").alias("ingestion_timestamp_str"),
        F.col("_meta.zone").alias("zone"),
        F.explode("history").alias("rec"),
    ).select(
        "zone",
        "ingestion_timestamp_str",
        F.col("rec.datetime").alias("datetime_str"),
        F.col("rec.import").alias("import"),
        F.col("rec.export").alias("export"),
    )

    imports_df = _flatten_flow_map(base, "import", "import")
    exports_df = _flatten_flow_map(base, "export", "export")

    return imports_df.unionByName(exports_df)


def _apply_schema(df: DataFrame) -> DataFrame:
    df = (
        df.withColumn(
            "data_timestamp",
            F.to_timestamp(F.col("datetime_str"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        )
        .withColumn("ingestion_timestamp", F.to_timestamp(F.col("ingestion_timestamp_str")))
        .withColumn("power_mw", F.col("power_mw").cast(DoubleType()))
        .withColumn("year",  F.date_format("data_timestamp", "yyyy"))
        .withColumn("month", F.date_format("data_timestamp", "MM"))
        .withColumn("day",   F.date_format("data_timestamp", "dd"))
        .withColumn(
            "record_id",
            F.concat_ws(
                "|",
                F.col("zone"),
                F.col("datetime_str"),
                F.col("counterpart_zone"),
                F.col("flow_type"),
            ),
        )
    )

    return df.select(
        "record_id",
        "zone",
        "data_timestamp",
        "counterpart_zone",
        "flow_type",
        "power_mw",
        "ingestion_timestamp",
        "year",
        "month",
        "day",
    )


def _deduplicate(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("record_id").orderBy(F.col("ingestion_timestamp").desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def _upsert_delta(spark: SparkSession, df: DataFrame, silver_path: str) -> None:
    if DeltaTable.isDeltaTable(spark, silver_path):
        dt = DeltaTable.forPath(spark, silver_path)
        (
            dt.alias("t")
            .merge(df.alias("s"), "t.record_id = s.record_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            df.write.format("delta")
            .partitionBy("year", "month", "day")
            .mode("overwrite")
            .save(silver_path)
        )
    logger.info("Silver flows Delta written to %s", silver_path)


def transform(spark: SparkSession | None = None) -> DataFrame:
    spark = spark or get_spark()
    bronze_path = Config.bronze_path(STREAM)
    silver_path = Config.silver_path(STREAM)

    logger.info("Reading Bronze flows from %s", bronze_path)
    raw = _read_bronze(spark, bronze_path)

    flat = _flatten(raw)
    typed = _apply_schema(flat)
    deduped = _deduplicate(typed)

    _upsert_delta(spark, deduped, silver_path)

    parquet_path = silver_path + "_parquet"
    deduped.write.partitionBy("year", "month", "day").mode("overwrite").parquet(parquet_path)
    logger.info("Silver flows Parquet written to %s", parquet_path)

    return deduped
