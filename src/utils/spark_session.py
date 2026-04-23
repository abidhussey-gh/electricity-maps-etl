"""
Shared SparkSession factory configured for Delta Lake.
"""
from pyspark.sql import SparkSession

from config import Config
import os
import sys

def get_spark(app_name: str | None = None) -> SparkSession:
    """
    Return (or create) a SparkSession with Delta Lake extensions enabled.
    Calling this multiple times in the same process returns the existing session.
    """
    name = app_name or Config.SPARK_APP_NAME

    # Delta Lake 3.2.0 requires these two JARs for Spark 3.5.x
    delta_packages = "io.delta:delta-spark_2.12:3.2.0,io.delta:delta-storage:3.2.0"

    builder = (
        SparkSession.builder.appName(name)
        .master(Config.SPARK_MASTER)
        # Pull Delta JARs automatically (cached after first run)
        .config("spark.jars.packages", delta_packages)
        # Delta Lake core settings
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Avoid schema-on-read headaches
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        # Adaptive query execution (auto-tunes shuffle partitions)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    return builder.getOrCreate()
