"""
Unit tests — Data quality checks
"""
from __future__ import annotations

import pytest


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("test-dq")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class TestDataQualityChecks:

    def _make_mix_df(self, spark, rows):
        from pyspark.sql.types import (
            DoubleType, StringType, StructField, StructType, TimestampType
        )
        from datetime import datetime

        schema = StructType([
            StructField("record_id", StringType()),
            StructField("zone", StringType()),
            StructField("data_timestamp", TimestampType()),
            StructField("source_type", StringType()),
            StructField("power_mw", DoubleType()),
            StructField("fossil_free_percentage", DoubleType()),
            StructField("renewable_percentage", DoubleType()),
            StructField("power_consumption_total_mw", DoubleType()),
            StructField("power_production_total_mw", DoubleType()),
        ])
        return spark.createDataFrame(rows, schema)

    def test_not_empty_passes_on_non_empty_df(self, spark):
        from src.utils.data_quality import _check_not_empty
        from datetime import datetime

        df = self._make_mix_df(spark, [
            ("id1", "FR", datetime(2024, 1, 15, 10), "nuclear", 45000.0, 92.0, 25.0, 65000.0, 63000.0)
        ])
        result = _check_not_empty(df, "silver", "electricity_mix")
        assert result.passed

    def test_not_empty_fails_on_empty_df(self, spark):
        from src.utils.data_quality import _check_not_empty

        df = self._make_mix_df(spark, [])
        result = _check_not_empty(df, "silver", "electricity_mix")
        assert not result.passed

    def test_no_duplicate_ids_passes(self, spark):
        from src.utils.data_quality import _check_no_duplicate_ids
        from datetime import datetime

        df = self._make_mix_df(spark, [
            ("id1", "FR", datetime(2024, 1, 15, 10), "nuclear", 45000.0, 92.0, 25.0, 65000.0, 63000.0),
            ("id2", "FR", datetime(2024, 1, 15, 10), "wind",    3000.0,  92.0, 25.0, 65000.0, 63000.0),
        ])
        result = _check_no_duplicate_ids(df, "silver", "electricity_mix")
        assert result.passed

    def test_no_duplicate_ids_fails_on_duplicates(self, spark):
        from src.utils.data_quality import _check_no_duplicate_ids
        from datetime import datetime

        df = self._make_mix_df(spark, [
            ("id1", "FR", datetime(2024, 1, 15, 10), "nuclear", 45000.0, 92.0, 25.0, 65000.0, 63000.0),
            ("id1", "FR", datetime(2024, 1, 15, 10), "nuclear", 45000.0, 92.0, 25.0, 65000.0, 63000.0),
        ])
        result = _check_no_duplicate_ids(df, "silver", "electricity_mix")
        assert not result.passed

    def test_power_non_negative_passes(self, spark):
        from src.utils.data_quality import _check_power_non_negative
        from datetime import datetime

        df = self._make_mix_df(spark, [
            ("id1", "FR", datetime(2024, 1, 15, 10), "nuclear", 45000.0, 92.0, 25.0, 65000.0, 63000.0),
        ])
        result = _check_power_non_negative(df)
        assert result.passed

    def test_power_non_negative_fails_on_negative(self, spark):
        from src.utils.data_quality import _check_power_non_negative
        from datetime import datetime

        df = self._make_mix_df(spark, [
            ("id1", "FR", datetime(2024, 1, 15, 10), "nuclear", -100.0, 92.0, 25.0, 65000.0, 63000.0),
        ])
        result = _check_power_non_negative(df)
        assert not result.passed

    def test_run_checks_raises_on_critical_failure(self, spark):
        from src.utils.data_quality import run_checks

        df = self._make_mix_df(spark, [])  # empty → critical failure
        with pytest.raises(ValueError, match="Critical data-quality checks failed"):
            run_checks(df, layer="silver", stream="electricity_mix")
