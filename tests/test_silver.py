"""
Unit tests — Silver layer transformations
Uses a local SparkSession (no Delta write); we test the transformation
logic (flatten → schema → dedup) in isolation.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

# ---------------------------------------------------------------------------
# SparkSession fixture (module-scoped for speed)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("test-silver")
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


# ---------------------------------------------------------------------------
# Shared sample data
# ---------------------------------------------------------------------------

SAMPLE_BRONZE_MIX = {
    "_meta": {
        "ingestion_timestamp": "2024-01-15T12:00:00+00:00",
        "source_url": "https://api.electricitymaps.com/v4/electricity-mix/history?zone=FR",
        "zone": "FR",
        "record_count": 2,
    },
    "history": [
        {
            "datetime": "2024-01-15T10:00:00.000Z",
            "isEstimated": True,
            "estimationMethod": "SANDBOX_MODE_DATA",
            "mix": {
                "nuclear": 45000.0,
                "hydro": 8000.0,
                "wind": 3000.0,
                "solar": None,
                "gas": 4500.0,
                "coal": 300.0,
                "oil": 43.6,
                "biomass": 1166.0,
                "geothermal": None,
                "unknown": None,
                "hydro storage": {"charge": 713.0, "discharge": None},
                "battery storage": {"charge": 11.0, "discharge": None},
            },
        },
        {
            # Duplicate datetime — for dedup test
            "datetime": "2024-01-15T10:00:00.000Z",
            "isEstimated": True,
            "estimationMethod": "SANDBOX_MODE_DATA",
            "mix": {
                "nuclear": 45000.0,
                "hydro": 8000.0,
                "wind": 3000.0,
                "solar": None,
                "gas": 4500.0,
                "coal": 300.0,
                "oil": 43.6,
                "biomass": 1166.0,
                "geothermal": None,
                "unknown": None,
                "hydro storage": {"charge": 713.0, "discharge": None},
                "battery storage": {"charge": 11.0, "discharge": None},
            },
        },
    ],
}

SAMPLE_BRONZE_FLOWS = {
    "_meta": {
        "ingestion_timestamp": "2024-01-15T12:00:00+00:00",
        "source_url": "https://api.electricitymaps.com/v4/electricity-flows/history?zone=FR",
        "zone": "FR",
        "record_count": 1,
    },
    "history": [
        {
            "datetime": "2024-01-15T10:00:00.000Z",
            "import": {"ES": 1200.0, "DE": 800.0},   # ← was powerImport
            "export": {"GB": 500.0},                  # ← was powerExport
        }
    ],
}


# ---------------------------------------------------------------------------
# Helper: create DataFrame from raw dict
# ---------------------------------------------------------------------------

def make_mix_df(spark, data=None):
    data = data or SAMPLE_BRONZE_MIX
    rdd = spark.sparkContext.parallelize([json.dumps(data)])
    return spark.read.option("multiline", "false").json(rdd)


def make_flows_df(spark, data=None):
    data = data or SAMPLE_BRONZE_FLOWS
    rdd = spark.sparkContext.parallelize([json.dumps(data)])
    return spark.read.option("multiline", "false").json(rdd)


# ---------------------------------------------------------------------------
# Mix transformation tests
# ---------------------------------------------------------------------------

class TestSilverMixTransform:

    def test_flatten_produces_one_row_per_source(self, spark):
        from src.silver.transform_electricity_mix import _flatten, SIMPLE_SOURCES, STORAGE_SOURCES

        raw = make_mix_df(spark)
        flat = _flatten(raw)

        # 2 history records × (11 simple + 4 storage directions) = 30 rows before dedup
        expected = len(SIMPLE_SOURCES) + (len(STORAGE_SOURCES) * 2)
        assert flat.count() == expected * 2  # 2 history records

    def test_schema_has_required_columns(self, spark):
        from src.silver.transform_electricity_mix import _flatten, _apply_schema

        raw = make_mix_df(spark)
        typed = _apply_schema(_flatten(raw))

        cols = set(typed.columns)
        for expected in (
            "record_id", "zone", "data_timestamp", "source_type",
            "power_mw", "is_estimated", "year", "month", "day",
        ):
            assert expected in cols, f"Missing column: {expected}"

        # These columns no longer exist in v4 schema
        assert "fossil_free_percentage" not in cols
        assert "renewable_percentage" not in cols

    def test_data_timestamp_is_timestamp_type(self, spark):
        from pyspark.sql.types import TimestampType
        from src.silver.transform_electricity_mix import _flatten, _apply_schema

        raw = make_mix_df(spark)
        typed = _apply_schema(_flatten(raw))

        ts_field = [f for f in typed.schema.fields if f.name == "data_timestamp"][0]
        assert isinstance(ts_field.dataType, TimestampType)

    def test_dedup_removes_duplicate_records(self, spark):
        from src.silver.transform_electricity_mix import (
            _flatten, _apply_schema, _deduplicate, SIMPLE_SOURCES, STORAGE_SOURCES
        )

        raw = make_mix_df(spark)
        deduped = _deduplicate(_apply_schema(_flatten(raw)))

        # Both history rows are identical → should collapse to unique sources only
        expected = len(SIMPLE_SOURCES) + (len(STORAGE_SOURCES) * 2)
        assert deduped.count() == expected

    def test_record_id_is_deterministic(self, spark):
        from src.silver.transform_electricity_mix import _flatten, _apply_schema

        raw = make_mix_df(spark)
        typed = _apply_schema(_flatten(raw))

        sample = typed.filter(typed.source_type == "nuclear").select("record_id").first()
        assert sample is not None
        assert "FR" in sample["record_id"]
        assert "nuclear" in sample["record_id"]

    def test_storage_sources_produce_charge_discharge_rows(self, spark):
        from src.silver.transform_electricity_mix import _flatten

        raw = make_mix_df(spark)
        flat = _flatten(raw)

        source_types = {r["source_type"] for r in flat.select("source_type").collect()}
        assert "hydro storage_charge" in source_types
        assert "hydro storage_discharge" in source_types
        assert "battery storage_charge" in source_types
        assert "battery storage_discharge" in source_types

    def test_partition_columns_correct(self, spark):
        from src.silver.transform_electricity_mix import _flatten, _apply_schema

        raw = make_mix_df(spark)
        typed = _apply_schema(_flatten(raw))
        row = typed.first()

        assert row["year"] == "2024"
        assert row["month"] == "01"
        assert row["day"] == "15"


# ---------------------------------------------------------------------------
# Flows transformation tests
# ---------------------------------------------------------------------------

class TestSilverFlowsTransform:

    def test_flatten_produces_rows_per_flow_type(self, spark):
        from src.silver.transform_electricity_flows import _flatten

        raw = make_flows_df(spark)
        flat = _flatten(raw)

        # 1 import record (ES, DE = 2 rows) + 1 export record (GB = 1 row) = 3 rows
        assert flat.count() == 3

    def test_only_import_and_export_flow_types_present(self, spark):
        from src.silver.transform_electricity_flows import _flatten

        raw = make_flows_df(spark)
        flat = _flatten(raw)

        flow_types = {r["flow_type"] for r in flat.select("flow_type").collect()}
        assert flow_types == {"import", "export"}
        assert "net_import" not in flow_types   # ← removed in v4

    def test_schema_typed_correctly(self, spark):
        from pyspark.sql.types import DoubleType, TimestampType
        from src.silver.transform_electricity_flows import _flatten, _apply_schema

        raw = make_flows_df(spark)
        typed = _apply_schema(_flatten(raw))

        field_types = {f.name: type(f.dataType) for f in typed.schema.fields}
        assert field_types["power_mw"] == DoubleType
        assert field_types["data_timestamp"] == TimestampType
