"""
Unit tests — Bronze layer ingestion
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MOCK_API_RESPONSE = {
    "history": [
        {
            "datetime": "2024-01-15T10:00:00Z",
            "zone": "FR",
            "fossilFreePercentage": 92.5,
            "renewablePercentage": 25.3,
            "powerConsumptionTotal": 65000.0,
            "powerProductionTotal": 63000.0,
            "powerConsumptionBreakdown": {
                "nuclear": 45000.0,
                "hydro": 8000.0,
                "wind": 3000.0,
                "solar": 1200.0,
                "gas": 4500.0,
                "coal": 300.0,
            },
            "powerImport": {"ES": 1200.0, "DE": 800.0},
            "powerExport": {"GB": 500.0, "IT": 300.0},
            "powerNetImport": {"ES": 900.0, "DE": 600.0, "GB": -500.0, "IT": -300.0},
        },
        {
            "datetime": "2024-01-15T11:00:00Z",
            "zone": "FR",
            "fossilFreePercentage": 91.0,
            "renewablePercentage": 24.8,
            "powerConsumptionTotal": 66000.0,
            "powerProductionTotal": 64000.0,
            "powerConsumptionBreakdown": {
                "nuclear": 46000.0,
                "hydro": 7500.0,
                "wind": 2800.0,
                "solar": 1000.0,
                "gas": 5000.0,
                "coal": 200.0,
            },
            "powerImport": {"ES": 1100.0, "DE": 900.0},
            "powerExport": {"GB": 400.0, "IT": 350.0},
            "powerNetImport": {"ES": 800.0, "DE": 700.0, "GB": -400.0, "IT": -350.0},
        },
    ]
}


@pytest.fixture()
def mock_client():
    client = MagicMock()
    client.get_power_breakdown_history.return_value = MOCK_API_RESPONSE
    return client


@pytest.fixture()
def tmp_data_root(tmp_path):
    """Patch Config.DATA_LAKE_ROOT to a temp directory."""
    with patch("config.settings.Config.DATA_LAKE_ROOT", str(tmp_path)):
        with patch("src.bronze.ingest_electricity_mix.Config.DATA_LAKE_ROOT", str(tmp_path)):
            with patch("src.bronze.ingest_electricity_flows.Config.DATA_LAKE_ROOT", str(tmp_path)):
                yield tmp_path


# ---------------------------------------------------------------------------
# Mix ingestion tests
# ---------------------------------------------------------------------------

class TestBronzeMixIngestion:

    def test_writes_json_file(self, mock_client, tmp_data_root):
        from src.bronze.ingest_electricity_mix import ingest
        paths = ingest(client=mock_client, full_load=True)

        assert len(paths) == 1
        assert paths[0].exists()
        assert paths[0].suffix == ".json"

    def test_json_envelope_structure(self, mock_client, tmp_data_root):
        from src.bronze.ingest_electricity_mix import ingest
        paths = ingest(client=mock_client, full_load=True)

        with open(paths[0]) as fh:
            data = json.load(fh)

        assert "_meta" in data
        assert "history" in data
        assert "ingestion_timestamp" in data["_meta"]
        assert "source_url" in data["_meta"]
        assert data["_meta"]["zone"] == "FR"

    def test_record_count_in_metadata(self, mock_client, tmp_data_root):
        from src.bronze.ingest_electricity_mix import ingest
        paths = ingest(client=mock_client, full_load=True)

        with open(paths[0]) as fh:
            data = json.load(fh)

        assert data["_meta"]["record_count"] == 2

    def test_partitioned_by_ingestion_timestamp(self, mock_client, tmp_data_root):
        from src.bronze.ingest_electricity_mix import ingest
        paths = ingest(client=mock_client, full_load=True)

        # Path should contain year=/month=/day= segments
        path_str = str(paths[0])
        assert "year=" in path_str
        assert "month=" in path_str
        assert "day=" in path_str

    def test_watermark_updated_after_ingest(self, mock_client, tmp_data_root):
        from src.bronze.ingest_electricity_mix import ingest
        from src.utils.watermark import get_last_ingested

        ingest(client=mock_client, full_load=True)
        wm = get_last_ingested(str(tmp_data_root), "electricity_mix")

        assert wm is not None
        assert wm == datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc)

    def test_incremental_filters_old_records(self, mock_client, tmp_data_root):
        from src.bronze.ingest_electricity_mix import ingest
        from src.utils.watermark import set_last_ingested

        # Pre-set watermark so first record is filtered out
        set_last_ingested(
            str(tmp_data_root),
            "electricity_mix",
            datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
        )

        paths = ingest(client=mock_client, full_load=False)
        with open(paths[0]) as fh:
            data = json.load(fh)

        assert data["_meta"]["record_count"] == 1
        assert data["history"][0]["datetime"] == "2024-01-15T11:00:00Z"

    def test_no_new_data_returns_empty_list(self, mock_client, tmp_data_root):
        from src.bronze.ingest_electricity_mix import ingest
        from src.utils.watermark import set_last_ingested

        # Watermark ahead of all records
        set_last_ingested(
            str(tmp_data_root),
            "electricity_mix",
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        )

        paths = ingest(client=mock_client, full_load=False)
        assert paths == []


# ---------------------------------------------------------------------------
# Flows ingestion tests
# ---------------------------------------------------------------------------

class TestBronzeFlowsIngestion:

    def test_writes_json_file(self, mock_client, tmp_data_root):
        from src.bronze.ingest_electricity_flows import ingest
        paths = ingest(client=mock_client, full_load=True)

        assert len(paths) == 1
        assert paths[0].exists()

    def test_only_flow_keys_stored(self, mock_client, tmp_data_root):
        from src.bronze.ingest_electricity_flows import ingest
        paths = ingest(client=mock_client, full_load=True)

        with open(paths[0]) as fh:
            data = json.load(fh)

        first_record = data["history"][0]
        assert "powerImport" in first_record
        assert "powerExport" in first_record
        assert "powerNetImport" in first_record
        # Mix-only columns should NOT be present
        assert "powerConsumptionBreakdown" not in first_record
