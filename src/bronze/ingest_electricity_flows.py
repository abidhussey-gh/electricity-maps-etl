"""
Bronze Layer — Electricity Flows (power imports / exports)
==========================================================
The Electricity Maps sandbox exposes import/export flows through the
power-breakdown endpoint (``powerImport`` / ``powerExport`` sub-keys).
We store the full raw response for this stream separately so that the
Silver layer can treat flows and mix as independent tables.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from config import Config
from src.utils.api_client import ElectricityMapsClient
from src.utils.watermark import get_last_ingested, set_last_ingested

logger = logging.getLogger(__name__)

STREAM = "electricity_flows"


def ingest(client=None, zone=Config.ZONE, full_load=False):
    client = client or ElectricityMapsClient()
    bronze_root = Config.DATA_LAKE_ROOT
    ingestion_ts = datetime.now(tz=timezone.utc)

    last_ts = None if full_load else get_last_ingested(bronze_root, STREAM)

    logger.info("Fetching electricity flows for zone=%s", zone)
    raw_payload = client.get_electricity_flows(zone=zone)  # ← correct method

    history = raw_payload.get("history", [])

    # Keep only flow-relevant keys
    flow_records = [
        {
            "datetime":  r.get("datetime"),
            "zone":      zone,
            "import":    r.get("import", {}),   # ← was powerImport
            "export":    r.get("export", {}),   # ← was powerExport
        }
        for r in history
    ]

    if last_ts:
        flow_records = [
            r for r in flow_records
            if datetime.fromisoformat(
                r["datetime"].replace("Z", "+00:00")
            ) > last_ts
        ]

    if not flow_records:
        logger.info("No new data for %s.", STREAM)
        return []

    envelope = {
        "_meta": {
            "ingestion_timestamp": ingestion_ts.isoformat(),
            "source_url": f"{Config.BASE_URL}{Config.ENDPOINTS['electricity_flows']}?zone={zone}",
            "zone":         zone,
            "record_count": len(flow_records),
        },
        "history": flow_records,
    }

    partition = f"year={ingestion_ts.year:04d}/month={ingestion_ts.month:02d}/day={ingestion_ts.day:02d}"
    output_dir = Path(Config.bronze_path(STREAM)) / partition
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{STREAM}_{ingestion_ts.strftime('%Y%m%dT%H%M%SZ')}.json"
    output_path = output_dir / filename

    with open(output_path, "w") as fh:
        json.dump(envelope, fh, indent=2)

    logger.info("Bronze written → %s", output_path)

    latest_dt = max(
        datetime.fromisoformat(r["datetime"].replace("Z", "+00:00"))
        for r in flow_records
    )
    set_last_ingested(bronze_root, STREAM, latest_dt)
    return [output_path]
