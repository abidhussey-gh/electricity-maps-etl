"""
Bronze Layer — Electricity Mix (power breakdown history)
=========================================================
Fetches raw JSON from the Electricity Maps API and writes it to the
Bronze zone partitioned by ingestion timestamp (year/month/day).

Design decisions
----------------
* Raw API response is stored as-is (JSON) — no schema enforcement.
* Ingestion timestamp and source URL are added as envelope metadata.
* Supports incremental runs via the watermark utility.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import Config
from src.utils.api_client import ElectricityMapsClient
from src.utils.watermark import get_last_ingested, set_last_ingested

logger = logging.getLogger(__name__)

STREAM = "electricity_mix"


def ingest(client=None, zone=Config.ZONE, full_load=False):
    client = client or ElectricityMapsClient()
    bronze_root = Config.DATA_LAKE_ROOT
    ingestion_ts = datetime.now(tz=timezone.utc)

    last_ts = None if full_load else get_last_ingested(bronze_root, STREAM)

    logger.info("Fetching electricity mix for zone=%s", zone)
    raw_payload = client.get_electricity_mix(zone=zone)   # ← correct method

    history = raw_payload.get("history", [])

    if last_ts:
        history = [
            h for h in history
            if datetime.fromisoformat(
                h["datetime"].replace("Z", "+00:00")
            ) > last_ts
        ]

    if not history:
        logger.info("No new data for %s.", STREAM)
        return []

    envelope = {
        "_meta": {
            "ingestion_timestamp": ingestion_ts.isoformat(),
            "source_url": f"{Config.BASE_URL}{Config.ENDPOINTS['electricity_mix']}?zone={zone}",
            "zone": zone,
            "record_count": len(history),
        },
        "history": history,
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
        datetime.fromisoformat(h["datetime"].replace("Z", "+00:00"))
        for h in history
    )
    set_last_ingested(bronze_root, STREAM, latest_dt)
    return [output_path]
