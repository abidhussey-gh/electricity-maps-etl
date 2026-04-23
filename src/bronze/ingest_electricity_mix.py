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


def ingest(
    client: ElectricityMapsClient | None = None,
    zone: str = Config.ZONE,
    full_load: bool = False,
) -> list[Path]:
    """
    Fetch power-breakdown history and write raw JSON files to Bronze.

    Parameters
    ----------
    client:     Pre-built API client (injected for testing).
    zone:       ISO zone code.
    full_load:  If True, ignore the watermark and ingest the last
                FULL_LOAD_HISTORY_DAYS of data.

    Returns
    -------
    List of paths that were written.
    """
    client = client or ElectricityMapsClient()
    bronze_root = Config.DATA_LAKE_ROOT

    ingestion_ts = datetime.now(tz=timezone.utc)

    # ── Decide what to fetch ──────────────────────────────────────────────────
    last_ts = None if full_load else get_last_ingested(bronze_root, STREAM)

    if last_ts:
        logger.info("Incremental run — last ingested: %s", last_ts.isoformat())
    else:
        logger.info(
            "Full load — fetching last %d days.", Config.FULL_LOAD_HISTORY_DAYS
        )

    # ── Call API ──────────────────────────────────────────────────────────────
    logger.info("Fetching power breakdown history for zone=%s", zone)
    raw_payload = client.get_power_breakdown_history(zone=zone)

    # ── Filter records newer than watermark (incremental) ────────────────────
    history: list[dict] = raw_payload.get("history", [])

    if last_ts:
        history = [
            h for h in history
            if datetime.fromisoformat(
                h["datetime"].replace("Z", "+00:00")
            ) > last_ts
        ]
        logger.info("%d new records after filtering by watermark.", len(history))
    else:
        logger.info("%d records in full payload.", len(history))

    if not history:
        logger.info("No new data to ingest for %s.", STREAM)
        return []

    # ── Build envelope ────────────────────────────────────────────────────────
    envelope = {
        "_meta": {
            "ingestion_timestamp": ingestion_ts.isoformat(),
            "source_url": (
                f"{Config.BASE_URL}/power-breakdown/history?zone={zone}"
            ),
            "zone": zone,
            "record_count": len(history),
        },
        "history": history,
    }

    # ── Write to Bronze partition ─────────────────────────────────────────────
    partition = (
        f"year={ingestion_ts.year:04d}/"
        f"month={ingestion_ts.month:02d}/"
        f"day={ingestion_ts.day:02d}"
    )
    output_dir = Path(Config.bronze_path(STREAM)) / partition
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{STREAM}_{ingestion_ts.strftime('%Y%m%dT%H%M%SZ')}.json"
    output_path = output_dir / filename

    with open(output_path, "w") as fh:
        json.dump(envelope, fh, indent=2)

    logger.info("Bronze written → %s", output_path)

    # ── Update watermark ──────────────────────────────────────────────────────
    latest_dt = max(
        datetime.fromisoformat(h["datetime"].replace("Z", "+00:00"))
        for h in history
    )
    set_last_ingested(bronze_root, STREAM, latest_dt)

    return [output_path]
