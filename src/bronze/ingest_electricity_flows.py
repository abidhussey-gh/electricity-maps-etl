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


def ingest(
    client: ElectricityMapsClient | None = None,
    zone: str = Config.ZONE,
    full_load: bool = False,
) -> list[Path]:
    """
    Fetch electricity flow history and write raw JSON to Bronze.

    The API returns the same power-breakdown endpoint; the Silver layer
    is responsible for splitting mix vs. flows columns.
    """
    client = client or ElectricityMapsClient()
    bronze_root = Config.DATA_LAKE_ROOT

    ingestion_ts = datetime.now(tz=timezone.utc)

    last_ts = None if full_load else get_last_ingested(bronze_root, STREAM)

    if last_ts:
        logger.info("Incremental run — last ingested: %s", last_ts.isoformat())
    else:
        logger.info("Full load — fetching all available history.")

    logger.info("Fetching power breakdown (flows view) for zone=%s", zone)
    raw_payload = client.get_power_breakdown_history(zone=zone)

    history: list[dict] = raw_payload.get("history", [])

    # ── Keep only flow-relevant keys to avoid data duplication ───────────────
    flow_records = []
    for record in history:
        flow_record = {
            "datetime": record.get("datetime"),
            "zone": record.get("zone"),
            "powerImport": record.get("powerImport", {}),
            "powerExport": record.get("powerExport", {}),
            "powerNetImport": record.get("powerNetImport", {}),
        }
        flow_records.append(flow_record)

    if last_ts:
        flow_records = [
            r for r in flow_records
            if datetime.fromisoformat(
                r["datetime"].replace("Z", "+00:00")
            ) > last_ts
        ]
        logger.info(
            "%d new flow records after filtering by watermark.", len(flow_records)
        )

    if not flow_records:
        logger.info("No new data to ingest for %s.", STREAM)
        return []

    envelope = {
        "_meta": {
            "ingestion_timestamp": ingestion_ts.isoformat(),
            "source_url": (
                f"{Config.BASE_URL}/power-breakdown/history?zone={zone}"
            ),
            "zone": zone,
            "record_count": len(flow_records),
        },
        "history": flow_records,
    }

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

    latest_dt = max(
        datetime.fromisoformat(r["datetime"].replace("Z", "+00:00"))
        for r in flow_records
    )
    set_last_ingested(bronze_root, STREAM, latest_dt)

    return [output_path]
