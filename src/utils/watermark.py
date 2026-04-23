"""
Lightweight incremental-ingestion state tracker.

Stores the last successfully ingested datetime per stream in a small JSON
file alongside the Bronze data.  On the next run the pipeline only fetches
data newer than that watermark.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_WATERMARK_FILE = "._watermark.json"


def _watermark_path(bronze_root: str, stream: str) -> Path:
    return Path(bronze_root) / stream / _WATERMARK_FILE


def get_last_ingested(bronze_root: str, stream: str) -> datetime | None:
    """Return the last-ingested watermark for *stream*, or None if first run."""
    path = _watermark_path(bronze_root, stream)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        ts = data.get("last_ingested")
        if ts:
            return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
    except Exception as exc:
        logger.warning("Could not read watermark for %s: %s", stream, exc)
    return None


def set_last_ingested(bronze_root: str, stream: str, ts: datetime) -> None:
    """Persist *ts* as the new watermark for *stream*."""
    path = _watermark_path(bronze_root, stream)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"last_ingested": ts.isoformat(), "stream": stream})
    )
    logger.info("Watermark updated for %s → %s", stream, ts.isoformat())
