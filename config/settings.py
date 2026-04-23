"""
Central configuration for the Electricity Maps ETL pipeline.
All settings are loaded from environment variables (via .env).
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # ── API ──────────────────────────────────────────────────────────────────
    API_KEY: str = os.getenv("ELECTRICITY_MAPS_API_KEY", "")
    BASE_URL: str = os.getenv(
        "ELECTRICITY_MAPS_BASE_URL", "https://api.electricitymap.org/v3"
    )
    ZONE: str = "FR"

    # Sandbox endpoints (free-tier)
    ENDPOINTS = {
        "power_breakdown": "/power-breakdown/history",   # electricity mix
        "power_imports_exports": "/power-breakdown/history",  # flows derived from same endpoint
    }

    # ── Storage ───────────────────────────────────────────────────────────────
    DATA_LAKE_ROOT: str = os.getenv("DATA_LAKE_ROOT", "./data")

    @classmethod
    def bronze_path(cls, stream: str) -> str:
        return f"{cls.DATA_LAKE_ROOT}/bronze/{stream}"

    @classmethod
    def silver_path(cls, stream: str) -> str:
        return f"{cls.DATA_LAKE_ROOT}/silver/{stream}"

    @classmethod
    def gold_path(cls, product: str) -> str:
        return f"{cls.DATA_LAKE_ROOT}/gold/{product}"

    # ── Spark ─────────────────────────────────────────────────────────────────
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")
    SPARK_APP_NAME: str = os.getenv("SPARK_APP_NAME", "electricity-maps-etl")

    # ── Retry / rate-limit ────────────────────────────────────────────────────
    API_MAX_RETRIES: int = 5
    API_BACKOFF_FACTOR: float = 2.0   # exponential backoff multiplier
    API_RATE_LIMIT_PAUSE: float = 1.0  # seconds between requests

    # ── Ingestion ─────────────────────────────────────────────────────────────
    # How far back to look on a full (non-incremental) run (days)
    FULL_LOAD_HISTORY_DAYS: int = 7
