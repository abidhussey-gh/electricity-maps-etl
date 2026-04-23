"""
Electricity Maps API client.

Features
--------
* Exponential-backoff retries on transient HTTP errors (429, 5xx).
* Per-request rate-limit pause to avoid hammering the sandbox tier.
* Incremental support: caller can pass a specific datetime to fetch history.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from config import Config

logger = logging.getLogger(__name__)


class RateLimitError(Exception):
    """Raised when the API returns HTTP 429."""


class ElectricityMapsClient:
    """Thin wrapper around the Electricity Maps REST API."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or Config.API_KEY
        if not self.api_key:
            raise ValueError(
                "No API key found. Set ELECTRICITY_MAPS_API_KEY in your .env file."
            )
        self.session = requests.Session()
        self.session.headers.update(
            {"auth-token": self.api_key, "Content-Type": "application/json"}
        )

    # ── Internal helpers ──────────────────────────────────────────────────────

    @retry(
        retry=retry_if_exception_type((RateLimitError, requests.ConnectionError, requests.Timeout)),
        stop=stop_after_attempt(Config.API_MAX_RETRIES),
        wait=wait_exponential(multiplier=Config.API_BACKOFF_FACTOR, min=2, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _get(self, endpoint: str, params: dict) -> dict:
        url = f"{Config.BASE_URL}{endpoint}"
        logger.debug("GET %s params=%s", url, params)

        response = self.session.get(url, params=params, timeout=30)

        if response.status_code == 429:
            logger.warning("Rate limited — will retry after backoff.")
            raise RateLimitError("HTTP 429 from Electricity Maps API")

        if response.status_code >= 500:
            response.raise_for_status()

        response.raise_for_status()

        # Polite pause between requests
        time.sleep(Config.API_RATE_LIMIT_PAUSE)
        return response.json()

    # ── Public methods ────────────────────────────────────────────────────────

    def get_power_breakdown_history(
        self, zone: str = Config.ZONE, datetime_: datetime | None = None
    ) -> dict:
        """
        Fetch the power breakdown history for *zone*.

        The sandbox key returns a fixed sample dataset regardless of the
        requested datetime, but we pass it so production keys work correctly.
        """
        params: dict = {"zone": zone}
        if datetime_:
            params["datetime"] = datetime_.astimezone(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        return self._get("/power-breakdown/history", params)

    def get_carbon_intensity_history(
        self, zone: str = Config.ZONE, datetime_: datetime | None = None
    ) -> dict:
        """Fetch the carbon intensity history for *zone*."""
        params: dict = {"zone": zone}
        if datetime_:
            params["datetime"] = datetime_.astimezone(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        return self._get("/carbon-intensity/history", params)
