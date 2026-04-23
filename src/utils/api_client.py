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
    pass


class ElectricityMapsClient:

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or Config.API_KEY
        if not self.api_key:
            raise ValueError("No API key found. Set ELECTRICITY_MAPS_API_KEY in your .env file.")
        self.session = requests.Session()
        self.session.headers.update({"auth-token": self.api_key})

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
            raise RateLimitError("HTTP 429 — rate limited")
        if response.status_code >= 500:
            response.raise_for_status()

        response.raise_for_status()
        time.sleep(Config.API_RATE_LIMIT_PAUSE)
        return response.json()

    def get_electricity_mix(self, zone: str = Config.ZONE) -> dict:
        """Fetch electricity mix history — /v4/electricity-mix/history"""
        return self._get(Config.ENDPOINTS["electricity_mix"], {"zone": zone})

    def get_electricity_flows(self, zone: str = Config.ZONE) -> dict:
        """Fetch electricity flows history — /v4/electricity-flows/history"""
        return self._get(Config.ENDPOINTS["electricity_flows"], {"zone": zone})