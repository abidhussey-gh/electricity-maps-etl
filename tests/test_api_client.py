"""
Unit tests — API client
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests


@pytest.fixture()
def client():
    with patch("src.utils.api_client.Config.API_KEY", "test-key-123"):
        from src.utils.api_client import ElectricityMapsClient
        return ElectricityMapsClient(api_key="test-key-123")


class TestElectricityMapsClient:

    def test_raises_if_no_api_key(self):
        with patch("src.utils.api_client.Config.API_KEY", ""):
            from src.utils.api_client import ElectricityMapsClient
            with pytest.raises(ValueError, match="No API key"):
                ElectricityMapsClient(api_key="")

    def test_auth_header_set(self, client):
        assert client.session.headers["auth-token"] == "test-key-123"

    def test_successful_response_returned(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"history": []}
        mock_response.raise_for_status = MagicMock()

        with patch.object(client.session, "get", return_value=mock_response):
            with patch("time.sleep"):  # skip rate-limit pause
                result = client.get_power_breakdown_history(zone="FR")

        assert result == {"history": []}

    def test_retries_on_rate_limit(self, client):
        """Client should retry on HTTP 429 and eventually succeed."""
        mock_429 = MagicMock()
        mock_429.status_code = 429

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = {"history": [{"datetime": "2024-01-15T10:00:00Z"}]}
        mock_200.raise_for_status = MagicMock()

        with patch.object(client.session, "get", side_effect=[mock_429, mock_429, mock_200]):
            with patch("time.sleep"):
                from src.utils.api_client import RateLimitError
                result = client.get_power_breakdown_history(zone="FR")

        assert "history" in result

    def test_raises_on_http_error(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.HTTPError("404")

        with patch.object(client.session, "get", return_value=mock_response):
            with patch("time.sleep"):
                with pytest.raises(requests.HTTPError):
                    client.get_power_breakdown_history(zone="FR")

    def test_datetime_param_formatted_correctly(self, client):
        from datetime import datetime, timezone

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"history": []}
        mock_response.raise_for_status = MagicMock()

        dt = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

        with patch.object(client.session, "get", return_value=mock_response) as mock_get:
            with patch("time.sleep"):
                client.get_power_breakdown_history(zone="FR", datetime_=dt)

        _, kwargs = mock_get.call_args
        assert kwargs["params"]["datetime"] == "2024-01-15T10:00:00Z"
