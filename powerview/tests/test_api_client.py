"""Unit tests for the api_client module."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from powerview.src.api_client import (
    get_meter_data,
    get_meter_data_with_retry,
    get_metering_points,
)


class TestGetMeteringPoints:
    """Tests for get_metering_points function."""

    @patch("powerview.src.api_client.requests.get")
    def test_get_metering_points_success(self, mock_get):
        """Test successful metering points retrieval."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": [
                {
                    "meteringPointId": "123456789012345678",
                    "consumerName": "Test User",
                }
            ]
        }
        mock_get.return_value = mock_response

        result = get_metering_points("test_token")

        assert "result" in result
        assert len(result["result"]) == 1
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert (
            call_args[0][0]
            == "https://api.eloverblik.dk/CustomerApi/api/meteringpoints/meteringpoints"
        )

    @patch("powerview.src.api_client.requests.get")
    def test_get_metering_points_http_error(self, mock_get):
        """Test metering points retrieval with HTTP error."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("401 Unauthorized")
        mock_get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            get_metering_points("invalid_token")


class TestGetMeterData:
    """Tests for get_meter_data function."""

    @patch("powerview.src.api_client.requests.post")
    def test_get_meter_data_success(self, mock_post):
        """Test successful meter data retrieval."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": [
                {
                    "success": True,
                    "MyEnergyData_MarketDocument": {
                        "TimeSeries": [
                            {
                                "MarketEvaluationPoint": {"mRID": {"name": "123456789012345678"}},
                                "Period": [
                                    {
                                        "timeInterval": {"start": "2025-01-01T00:00:00Z"},
                                        "Point": [
                                            {
                                                "position": "1",
                                                "out_Quantity": {
                                                    "quantity": "1.5",
                                                    "quality": "A01",
                                                },
                                            }
                                        ],
                                    }
                                ],
                            }
                        ]
                    },
                }
            ]
        }
        mock_post.return_value = mock_response

        result = get_meter_data("test_token", "2025-01-01", "2025-01-31")

        assert "result" in result
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert (
            call_args[0][0] == "https://api.eloverblik.dk/CustomerApi/api/meterdata/gettimeseries"
        )
        assert call_args[1]["json"]["dateFrom"] == "2025-01-01"
        assert call_args[1]["json"]["dateTo"] == "2025-01-31"

    @patch("powerview.src.api_client.requests.post")
    def test_get_meter_data_http_error(self, mock_post):
        """Test meter data retrieval with HTTP error."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("400 Bad Request")
        mock_post.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            get_meter_data("test_token", "2025-01-01", "2025-01-31")


class TestGetMeterDataWithRetry:
    """Tests for get_meter_data_with_retry function."""

    @patch("powerview.src.api_client.get_meter_data")
    def test_get_meter_data_with_retry_success_first_try(self, mock_get_meter_data):
        """Test successful meter data retrieval on first try."""
        mock_get_meter_data.return_value = {"result": []}

        result = get_meter_data_with_retry("test_token", "2025-01-01", "2025-01-31")

        assert result == {"result": []}
        mock_get_meter_data.assert_called_once()

    @patch("powerview.src.api_client.time.sleep")
    @patch("powerview.src.api_client.get_meter_data")
    def test_get_meter_data_with_retry_429_then_success(self, mock_get_meter_data, mock_sleep):
        """Test retry on HTTP 429 (rate limited) then success."""
        mock_response = MagicMock()
        mock_response.status_code = 429
        error = requests.HTTPError()
        error.response = mock_response

        mock_get_meter_data.side_effect = [error, {"result": []}]

        result = get_meter_data_with_retry("test_token", "2025-01-01", "2025-01-31")

        assert result == {"result": []}
        assert mock_get_meter_data.call_count == 2
        mock_sleep.assert_called_once_with(60)

    @patch("powerview.src.api_client.time.sleep")
    @patch("powerview.src.api_client.get_meter_data")
    def test_get_meter_data_with_retry_503_then_success(self, mock_get_meter_data, mock_sleep):
        """Test retry on HTTP 503 (service unavailable) then success."""
        mock_response = MagicMock()
        mock_response.status_code = 503
        error = requests.HTTPError()
        error.response = mock_response

        mock_get_meter_data.side_effect = [error, {"result": []}]

        result = get_meter_data_with_retry("test_token", "2025-01-01", "2025-01-31")

        assert result == {"result": []}
        assert mock_get_meter_data.call_count == 2
        mock_sleep.assert_called_once_with(60)

    @patch("powerview.src.api_client.time.sleep")
    @patch("powerview.src.api_client.get_meter_data")
    def test_get_meter_data_with_retry_max_retries_exceeded(self, mock_get_meter_data, mock_sleep):
        """Test max retries exceeded for rate limiting."""
        mock_response = MagicMock()
        mock_response.status_code = 429
        error = requests.HTTPError()
        error.response = mock_response

        mock_get_meter_data.side_effect = error

        with pytest.raises(requests.HTTPError):
            get_meter_data_with_retry("test_token", "2025-01-01", "2025-01-31", max_retries=2)

        assert mock_get_meter_data.call_count == 2
        assert mock_sleep.call_count == 1

    @patch("powerview.src.api_client.get_meter_data")
    def test_get_meter_data_with_retry_non_retryable_error(self, mock_get_meter_data):
        """Test non-retryable HTTP error raises immediately."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        error = requests.HTTPError()
        error.response = mock_response

        mock_get_meter_data.side_effect = error

        with pytest.raises(requests.HTTPError):
            get_meter_data_with_retry("test_token", "2025-01-01", "2025-01-31")

        mock_get_meter_data.assert_called_once()
