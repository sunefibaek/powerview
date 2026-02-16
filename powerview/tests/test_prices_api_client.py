"""Unit tests for the prices_api_client module."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from powerview.src.prices_api_client import (
    get_dayahead_prices,
    get_elspot_prices,
    get_prices_with_retry,
)


class TestGetElspotPrices:
    """Tests for get_elspot_prices function."""

    @patch("powerview.src.prices_api_client.requests.get")
    def test_get_elspot_prices_success(self, mock_get):
        """Test successful Elspotprices retrieval."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "dataset": "Elspotprices",
            "records": [{"HourUTC": "2025-09-30T21:00:00", "PriceArea": "DK1"}],
        }
        mock_get.return_value = mock_response

        result = get_elspot_prices("2025-09-01", "2025-09-30", ["DK1"])

        assert result["dataset"] == "Elspotprices"
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert call_args[0][0] == "https://api.energidataservice.dk/dataset/Elspotprices"
        assert call_args[1]["params"]["columns"] == "HourUTC,PriceArea,SpotPriceDKK,SpotPriceEUR"
        assert call_args[1]["params"]["start"] == "2025-09-01"
        assert call_args[1]["params"]["end"] == "2025-09-30"
        assert call_args[1]["params"]["limit"] == 0

    @patch("powerview.src.prices_api_client.requests.get")
    def test_get_elspot_prices_http_error(self, mock_get):
        """Test Elspotprices retrieval with HTTP error."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("400 Bad Request")
        mock_get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            get_elspot_prices("2025-09-01", "2025-09-30", ["DK1"])


class TestGetDayaheadPrices:
    """Tests for get_dayahead_prices function."""

    @patch("powerview.src.prices_api_client.requests.get")
    def test_get_dayahead_prices_success(self, mock_get):
        """Test successful DayAheadPrices retrieval."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "dataset": "DayAheadPrices",
            "records": [{"TimeUTC": "2025-10-01T00:00:00", "PriceArea": "DK1"}],
        }
        mock_get.return_value = mock_response

        result = get_dayahead_prices("2025-10-01", "2025-10-31", ["DK1", "DK2"])

        assert result["dataset"] == "DayAheadPrices"
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert call_args[0][0] == "https://api.energidataservice.dk/dataset/DayAheadPrices"
        assert (
            call_args[1]["params"]["columns"]
            == "TimeUTC,PriceArea,DayAheadPriceDKK,DayAheadPriceEUR"
        )
        assert call_args[1]["params"]["start"] == "2025-10-01"
        assert call_args[1]["params"]["end"] == "2025-10-31"
        assert call_args[1]["params"]["limit"] == 0

    @patch("powerview.src.prices_api_client.requests.get")
    def test_get_dayahead_prices_http_error(self, mock_get):
        """Test DayAheadPrices retrieval with HTTP error."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("400 Bad Request")
        mock_get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            get_dayahead_prices("2025-10-01", "2025-10-31", ["DK1"])


class TestGetPricesWithRetry:
    """Tests for get_prices_with_retry function."""

    @patch("powerview.src.prices_api_client.get_elspot_prices")
    def test_get_prices_with_retry_auto_selects_elspot(self, mock_get_elspot):
        """Test auto mode selects Elspotprices before transition date."""
        mock_get_elspot.return_value = {"dataset": "Elspotprices", "records": []}

        result = get_prices_with_retry("2025-09-01", "2025-09-30", ["DK1"], dataset="auto")

        assert result == {"dataset": "Elspotprices", "records": []}
        mock_get_elspot.assert_called_once_with("2025-09-01", "2025-09-30", ["DK1"])

    @patch("powerview.src.prices_api_client.get_dayahead_prices")
    def test_get_prices_with_retry_auto_selects_dayahead(self, mock_get_dayahead):
        """Test auto mode selects DayAheadPrices from transition date."""
        mock_get_dayahead.return_value = {"dataset": "DayAheadPrices", "records": []}

        result = get_prices_with_retry("2025-10-01", "2025-10-31", ["DK1"], dataset="auto")

        assert result == {"dataset": "DayAheadPrices", "records": []}
        mock_get_dayahead.assert_called_once_with("2025-10-01", "2025-10-31", ["DK1"])

    @patch("powerview.src.prices_api_client.time.sleep")
    @patch("powerview.src.prices_api_client.get_dayahead_prices")
    def test_get_prices_with_retry_429_then_success(self, mock_get_dayahead, mock_sleep):
        """Test retry on HTTP 429 (rate limited) then success."""
        mock_response = MagicMock()
        mock_response.status_code = 429
        error = requests.HTTPError()
        error.response = mock_response

        mock_get_dayahead.side_effect = [error, {"dataset": "DayAheadPrices", "records": []}]

        result = get_prices_with_retry("2025-10-01", "2025-10-31", dataset="dayahead")

        assert result == {"dataset": "DayAheadPrices", "records": []}
        assert mock_get_dayahead.call_count == 2
        mock_sleep.assert_called_once_with(60)

    @patch("powerview.src.prices_api_client.time.sleep")
    @patch("powerview.src.prices_api_client.get_elspot_prices")
    def test_get_prices_with_retry_503_then_success(self, mock_get_elspot, mock_sleep):
        """Test retry on HTTP 503 (service unavailable) then success."""
        mock_response = MagicMock()
        mock_response.status_code = 503
        error = requests.HTTPError()
        error.response = mock_response

        mock_get_elspot.side_effect = [error, {"dataset": "Elspotprices", "records": []}]

        result = get_prices_with_retry("2025-09-01", "2025-09-30", dataset="elspot")

        assert result == {"dataset": "Elspotprices", "records": []}
        assert mock_get_elspot.call_count == 2
        mock_sleep.assert_called_once_with(60)

    @patch("powerview.src.prices_api_client.time.sleep")
    @patch("powerview.src.prices_api_client.get_elspot_prices")
    def test_get_prices_with_retry_max_retries_exceeded(self, mock_get_elspot, mock_sleep):
        """Test max retries exceeded for rate limiting."""
        mock_response = MagicMock()
        mock_response.status_code = 429
        error = requests.HTTPError()
        error.response = mock_response

        mock_get_elspot.side_effect = error

        with pytest.raises(requests.HTTPError):
            get_prices_with_retry("2025-09-01", "2025-09-30", dataset="elspot", max_retries=2)

        assert mock_get_elspot.call_count == 2
        assert mock_sleep.call_count == 1

    @patch("powerview.src.prices_api_client.get_dayahead_prices")
    def test_get_prices_with_retry_non_retryable_error(self, mock_get_dayahead):
        """Test non-retryable HTTP error raises immediately."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        error = requests.HTTPError()
        error.response = mock_response

        mock_get_dayahead.side_effect = error

        with pytest.raises(requests.HTTPError):
            get_prices_with_retry("2025-10-01", "2025-10-31", dataset="dayahead")

        mock_get_dayahead.assert_called_once()

    def test_get_prices_with_retry_invalid_dataset(self):
        """Test unsupported dataset value raises ValueError."""
        with pytest.raises(ValueError, match="dataset must be one of"):
            get_prices_with_retry("2025-10-01", "2025-10-31", dataset="unknown")
