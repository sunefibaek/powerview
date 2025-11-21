"""Unit tests for the auth module."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from powerview.src.auth import get_access_token


class TestGetAccessToken:
    """Tests for get_access_token function."""

    @patch("powerview.src.auth.requests.get")
    def test_get_access_token_success(self, mock_get):
        """Test successful token exchange."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"result": "test_access_token_123"}
        mock_get.return_value = mock_response

        token = get_access_token("test_refresh_token")

        assert token == "test_access_token_123"
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert call_args[0][0] == "https://api.eloverblik.dk/CustomerApi/api/token"
        assert call_args[1]["headers"]["Authorization"] == "Bearer test_refresh_token"

    @patch("powerview.src.auth.requests.get")
    def test_get_access_token_http_error(self, mock_get):
        """Test token exchange with HTTP error."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("401 Unauthorized")
        mock_get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            get_access_token("invalid_token")

    @patch("powerview.src.auth.requests.get")
    def test_get_access_token_missing_result(self, mock_get):
        """Test token exchange with missing result field."""
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_get.return_value = mock_response

        with pytest.raises(KeyError):
            get_access_token("test_refresh_token")

    @patch("powerview.src.auth.requests.get")
    def test_get_access_token_network_error(self, mock_get):
        """Test token exchange with network error."""
        mock_get.side_effect = requests.ConnectionError("Network error")

        with pytest.raises(requests.ConnectionError):
            get_access_token("test_refresh_token")
