"""Unit tests for the config module."""

import os
from unittest.mock import patch

import pytest

from powerview.src.config import load_config


class TestLoadConfig:
    """Tests for load_config function."""

    @patch.dict(
        os.environ,
        {
            "ELOVERBLIK_REFRESH_TOKEN": "test_token_123",
            "DELIVERY_TO_GRID_ID": "meter_001",
        },
    )
    def test_load_config_success(self):
        """Test successful configuration loading."""
        config = load_config()

        assert config["refresh_token"] == "test_token_123"
        assert config["valid_metering_points"]["delivery_to_grid"] == "meter_001"
        assert config["data_storage_path"] == "./data"
        assert config["state_db_path"] == "./state.duckdb"

    @patch("powerview.src.config.load_dotenv")
    @patch.dict(os.environ, {}, clear=True)
    def test_load_config_missing_refresh_token(self, mock_load_dotenv):
        """Test error when refresh token is missing."""
        with pytest.raises(ValueError, match="ELOVERBLIK_REFRESH_TOKEN is required"):
            load_config()

    @patch("powerview.src.config.load_dotenv")
    @patch.dict(
        os.environ,
        {"ELOVERBLIK_REFRESH_TOKEN": "test_token_123"},
        clear=True,
    )
    def test_load_config_no_metering_points(self, mock_load_dotenv):
        """Test error when no metering points configured."""
        with pytest.raises(ValueError, match="At least one metering point ID must be configured"):
            load_config()

    @patch.dict(
        os.environ,
        {
            "ELOVERBLIK_REFRESH_TOKEN": "test_token_123",
            "DELIVERY_TO_GRID_ID": "meter_001",
            "ELECTRIC_HEATING_ID": "meter_002",
            "CONSUMED_FROM_GRID_ID": "meter_003",
            "NET_CONSUMPTION_ID": "meter_004",
            "HJEM_METER_ID": "meter_005",
            "SOMMERHUS_METER_ID": "meter_006",
            "DATA_STORAGE_PATH": "/custom/data",
            "STATE_DB_PATH": "/custom/state.duckdb",
            "LOG_LEVEL": "DEBUG",
            "INITIAL_BACKFILL_DAYS": "365",
        },
    )
    def test_load_config_custom_values(self):
        """Test loading configuration with custom values."""
        config = load_config()

        assert config["refresh_token"] == "test_token_123"
        assert len(config["valid_metering_points"]) == 6
        assert config["data_storage_path"] == "/custom/data"
        assert config["state_db_path"] == "/custom/state.duckdb"
        assert config["log_level"] == "DEBUG"
        assert config["initial_backfill_days"] == 365

    @patch("powerview.src.config.load_dotenv")
    @patch.dict(
        os.environ,
        {
            "ELOVERBLIK_REFRESH_TOKEN": "test_token_123",
            "DELIVERY_TO_GRID_ID": "meter_001",
            "ELECTRIC_HEATING_ID": "",
        },
        clear=True,
    )
    def test_load_config_filters_none_metering_points(self, mock_load_dotenv):
        """Test that None/empty metering points are filtered out."""
        config = load_config()

        assert len(config["valid_metering_points"]) == 1
        assert "delivery_to_grid" in config["valid_metering_points"]
        assert "electric_heating" not in config["valid_metering_points"]
        assert "consumed_from_grid" not in config["valid_metering_points"]
