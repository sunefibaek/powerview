"""Unit tests for the config module."""

import textwrap
from unittest.mock import patch

import pytest

from powerview.src.config import load_config


class TestLoadConfig:
    """Tests for load_config function."""

    @staticmethod
    def _write_metering_points_file(tmp_path, content: str) -> str:
        file_path = tmp_path / "metering_points.yml"
        file_path.write_text(textwrap.dedent(content), encoding="utf-8")
        return str(file_path)

    def test_load_config_success(self, tmp_path, monkeypatch):
        """Test successful configuration loading."""

        file_path = self._write_metering_points_file(
            tmp_path,
            """
            metering_points:
              "meter_001":
                name: Solar Export
                type: production
            """,
        )

        monkeypatch.setenv("ELOVERBLIK_REFRESH_TOKEN", "test_token_123")
        monkeypatch.setenv("METERING_POINTS_FILE", file_path)

        with patch("powerview.src.config.load_dotenv"):
            config = load_config()

        assert config["refresh_token"] == "test_token_123"
        assert config["metering_points"]["meter_001"]["name"] == "Solar Export"
        assert config["valid_metering_points"]["Solar Export"] == "meter_001"
        assert config["data_storage_path"] == "./data"
        assert config["state_db_path"] == "./state.duckdb"

    def test_load_config_missing_refresh_token(self, tmp_path, monkeypatch):
        """Test error when refresh token is missing."""

        file_path = self._write_metering_points_file(
            tmp_path,
            """
            metering_points:
              "meter_001":
                name: Solar Export
            """,
        )

        monkeypatch.setenv("METERING_POINTS_FILE", file_path)

        with patch("powerview.src.config.load_dotenv"):
            with pytest.raises(ValueError, match="ELOVERBLIK_REFRESH_TOKEN is required"):
                load_config()

    def test_load_config_no_metering_points(self, tmp_path, monkeypatch):
        """Test error when no metering points configured."""

        file_path = self._write_metering_points_file(
            tmp_path,
            """
            metering_points: {}
            """,
        )

        monkeypatch.setenv("ELOVERBLIK_REFRESH_TOKEN", "test_token_123")
        monkeypatch.setenv("METERING_POINTS_FILE", file_path)

        with patch("powerview.src.config.load_dotenv"):
            with pytest.raises(
                ValueError,
                match="At least one metering point ID must be configured",
            ):
                load_config()

    def test_load_config_custom_values(self, tmp_path, monkeypatch):
        """Test loading configuration with custom values."""

        file_path = self._write_metering_points_file(
            tmp_path,
            """
            metering_points:
              "meter_001":
                name: Solar Export
              "meter_002":
                name: Grid Import
            """,
        )

        monkeypatch.setenv("ELOVERBLIK_REFRESH_TOKEN", "test_token_123")
        monkeypatch.setenv("METERING_POINTS_FILE", file_path)
        monkeypatch.setenv("DATA_STORAGE_PATH", "/custom/data")
        monkeypatch.setenv("STATE_DB_PATH", "/custom/state.duckdb")
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        monkeypatch.setenv("INITIAL_BACKFILL_DAYS", "365")

        with patch("powerview.src.config.load_dotenv"):
            config = load_config()

        assert config["refresh_token"] == "test_token_123"
        assert len(config["valid_metering_points"]) == 2
        assert config["data_storage_path"] == "/custom/data"
        assert config["state_db_path"] == "/custom/state.duckdb"
        assert config["log_level"] == "DEBUG"
        assert config["initial_backfill_days"] == 365

    def test_load_config_name_fallback(self, tmp_path, monkeypatch):
        """Ensure IDs become keys when names are missing."""

        file_path = self._write_metering_points_file(
            tmp_path,
            """
            metering_points:
              "meter_001": {}
            """,
        )

        monkeypatch.setenv("ELOVERBLIK_REFRESH_TOKEN", "test_token_123")
        monkeypatch.setenv("METERING_POINTS_FILE", file_path)

        with patch("powerview.src.config.load_dotenv"):
            config = load_config()

        assert config["valid_metering_points"]["meter_001"] == "meter_001"
