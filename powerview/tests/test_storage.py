"""Unit tests for the storage module."""

from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from powerview.src.storage import (
    get_last_ingestion_date,
    init_duckdb_state,
    save_to_parquet,
    update_last_ingestion_date,
)


class TestInitDuckdbState:
    """Tests for init_duckdb_state function."""

    @patch("powerview.src.storage.duckdb.connect")
    def test_init_duckdb_state_success(self, mock_connect):
        """Test successful DuckDB state initialization."""
        mock_con = MagicMock()
        mock_connect.return_value = mock_con

        init_duckdb_state("test.duckdb")

        mock_connect.assert_called_once_with("test.duckdb")
        mock_con.execute.assert_called_once()
        mock_con.close.assert_called_once()

    @patch("powerview.src.storage.duckdb.connect")
    def test_init_duckdb_state_error(self, mock_connect):
        """Test DuckDB state initialization with error."""
        mock_connect.side_effect = Exception("Connection failed")

        with pytest.raises(Exception):  # noqa B017
            init_duckdb_state("test.duckdb")


class TestGetLastIngestionDate:
    """Tests for get_last_ingestion_date function."""

    @patch("powerview.src.storage.duckdb.connect")
    def test_get_last_ingestion_date_exists(self, mock_connect):
        """Test retrieving existing last ingestion date."""
        mock_con = MagicMock()
        test_date = date(2025, 11, 20)
        mock_con.execute().fetchone.return_value = (test_date,)
        mock_connect.return_value = mock_con

        result = get_last_ingestion_date("123456789012345678", "test.duckdb")

        assert result == test_date
        mock_con.close.assert_called_once()

    @patch("powerview.src.storage.duckdb.connect")
    def test_get_last_ingestion_date_not_found(self, mock_connect):
        """Test retrieving when no ingestion date exists."""
        mock_con = MagicMock()
        mock_con.execute().fetchone.return_value = None
        mock_connect.return_value = mock_con

        result = get_last_ingestion_date("123456789012345678", "test.duckdb")

        assert result is None
        mock_con.close.assert_called_once()

    @patch("powerview.src.storage.duckdb.connect")
    def test_get_last_ingestion_date_error(self, mock_connect):
        """Test error handling in get_last_ingestion_date."""
        mock_connect.side_effect = Exception("Connection failed")

        result = get_last_ingestion_date("123456789012345678", "test.duckdb")

        assert result is None


class TestUpdateLastIngestionDate:
    """Tests for update_last_ingestion_date function."""

    @patch("powerview.src.storage.duckdb.connect")
    def test_update_last_ingestion_date_success(self, mock_connect):
        """Test successful state update."""
        mock_con = MagicMock()
        mock_connect.return_value = mock_con
        test_date = date(2025, 11, 21)

        update_last_ingestion_date("123456789012345678", test_date, "test.duckdb")

        mock_con.execute.assert_called_once()
        mock_con.close.assert_called_once()

    @patch("powerview.src.storage.duckdb.connect")
    def test_update_last_ingestion_date_retry_then_success(self, mock_connect):
        """Test retry logic on first failure then success."""
        mock_con = MagicMock()
        # First call fails, second succeeds
        mock_con.execute.side_effect = [Exception("Lock"), None]
        mock_connect.return_value = mock_con
        test_date = date(2025, 11, 21)

        update_last_ingestion_date("123456789012345678", test_date, "test.duckdb")

        assert mock_con.execute.call_count == 2
        assert mock_con.close.call_count == 1

    @patch("powerview.src.storage.duckdb.connect")
    def test_update_last_ingestion_date_max_retries_exceeded(self, mock_connect):
        """Test max retries exceeded."""
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("Lock")
        mock_connect.return_value = mock_con
        test_date = date(2025, 11, 21)

        with pytest.raises(Exception):  # noqa B017
            update_last_ingestion_date("123456789012345678", test_date, "test.duckdb")

        assert mock_con.execute.call_count == 3


class TestSaveToParquet:
    """Tests for save_to_parquet function."""

    def test_save_to_parquet_no_records(self, tmp_path):
        """Test saving with no records."""
        save_to_parquet([], str(tmp_path))

        # Should create no files
        files = list(tmp_path.glob("**/*.parquet"))
        assert len(files) == 0

    def test_save_to_parquet_single_date(self, tmp_path):
        """Test saving records from a single consumption date."""
        now = datetime.now(UTC)
        consumption_date = now.date()

        records = [
            {
                "metering_point_id": "571313113150035634",
                "timestamp": now,
                "consumption_value": 0.5,
                "quality": "A04",
                "unit": "kWh",
                "ingestion_timestamp": now,
                "ingestion_date": consumption_date,
            },
            {
                "metering_point_id": "571313113150035634",
                "timestamp": now + timedelta(hours=1),
                "consumption_value": 0.6,
                "quality": "A04",
                "unit": "kWh",
                "ingestion_timestamp": now,
                "ingestion_date": consumption_date,
            },
        ]

        save_to_parquet(records, str(tmp_path))

        # Verify file structure: metering_point=<ID>/date=<YYYY-MM-DD>/consumption_data.parquet
        expected_path = (
            tmp_path
            / "metering_point=571313113150035634"
            / f"date={consumption_date}"
            / "consumption_data.parquet"
        )
        assert expected_path.exists()

        # Verify data
        df = pd.read_parquet(expected_path)
        assert len(df) == 2
        assert list(df["metering_point_id"].unique()) == ["571313113150035634"]

    def test_save_to_parquet_multiple_dates(self, tmp_path):
        """Test saving records spanning multiple consumption dates."""
        now = datetime.now(UTC)
        date_1 = now.date()
        date_2 = (now + timedelta(days=1)).date()

        records = [
            {
                "metering_point_id": "571313113150035634",
                "timestamp": now,
                "consumption_value": 0.5,
                "quality": "A04",
                "unit": "kWh",
                "ingestion_timestamp": now,
                "ingestion_date": date_1,
            },
            {
                "metering_point_id": "571313113150035634",
                "timestamp": now + timedelta(days=1),
                "consumption_value": 0.6,
                "quality": "A04",
                "unit": "kWh",
                "ingestion_timestamp": now,
                "ingestion_date": date_2,
            },
        ]

        save_to_parquet(records, str(tmp_path))

        # Verify separate folders for each consumption date
        path_1 = (
            tmp_path
            / "metering_point=571313113150035634"
            / f"date={date_1}"
            / "consumption_data.parquet"
        )
        path_2 = (
            tmp_path
            / "metering_point=571313113150035634"
            / f"date={date_2}"
            / "consumption_data.parquet"
        )
        assert path_1.exists()
        assert path_2.exists()

        # Verify data in each file
        df_1 = pd.read_parquet(path_1)
        df_2 = pd.read_parquet(path_2)
        assert len(df_1) == 1
        assert len(df_2) == 1
        assert float(df_1["consumption_value"].iloc[0]) == 0.5
        assert float(df_2["consumption_value"].iloc[0]) == 0.6

    def test_save_to_parquet_multiple_metering_points(self, tmp_path):
        """Test saving records from multiple metering points."""
        now = datetime.now(UTC)
        consumption_date = now.date()

        records = [
            {
                "metering_point_id": "571313113150035634",
                "timestamp": now,
                "consumption_value": 0.5,
                "quality": "A04",
                "unit": "kWh",
                "ingestion_timestamp": now,
                "ingestion_date": consumption_date,
            },
            {
                "metering_point_id": "571313114500163366",
                "timestamp": now,
                "consumption_value": 0.7,
                "quality": "A04",
                "unit": "kWh",
                "ingestion_timestamp": now,
                "ingestion_date": consumption_date,
            },
        ]

        save_to_parquet(records, str(tmp_path))

        # Verify separate folders for each metering point
        path_1 = (
            tmp_path
            / "metering_point=571313113150035634"
            / f"date={consumption_date}"
            / "consumption_data.parquet"
        )
        path_2 = (
            tmp_path
            / "metering_point=571313114500163366"
            / f"date={consumption_date}"
            / "consumption_data.parquet"
        )
        assert path_1.exists()
        assert path_2.exists()

    def test_save_to_parquet_upsert_duplicate_timestamps(self, tmp_path):
        """Test upsert logic with duplicate timestamps."""
        now = datetime.now(UTC)
        consumption_date = now.date()
        mp_id = "571313113150035634"

        # First save
        records_1 = [
            {
                "metering_point_id": mp_id,
                "timestamp": now,
                "consumption_value": 0.5,
                "quality": "A04",
                "unit": "kWh",
                "ingestion_timestamp": now,
                "ingestion_date": consumption_date,
            }
        ]
        save_to_parquet(records_1, str(tmp_path))

        # Second save with same timestamp but different value (upsert)
        records_2 = [
            {
                "metering_point_id": mp_id,
                "timestamp": now,
                "consumption_value": 0.6,  # Different value
                "quality": "A04",
                "unit": "kWh",
                "ingestion_timestamp": now,
                "ingestion_date": consumption_date,
            }
        ]
        save_to_parquet(records_2, str(tmp_path))

        # Verify only one record with updated value
        path = (
            tmp_path
            / f"metering_point={mp_id}"
            / f"date={consumption_date}"
            / "consumption_data.parquet"
        )
        df = pd.read_parquet(path)
        assert len(df) == 1
        assert float(df["consumption_value"].iloc[0]) == 0.6

    def test_save_to_parquet_preserves_schema(self, tmp_path):
        """Test that saved Parquet preserves all schema fields."""
        now = datetime.now(UTC)
        consumption_date = now.date()

        records = [
            {
                "metering_point_id": "571313113150035634",
                "timestamp": now,
                "consumption_value": 0.5,
                "quality": "A04",
                "unit": "kWh",
                "ingestion_timestamp": now,
                "ingestion_date": consumption_date,
            }
        ]

        save_to_parquet(records, str(tmp_path))

        path = (
            tmp_path
            / "metering_point=571313113150035634"
            / f"date={consumption_date}"
            / "consumption_data.parquet"
        )
        df = pd.read_parquet(path)

        # Verify all schema fields are present
        expected_columns = {
            "metering_point_id",
            "timestamp",
            "consumption_value",
            "quality",
            "unit",
            "ingestion_timestamp",
            "ingestion_date",
        }
        assert set(df.columns) == expected_columns
