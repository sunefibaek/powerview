"""Unit tests for the main module."""

from datetime import date
from unittest.mock import patch

from powerview.src.main import main


class TestMain:
    """Tests for main function."""

    @patch("powerview.src.main.load_config")
    @patch("powerview.src.main.init_duckdb_state")
    @patch("powerview.src.main.get_access_token")
    @patch("powerview.src.main.get_timeframe")
    @patch("powerview.src.main.chunk_date_range")
    @patch("powerview.src.main.get_meter_data_with_retry")
    @patch("powerview.src.main.normalize_api_response")
    @patch("powerview.src.main.save_to_parquet")
    @patch("powerview.src.main.update_last_ingestion_date")
    def test_main_success(
        self,
        mock_update_state,
        mock_save,
        mock_normalize,
        mock_get_data,
        mock_chunk,
        mock_timeframe,
        mock_token,
        mock_init_db,
        mock_config,
    ):
        """Test successful main execution with one metering point."""
        # Setup mocks
        mock_config.return_value = {
            "log_level": "INFO",
            "state_db_path": "./test.duckdb",
            "refresh_token": "test_refresh",
            "valid_metering_points": {"test_meter": "123456"},
            "initial_backfill_days": 30,
            "data_storage_path": "./test_data",
        }
        mock_token.return_value = "test_access_token"
        mock_timeframe.return_value = (date(2025, 11, 1), date(2025, 12, 1))
        mock_chunk.return_value = [(date(2025, 11, 1), date(2025, 12, 1))]
        mock_get_data.return_value = {"result": []}
        mock_normalize.return_value = []

        # Execute
        main()

        # Verify
        mock_config.assert_called_once()
        mock_init_db.assert_called_once_with("./test.duckdb")
        mock_token.assert_called_once_with("test_refresh")
        mock_timeframe.assert_called_once()
        mock_get_data.assert_called_once()
        mock_normalize.assert_called_once()
        mock_save.assert_called_once()
        mock_update_state.assert_called_once()

    @patch("powerview.src.main.load_config")
    def test_main_config_error(self, mock_config):
        """Test main handles configuration errors gracefully."""
        mock_config.side_effect = ValueError("Missing config")

        # Should not raise, just log and return
        main()

        mock_config.assert_called_once()

    @patch("powerview.src.main.load_config")
    @patch("powerview.src.main.init_duckdb_state")
    def test_main_db_init_error(self, mock_init_db, mock_config):
        """Test main handles database initialization errors."""
        mock_config.return_value = {
            "log_level": "INFO",
            "state_db_path": "./test.duckdb",
            "refresh_token": "test_refresh",
            "valid_metering_points": {},
            "initial_backfill_days": 30,
            "data_storage_path": "./test_data",
        }
        mock_init_db.side_effect = Exception("DB error")

        # Should not raise, just log and return
        main()

        mock_init_db.assert_called_once()

    @patch("powerview.src.main.load_config")
    @patch("powerview.src.main.init_duckdb_state")
    @patch("powerview.src.main.get_access_token")
    def test_main_token_error(self, mock_token, mock_init_db, mock_config):
        """Test main handles token retrieval errors."""
        mock_config.return_value = {
            "log_level": "INFO",
            "state_db_path": "./test.duckdb",
            "refresh_token": "test_refresh",
            "valid_metering_points": {},
            "initial_backfill_days": 30,
            "data_storage_path": "./test_data",
        }
        mock_token.side_effect = Exception("Token error")

        # Should not raise, just log and return
        main()

        mock_token.assert_called_once()

    @patch("powerview.src.main.load_config")
    @patch("powerview.src.main.init_duckdb_state")
    @patch("powerview.src.main.get_access_token")
    @patch("powerview.src.main.get_timeframe")
    @patch("powerview.src.main.chunk_date_range")
    @patch("powerview.src.main.get_meter_data_with_retry")
    @patch("powerview.src.main.normalize_api_response")
    @patch("powerview.src.main.save_to_parquet")
    @patch("powerview.src.main.update_last_ingestion_date")
    def test_main_multiple_metering_points(
        self,
        mock_update_state,
        mock_save,
        mock_normalize,
        mock_get_data,
        mock_chunk,
        mock_timeframe,
        mock_token,
        mock_init_db,
        mock_config,
    ):
        """Test main processes multiple metering points."""
        mock_config.return_value = {
            "log_level": "INFO",
            "state_db_path": "./test.duckdb",
            "refresh_token": "test_refresh",
            "valid_metering_points": {
                "meter1": "123456",
                "meter2": "789012",
            },
            "initial_backfill_days": 30,
            "data_storage_path": "./test_data",
        }
        mock_token.return_value = "test_access_token"
        mock_timeframe.return_value = (date(2025, 11, 1), date(2025, 12, 1))
        mock_chunk.return_value = [(date(2025, 11, 1), date(2025, 12, 1))]
        mock_get_data.return_value = {"result": []}
        mock_normalize.return_value = []

        # Execute
        main()

        # Verify both metering points were processed
        assert mock_timeframe.call_count == 2
        assert mock_get_data.call_count == 2
        assert mock_update_state.call_count == 2

    @patch("powerview.src.main.load_config")
    @patch("powerview.src.main.init_duckdb_state")
    @patch("powerview.src.main.get_access_token")
    @patch("powerview.src.main.get_timeframe")
    @patch("powerview.src.main.chunk_date_range")
    @patch("powerview.src.main.get_meter_data_with_retry")
    def test_main_chunk_failure_no_state_update(
        self,
        mock_get_data,
        mock_chunk,
        mock_timeframe,
        mock_token,
        mock_init_db,
        mock_config,
    ):
        """Test main does not update state when all chunks fail."""
        mock_config.return_value = {
            "log_level": "INFO",
            "state_db_path": "./test.duckdb",
            "refresh_token": "test_refresh",
            "valid_metering_points": {"test_meter": "123456"},
            "initial_backfill_days": 30,
            "data_storage_path": "./test_data",
        }
        mock_token.return_value = "test_access_token"
        mock_timeframe.return_value = (date(2025, 11, 1), date(2025, 12, 1))
        mock_chunk.return_value = [(date(2025, 11, 1), date(2025, 12, 1))]
        mock_get_data.side_effect = Exception("API error")

        # Execute
        with patch("powerview.src.main.update_last_ingestion_date") as mock_update:
            main()

            # State should not be updated when all chunks fail
            mock_update.assert_not_called()

    @patch("powerview.src.main.load_config")
    @patch("powerview.src.main.init_duckdb_state")
    @patch("powerview.src.main.get_access_token")
    @patch("powerview.src.main.get_timeframe")
    @patch("powerview.src.main.chunk_date_range")
    @patch("powerview.src.main.get_meter_data_with_retry")
    @patch("powerview.src.main.normalize_api_response")
    @patch("powerview.src.main.save_to_parquet")
    @patch("powerview.src.main.update_last_ingestion_date")
    def test_main_multiple_chunks(
        self,
        mock_update_state,
        mock_save,
        mock_normalize,
        mock_get_data,
        mock_chunk,
        mock_timeframe,
        mock_token,
        mock_init_db,
        mock_config,
    ):
        """Test main processes multiple date chunks."""
        mock_config.return_value = {
            "log_level": "INFO",
            "state_db_path": "./test.duckdb",
            "refresh_token": "test_refresh",
            "valid_metering_points": {"test_meter": "123456"},
            "initial_backfill_days": 30,
            "data_storage_path": "./test_data",
        }
        mock_token.return_value = "test_access_token"
        mock_timeframe.return_value = (date(2025, 9, 1), date(2025, 12, 1))
        mock_chunk.return_value = [
            (date(2025, 9, 1), date(2025, 10, 1)),
            (date(2025, 10, 1), date(2025, 11, 1)),
            (date(2025, 11, 1), date(2025, 12, 1)),
        ]
        mock_get_data.return_value = {"result": []}
        mock_normalize.return_value = []

        # Execute
        main()

        # Verify all chunks were processed
        assert mock_get_data.call_count == 3
        assert mock_normalize.call_count == 3
        assert mock_save.call_count == 3
        # State updated once per metering point (after all chunks)
        mock_update_state.assert_called_once()
