"""Unit tests for the extract module."""

from datetime import UTC, date, datetime, timedelta

import pytest

from powerview.src.extract import (
    chunk_date_range,
    get_timeframe,
    normalize_api_response,
)


class TestGetTimeframe:
    """Tests for get_timeframe function."""

    @pytest.fixture
    def mock_duckdb(self, monkeypatch):
        """Mock DuckDB for state queries."""
        from unittest.mock import patch

        with patch("powerview.src.extract.get_last_ingestion_date") as mock_get:
            yield mock_get

    def test_get_timeframe_no_prior_ingestion(self, mock_duckdb):
        """Test timeframe calculation with no prior ingestion (full backfill)."""
        mock_duckdb.return_value = None

        date_from, date_to = get_timeframe("test_mp_id", initial_backfill_days=1095)

        assert date_to == datetime.now(UTC).date()
        assert (date_to - date_from).days == 1095

    def test_get_timeframe_with_prior_ingestion(self, mock_duckdb):
        """Test timeframe calculation with prior ingestion (7-day overlap)."""
        last_date = date(2025, 11, 20)
        mock_duckdb.return_value = last_date

        date_from, date_to = get_timeframe("test_mp_id")

        # Should start 7 days before last ingestion
        assert date_from == date(2025, 11, 13)
        assert date_to == datetime.now(UTC).date()


class TestChunkDateRange:
    """Tests for chunk_date_range function."""

    def test_chunk_date_range_within_single_chunk(self):
        """Test date range that fits within single chunk."""
        date_from = date(2025, 11, 1)
        date_to = date(2025, 11, 30)

        chunks = chunk_date_range(date_from, date_to, chunk_days=90)

        assert len(chunks) == 1
        assert chunks[0] == (date(2025, 11, 1), date(2025, 11, 30))

    def test_chunk_date_range_multiple_chunks(self):
        """Test date range that requires multiple chunks."""
        date_from = date(2025, 1, 1)
        date_to = date(2025, 12, 31)

        chunks = chunk_date_range(date_from, date_to, chunk_days=90)

        # Should split into 4 chunks of ~90 days each
        assert len(chunks) == 4
        assert chunks[0][0] == date(2025, 1, 1)
        assert chunks[-1][1] == date(2025, 12, 31)

    def test_chunk_date_range_exactly_90_days(self):
        """Test date range of exactly 90 days."""
        date_from = date(2025, 1, 1)
        date_to = date(2025, 3, 31)

        chunks = chunk_date_range(date_from, date_to, chunk_days=90)

        assert len(chunks) == 1
        assert chunks[0] == (date_from, date_to)


class TestNormalizeApiResponse:
    """Tests for normalize_api_response function."""

    def test_normalize_empty_response(self):
        """Test normalizing empty API response."""
        api_response = {"result": []}
        metering_point_ids = {"mp1": "571313113150035634"}

        records = normalize_api_response(api_response, metering_point_ids)

        assert len(records) == 0

    def test_normalize_unsuccessful_result(self):
        """Test normalizing unsuccessful API result."""
        api_response = {
            "result": [
                {
                    "success": False,
                    "errorText": "Some error",
                    "MyEnergyData_MarketDocument": {},
                }
            ]
        }
        metering_point_ids = {"mp1": "571313113150035634"}

        records = normalize_api_response(api_response, metering_point_ids)

        assert len(records) == 0

    def test_normalize_single_point(self):
        """Test normalizing single consumption point with correct flat field structure."""
        now = datetime.now(UTC)
        api_response = {
            "result": [
                {
                    "success": True,
                    "MyEnergyData_MarketDocument": {
                        "TimeSeries": [
                            {
                                "MarketEvaluationPoint": {"mRID": {"name": "571313113150035634"}},
                                "measurement_Unit": {"name": "KWH"},
                                "Period": [
                                    {
                                        "timeInterval": {"start": now.isoformat()},
                                        "Point": [
                                            {
                                                "position": "1",
                                                "out_Quantity.quantity": "0.5",
                                                "out_Quantity.quality": "A04",
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
        metering_point_ids = {"mp1": "571313113150035634"}

        records = normalize_api_response(api_response, metering_point_ids)

        assert len(records) == 1
        assert records[0]["metering_point_id"] == "571313113150035634"
        assert records[0]["consumption_value"] == 0.5
        assert records[0]["quality"] == "A04"
        assert records[0]["unit"] == "KWH"

    def test_normalize_multiple_points_same_period(self):
        """Test normalizing multiple hourly points in same period."""
        now = datetime.now(UTC)
        api_response = {
            "result": [
                {
                    "success": True,
                    "MyEnergyData_MarketDocument": {
                        "TimeSeries": [
                            {
                                "MarketEvaluationPoint": {"mRID": {"name": "571313113150035634"}},
                                "measurement_Unit": {"name": "KWH"},
                                "Period": [
                                    {
                                        "timeInterval": {"start": now.isoformat()},
                                        "Point": [
                                            {
                                                "position": "1",
                                                "out_Quantity.quantity": "0.5",
                                                "out_Quantity.quality": "A04",
                                            },
                                            {
                                                "position": "2",
                                                "out_Quantity.quantity": "0.6",
                                                "out_Quantity.quality": "A04",
                                            },
                                            {
                                                "position": "3",
                                                "out_Quantity.quantity": "0.7",
                                                "out_Quantity.quality": "A04",
                                            },
                                        ],
                                    }
                                ],
                            }
                        ]
                    },
                }
            ]
        }
        metering_point_ids = {"mp1": "571313113150035634"}

        records = normalize_api_response(api_response, metering_point_ids)

        assert len(records) == 3
        assert records[0]["consumption_value"] == 0.5
        assert records[1]["consumption_value"] == 0.6
        assert records[2]["consumption_value"] == 0.7

    def test_normalize_timestamp_calculation(self):
        """Test that timestamps are calculated correctly (position offset)."""
        period_start = datetime(2025, 11, 15, 23, 0, 0, tzinfo=UTC)
        api_response = {
            "result": [
                {
                    "success": True,
                    "MyEnergyData_MarketDocument": {
                        "TimeSeries": [
                            {
                                "MarketEvaluationPoint": {"mRID": {"name": "571313113150035634"}},
                                "measurement_Unit": {"name": "KWH"},
                                "Period": [
                                    {
                                        "timeInterval": {"start": period_start.isoformat()},
                                        "Point": [
                                            {
                                                "position": "1",
                                                "out_Quantity.quantity": "0.5",
                                                "out_Quantity.quality": "A04",
                                            },
                                            {
                                                "position": "2",
                                                "out_Quantity.quantity": "0.6",
                                                "out_Quantity.quality": "A04",
                                            },
                                        ],
                                    }
                                ],
                            }
                        ]
                    },
                }
            ]
        }
        metering_point_ids = {"mp1": "571313113150035634"}

        records = normalize_api_response(api_response, metering_point_ids)

        # Position 1 = period_start + 0 hours
        assert records[0]["timestamp"] == period_start
        # Position 2 = period_start + 1 hour
        assert records[1]["timestamp"] == period_start + timedelta(hours=1)

    def test_normalize_filters_untracked_metering_points(self):
        """Test that untracked metering points are filtered out."""
        now = datetime.now(UTC)
        api_response = {
            "result": [
                {
                    "success": True,
                    "MyEnergyData_MarketDocument": {
                        "TimeSeries": [
                            {
                                "MarketEvaluationPoint": {"mRID": {"name": "untracked_mp"}},
                                "measurement_Unit": {"name": "KWH"},
                                "Period": [
                                    {
                                        "timeInterval": {"start": now.isoformat()},
                                        "Point": [
                                            {
                                                "position": "1",
                                                "out_Quantity.quantity": "0.5",
                                                "out_Quantity.quality": "A04",
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
        metering_point_ids = {"mp1": "tracked_mp_only"}

        records = normalize_api_response(api_response, metering_point_ids)

        assert len(records) == 0

    def test_normalize_missing_quantity_defaults_to_zero(self):
        """Test that missing quantity defaults to 0."""
        now = datetime.now(UTC)
        api_response = {
            "result": [
                {
                    "success": True,
                    "MyEnergyData_MarketDocument": {
                        "TimeSeries": [
                            {
                                "MarketEvaluationPoint": {"mRID": {"name": "571313113150035634"}},
                                "measurement_Unit": {"name": "KWH"},
                                "Period": [
                                    {
                                        "timeInterval": {"start": now.isoformat()},
                                        "Point": [
                                            {
                                                "position": "1",
                                                # Missing out_Quantity.quantity field
                                                "out_Quantity.quality": "A04",
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
        metering_point_ids = {"mp1": "571313113150035634"}

        records = normalize_api_response(api_response, metering_point_ids)

        assert len(records) == 1
        assert records[0]["consumption_value"] == 0.0

    def test_normalize_string_quantity_converted_to_float(self):
        """Test that string quantity is properly converted to float."""
        now = datetime.now(UTC)
        api_response = {
            "result": [
                {
                    "success": True,
                    "MyEnergyData_MarketDocument": {
                        "TimeSeries": [
                            {
                                "MarketEvaluationPoint": {"mRID": {"name": "571313113150035634"}},
                                "measurement_Unit": {"name": "KWH"},
                                "Period": [
                                    {
                                        "timeInterval": {"start": now.isoformat()},
                                        "Point": [
                                            {
                                                "position": "1",
                                                "out_Quantity.quantity": "1.234",
                                                "out_Quantity.quality": "A04",
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
        metering_point_ids = {"mp1": "571313113150035634"}

        records = normalize_api_response(api_response, metering_point_ids)

        assert len(records) == 1
        assert records[0]["consumption_value"] == 1.234
