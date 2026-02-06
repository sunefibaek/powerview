"""Integration tests for the reporting-layer builder."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

import duckdb
from powerview.src.reporting import build_reporting_layer


def _write_sample_parquet(base_dir):
    base_ts = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    records = []
    for hour in range(24):
        records.append(
            {
                "metering_point_id": "meter_001",
                "timestamp": base_ts + timedelta(hours=hour),
                "consumption_value": float(hour + 1),
                "quality": "A",
                "unit": "kWh",
                "ingestion_timestamp": base_ts,
                "ingestion_date": base_ts.date(),
            }
        )

    df = pd.DataFrame.from_records(records)
    partition_path = base_dir / "metering_point=meter_001" / "date=2025-01-01"
    partition_path.mkdir(parents=True, exist_ok=True)
    file_path = partition_path / "consumption_data.parquet"
    df.to_parquet(file_path, engine="pyarrow", index=False)
    return file_path


def test_build_reporting_layer_creates_views(tmp_path, monkeypatch):
    """Ensure the builder wires Parquet data into analytics views."""

    data_root = tmp_path / "data"
    data_root.mkdir()
    _write_sample_parquet(data_root)
    analytics_path = tmp_path / "duckdb" / "analytics.duckdb"

    config = {
        "data_storage_path": data_root,
        "analytics_db_path": analytics_path,
        "metering_points": {
            "meter_001": {
                "id": "meter_001",
                "name": "Solar",
                "type": "production",
                "location": "Home",
                "description": "Test meter",
            }
        },
    }

    monkeypatch.setattr("powerview.src.reporting.load_config", lambda: config)

    build_reporting_layer()

    connection = duckdb.connect(analytics_path.as_posix())
    try:
        total_kwh = connection.execute(
            "SELECT SUM(total_kwh) FROM reporting.daily_consumption"
        ).fetchone()[0]
        assert total_kwh == sum(range(1, 25))

        metadata_rows = connection.execute(
            "SELECT DISTINCT name FROM reporting.meter_metadata_enriched"
        ).fetchall()
        assert metadata_rows == [("Solar",)]

        missing_row = connection.execute(
            "SELECT missing_readings FROM reporting.missing_data_summary"
        ).fetchone()[0]
        assert missing_row == 0
    finally:
        connection.close()


def test_build_reporting_layer_missing_data_path(tmp_path, monkeypatch):
    """Builder should fail fast when the data directory is absent."""

    analytics_path = tmp_path / "duckdb" / "analytics.duckdb"
    config = {
        "data_storage_path": tmp_path / "missing-data",
        "analytics_db_path": analytics_path,
        "metering_points": {"meter_001": {"id": "meter_001", "name": "Solar"}},
    }

    monkeypatch.setattr("powerview.src.reporting.load_config", lambda: config)

    with pytest.raises(FileNotFoundError):
        build_reporting_layer()
