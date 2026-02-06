"""Tests for metadata utilities."""

import json

import duckdb
from powerview.src.metadata import build_metadata_frame, load_metadata_table


def test_build_metadata_frame_handles_extra_fields():
    """Ensure arbitrary metadata fields are preserved as JSON."""

    metering_points = {
        "meter_001": {
            "name": "Solar",
            "type": "production",
            "location": "Home",
            "custom": "value",
        }
    }

    frame = build_metadata_frame(metering_points)

    assert frame.loc[0, "metering_point_id"] == "meter_001"
    assert frame.loc[0, "name"] == "Solar"
    assert json.loads(frame.loc[0, "extra_metadata"]) == {"custom": "value"}


def test_load_metadata_table_creates_table(tmp_path):
    """Verify DuckDB table creation from metadata DataFrame."""

    metering_points = {
        "meter_001": {
            "name": "Solar",
            "type": "production",
            "location": "Home",
        }
    }

    db_path = tmp_path / "metadata.duckdb"
    connection = duckdb.connect(str(db_path))
    try:
        load_metadata_table(connection, metering_points)
        rows = connection.execute(
            "SELECT metering_point_id, name FROM reporting.meter_metadata"
        ).fetchall()
    finally:
        connection.close()

    assert rows == [("meter_001", "Solar")]
