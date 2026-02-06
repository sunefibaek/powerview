"""Utilities for loading metering-point metadata into DuckDB."""

from __future__ import annotations

import json
import logging
from typing import Any

import pandas as pd

import duckdb

logger = logging.getLogger(__name__)

_CORE_METADATA_FIELDS = {"id", "name", "type", "location", "description"}


def build_metadata_frame(metering_points: dict[str, dict[str, Any]]) -> pd.DataFrame:
    """Convert metering-point metadata into a normalized pandas DataFrame.

    Args:
        metering_points: Mapping of metering point IDs to metadata dictionaries.

    Returns:
        pandas.DataFrame: Normalized metadata ready for DuckDB ingestion.
    """

    records: list[dict[str, Any]] = []
    for meter_id, metadata in sorted(metering_points.items()):
        normalized = metadata or {}
        extras = {
            key: value for key, value in normalized.items() if key not in _CORE_METADATA_FIELDS
        }
        records.append(
            {
                "metering_point_id": meter_id,
                "name": normalized.get("name", meter_id),
                "type": normalized.get("type"),
                "location": normalized.get("location"),
                "description": normalized.get("description"),
                "extra_metadata": json.dumps(extras, ensure_ascii=True) if extras else None,
            }
        )

    if not records:
        return pd.DataFrame(
            columns=[
                "metering_point_id",
                "name",
                "type",
                "location",
                "description",
                "extra_metadata",
            ]
        )

    return pd.DataFrame.from_records(records)


def load_metadata_table(
    connection: duckdb.DuckDBPyConnection,
    metering_points: dict[str, dict[str, Any]],
    table_name: str = "reporting.meter_metadata",
) -> None:
    """Create or replace the DuckDB metadata table from YAML-derived data.

    Args:
        connection: Active DuckDB connection for the analytics database.
        metering_points: Mapping of metering point IDs to metadata dictionaries.
        table_name: Fully qualified table name to create/replace.
    """

    metadata_frame = build_metadata_frame(metering_points)
    connection.execute("CREATE SCHEMA IF NOT EXISTS reporting")

    connection.register("meter_metadata_df", metadata_frame)
    try:
        connection.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM meter_metadata_df
            """
        )
    finally:
        connection.unregister("meter_metadata_df")

    logger.info(
        "Loaded %d metering-point metadata row(s) into %s",
        len(metadata_frame.index),
        table_name,
    )
