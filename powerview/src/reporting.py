"""Reporting layer builder for analytics DuckDB."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import duckdb
from powerview.src.config import load_config
from powerview.src.metadata import load_metadata_table

logger = logging.getLogger(__name__)


def _escape_literal(value: str) -> str:
    """Escape single quotes for safe SQL embedding."""

    return value.replace("'", "''")


def _parquet_glob(base_path: Path) -> tuple[str, list[Path]]:
    """Return the parquet glob string and the files that match it."""

    pattern = base_path / "metering_point=*/date=*/consumption_data.parquet"
    files = sorted(base_path.glob("metering_point=*/date=*/consumption_data.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No Parquet files found under {base_path}. Run the extraction pipeline first."
        )
    return pattern.as_posix(), files


def _view_statements(parquet_glob: str) -> list[tuple[str, str]]:
    """Assemble SQL statements required for the reporting layer."""

    escaped_glob = _escape_literal(parquet_glob)
    statements = [
        (
            "reporting.meter_data_stage",
            f"""
            CREATE OR REPLACE VIEW reporting.meter_data_stage AS
            WITH source AS (
                SELECT
                    metering_point_id,
                    timezone('UTC', timestamp) AS reading_ts_utc,
                    consumption_value,
                    quality,
                    unit,
                    ingestion_timestamp,
                    ingestion_date
                FROM read_parquet('{escaped_glob}')
            )
            SELECT
                metering_point_id,
                reading_ts_utc,
                DATE(reading_ts_utc) AS reading_date,
                EXTRACT('hour' FROM reading_ts_utc) AS reading_hour,
                EXTRACT('isodow' FROM reading_ts_utc) AS reading_weekday,
                strftime(reading_ts_utc, '%A') AS reading_weekday_label,
                EXTRACT('month' FROM reading_ts_utc) AS reading_month,
                EXTRACT('year' FROM reading_ts_utc) AS reading_year,
                consumption_value,
                quality,
                unit,
                ingestion_timestamp,
                ingestion_date
            FROM source;
            """,
        ),
        (
            "reporting.meter_data_clean",
            """
            CREATE OR REPLACE VIEW reporting.meter_data_clean AS
            SELECT *
            FROM reporting.meter_data_stage
            WHERE consumption_value IS NOT NULL AND consumption_value >= 0;
            """,
        ),
        (
            "reporting.daily_consumption",
            """
            CREATE OR REPLACE VIEW reporting.daily_consumption AS
            SELECT
                metering_point_id,
                reading_date,
                SUM(consumption_value) AS total_kwh,
                AVG(consumption_value) AS avg_kwh,
                MIN(consumption_value) AS min_kwh,
                MAX(consumption_value) AS max_kwh,
                COUNT(*) AS reading_count
            FROM reporting.meter_data_clean
            GROUP BY metering_point_id, reading_date;
            """,
        ),
        (
            "reporting.monthly_consumption",
            """
            CREATE OR REPLACE VIEW reporting.monthly_consumption AS
            WITH monthly AS (
                SELECT
                    metering_point_id,
                    CAST(DATE_TRUNC('month', reading_date) AS DATE) AS month_start,
                    SUM(consumption_value) AS total_kwh
                FROM reporting.meter_data_clean
                GROUP BY metering_point_id, month_start
            ),
            enriched AS (
                SELECT
                    metering_point_id,
                    month_start,
                    total_kwh,
                    LAG(total_kwh) OVER (
                        PARTITION BY metering_point_id
                        ORDER BY month_start
                    ) AS previous_month_kwh
                FROM monthly
            )
            SELECT
                metering_point_id,
                month_start,
                total_kwh,
                previous_month_kwh,
                total_kwh - previous_month_kwh AS delta_kwh,
                CASE
                    WHEN previous_month_kwh IS NULL OR previous_month_kwh = 0 THEN NULL
                    ELSE (total_kwh - previous_month_kwh) / previous_month_kwh
                END AS delta_ratio
            FROM enriched;
            """,
        ),
        (
            "reporting.hourly_profile",
            """
            CREATE OR REPLACE VIEW reporting.hourly_profile AS
            SELECT
                metering_point_id,
                reading_hour,
                reading_weekday,
                reading_weekday_label,
                AVG(consumption_value) AS avg_kwh
            FROM reporting.meter_data_clean
            GROUP BY metering_point_id, reading_hour, reading_weekday, reading_weekday_label;
            """,
        ),
        (
            "reporting.missing_data_summary",
            """
            CREATE OR REPLACE VIEW reporting.missing_data_summary AS
            WITH daily_counts AS (
                SELECT
                    metering_point_id,
                    reading_date,
                    COUNT(*) AS actual_readings
                FROM reporting.meter_data_stage
                GROUP BY metering_point_id, reading_date
            )
            SELECT
                metering_point_id,
                reading_date,
                actual_readings,
                24 AS expected_readings,
                24 - actual_readings AS missing_readings
            FROM daily_counts;
            """,
        ),
        (
            "reporting.load_variability",
            """
            CREATE OR REPLACE VIEW reporting.load_variability AS
            SELECT
                metering_point_id,
                reading_date,
                STDDEV_POP(consumption_value) AS hourly_stddev_kwh,
                MAX(consumption_value) AS peak_kwh
            FROM reporting.meter_data_clean
            GROUP BY metering_point_id, reading_date;
            """,
        ),
        (
            "reporting.daily_quality_flags",
            """
            CREATE OR REPLACE VIEW reporting.daily_quality_flags AS
            WITH enriched AS (
                SELECT
                    dc.metering_point_id,
                    dc.reading_date,
                    dc.total_kwh,
                    LAG(dc.total_kwh) OVER (
                        PARTITION BY dc.metering_point_id
                        ORDER BY dc.reading_date
                    ) AS previous_total,
                    LEAD(dc.total_kwh) OVER (
                        PARTITION BY dc.metering_point_id
                        ORDER BY dc.reading_date
                    ) AS next_total
                FROM reporting.daily_consumption AS dc
            )
            SELECT
                metering_point_id,
                reading_date,
                total_kwh,
                CASE
                    WHEN total_kwh = 0
                        AND COALESCE(previous_total, 0) > 0
                        AND COALESCE(next_total, 0) > 0 THEN TRUE
                    ELSE FALSE
                END AS isolated_zero_flag
            FROM enriched;
            """,
        ),
    ]
    return statements


_METADATA_ENRICHED_VIEW = """
CREATE OR REPLACE VIEW reporting.meter_metadata_enriched AS
SELECT
    dc.metering_point_id,
    meta.name,
    meta.type,
    meta.location,
    meta.description,
    meta.extra_metadata,
    dc.reading_date,
    dc.total_kwh,
    dc.avg_kwh,
    dc.min_kwh,
    dc.max_kwh,
    dc.reading_count
FROM reporting.daily_consumption AS dc
LEFT JOIN reporting.meter_metadata AS meta
    ON meta.metering_point_id = dc.metering_point_id;
"""


def build_reporting_layer(
    *,
    data_path: str | Path | None = None,
    analytics_db_path: str | Path | None = None,
    metering_points_override: dict[str, dict[str, Any]] | None = None,
) -> Path:
    """Build or refresh the DuckDB reporting layer defined in the plan.

    Args:
        data_path: Optional override for the Parquet data root.
        analytics_db_path: Optional override for the DuckDB database location.
        metering_points_override: Optional metadata mapping to bypass config loading.

    Returns:
        Path: Location of the analytics DuckDB file.
    """

    config = load_config()
    resolved_data_path = Path(data_path or config["data_storage_path"]).resolve()
    resolved_analytics_path = Path(analytics_db_path or config["analytics_db_path"]).resolve()
    metadata = metering_points_override or config["metering_points"]

    if not resolved_data_path.exists():
        raise FileNotFoundError(f"Data path does not exist: {resolved_data_path}")

    resolved_analytics_path.parent.mkdir(parents=True, exist_ok=True)
    parquet_glob, files = _parquet_glob(resolved_data_path)
    logger.info("Found %d parquet partition(s) for reporting", len(files))

    connection = duckdb.connect(resolved_analytics_path.as_posix())
    try:
        connection.execute("CREATE SCHEMA IF NOT EXISTS reporting")
        for view_name, statement in _view_statements(parquet_glob):
            connection.execute(statement)
            logger.info("Created or replaced view %s", view_name)

        load_metadata_table(connection, metadata)
        connection.execute(_METADATA_ENRICHED_VIEW)
        logger.info("Created metadata-enriched daily view")
    finally:
        connection.close()

    logger.info("Reporting layer refreshed at %s", resolved_analytics_path)
    return resolved_analytics_path
