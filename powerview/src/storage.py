"""Storage module for DuckDB state management and Parquet file I/O."""

import logging
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


# DuckDB State Management Functions


def init_duckdb_state(db_path: str = "./state.duckdb") -> None:
    """
    Initialize the DuckDB state table if it does not exist.

    Creates an ingestion_state table to track the last ingestion date
    for each metering point, enabling incremental extraction.

    Args:
        db_path: Path to the DuckDB database file.

    Raises:
        Exception: If database initialization fails.
    """
    try:
        con = duckdb.connect(db_path)
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_state (
                metering_point_id TEXT PRIMARY KEY,
                last_ingestion_date DATE
            )
        """
        )
        con.close()
        logger.info("Initialized DuckDB state at %s", db_path)
    except Exception as e:
        logger.error("Failed to initialize DuckDB state: %s", e)
        raise


def get_last_ingestion_date(metering_point_id: str, db_path: str = "./state.duckdb") -> date | None:
    """
    Query the last ingestion date for a metering point from DuckDB.

    Returns None if no prior ingestion exists, enabling the extraction
    pipeline to determine whether to do full backfill or incremental.

    Args:
        metering_point_id: The metering point ID to query.
        db_path: Path to the DuckDB database file.

    Returns:
        The last ingestion date as a date object, or None if no prior ingestion.
    """
    try:
        con = duckdb.connect(db_path)
        result = con.execute(
            "SELECT last_ingestion_date FROM ingestion_state WHERE metering_point_id = ?",
            [metering_point_id],
        ).fetchone()
        con.close()

        if result and result[0]:
            return result[0]
        return None
    except Exception as e:
        logger.warning("Failed to query last ingestion date for %s: %s", metering_point_id, e)
        return None


def update_last_ingestion_date(
    metering_point_id: str, date_to: date, db_path: str = "./state.duckdb"
) -> None:
    """
    Update the last ingestion date for a metering point in DuckDB.

    Uses upsert (insert or replace) logic. Retries up to 3 times on failure.

    Args:
        metering_point_id: The metering point ID to update.
        date_to: The new last ingestion date.
        db_path: Path to the DuckDB database file.

    Raises:
        Exception: If update fails after 3 retries.
    """
    retries = 0
    max_retries = 3
    while retries < max_retries:
        try:
            con = duckdb.connect(db_path)
            con.execute(
                """
                INSERT INTO ingestion_state (metering_point_id, last_ingestion_date)
                VALUES (?, ?)
                ON CONFLICT(metering_point_id) DO
                UPDATE SET last_ingestion_date=excluded.last_ingestion_date
                """,
                [metering_point_id, date_to],
            )
            con.close()
            logger.info("Updated last ingestion date for %s to %s", metering_point_id, date_to)
            return
        except Exception as e:
            retries += 1
            if retries >= max_retries:
                logger.error(
                    "Failed to update last ingestion date after %d retries: %s",
                    max_retries,
                    e,
                )
                raise
            logger.warning("DuckDB update failed, retry %d/%d", retries, max_retries)


# Parquet File I/O Functions


def save_to_parquet(records: list[dict], base_path: str = "./data") -> None:
    """
    Save normalized records to partitioned Parquet files.

    Files are partitioned by metering_point_id and consumption date for efficient
    querying. If a file already exists, duplicate timestamps are replaced with
    new data (upsert strategy).

    Partition structure: /data/metering_point=<ID>/date=<YYYY-MM-DD>/consumption_data.parquet
    where date is the consumption date extracted from the timestamp.

    Args:
        records: List of normalized record dicts.
        base_path: Base directory for storing Parquet files.

    Raises:
        Exception: If write operation fails.
    """
    if not records:
        logger.info("No records to save")
        return

    df = pd.DataFrame(records)

    # Extract consumption date from timestamp for partitioning
    df["consumption_date"] = df["timestamp"].dt.date

    # Group by metering point and consumption date
    for (mp_id, consumption_date), group_df in df.groupby(
        ["metering_point_id", "consumption_date"]
    ):
        try:
            # Create partitioned directory structure
            partition_path = (
                Path(base_path) / f"metering_point={mp_id}" / f"date={consumption_date}"
            )
            partition_path.mkdir(parents=True, exist_ok=True)

            file_path = partition_path / "consumption_data.parquet"

            # Upsert logic: check if file exists
            if file_path.exists():
                # Load existing data
                existing_df = pd.read_parquet(file_path)
                # Remove rows with timestamps that overlap with new data
                existing_df = existing_df[~existing_df["timestamp"].isin(group_df["timestamp"])]
                # Combine old (without duplicates) + new data and sort
                combined_df = pd.concat([existing_df, group_df], ignore_index=True)
                combined_df = combined_df.sort_values("timestamp")
            else:
                combined_df = group_df.sort_values("timestamp")

            # Drop the temporary consumption_date column before saving
            combined_df = combined_df.drop(columns=["consumption_date"])

            combined_df.to_parquet(file_path, engine="pyarrow", compression="snappy", index=False)

            logger.info("Wrote %d records to %s", len(combined_df), file_path)
        except Exception as e:
            logger.error("Failed to write Parquet file for %s/%s: %s", mp_id, consumption_date, e)
            raise
