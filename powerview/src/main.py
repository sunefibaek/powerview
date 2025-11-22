"""Main orchestration module for Eloverblik data extraction pipeline."""

import logging

from powerview.src.api_client import get_meter_data_with_retry
from powerview.src.auth import get_access_token
from powerview.src.config import load_config
from powerview.src.extract import (
    chunk_date_range,
    get_timeframe,
    normalize_api_response,
)
from powerview.src.storage import (
    init_duckdb_state,
    save_to_parquet,
    update_last_ingestion_date,
)


def main() -> None:
    """
    Main extraction workflow orchestrating all components.

    Coordinates configuration loading, API authentication, data extraction,
    normalization, and storage. Handles errors gracefully to ensure
    individual metering point or chunk failures do not stop entire run.
    """
    # Load configuration
    try:
        config = load_config()
    except ValueError as e:
        logger = logging.getLogger(__name__)
        logger.error("Configuration error: %s", e)
        return

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config["log_level"]),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    logger.info("Starting Eloverblik data extraction")

    # Initialize state database
    try:
        init_duckdb_state(config["state_db_path"])
    except Exception as e:
        logger.error("Failed to initialize DuckDB state: %s", e)
        return

    # Get access token (fresh for each run)
    try:
        access_token = get_access_token(config["refresh_token"])
    except Exception as e:
        logger.error("Failed to obtain access token: %s", e)
        return

    logger.info("Tracking %d metering point(s)", len(config["valid_metering_points"]))

    # Extract data for each metering point
    for mp_name, mp_id in config["valid_metering_points"].items():
        logger.info("Processing %s (%s)", mp_name, mp_id)

        try:
            # Get timeframe
            date_from, date_to = get_timeframe(
                mp_id, config["initial_backfill_days"], config["state_db_path"]
            )

            # Chunk large date ranges
            chunks = chunk_date_range(date_from, date_to, chunk_days=90)

            # Track if any chunk succeeded for this metering point
            chunk_success = False

            # Extract data for each chunk
            for chunk_from, chunk_to in chunks:
                logger.info("Fetching data from %s to %s", chunk_from, chunk_to)

                try:
                    # Fetch from API
                    api_response = get_meter_data_with_retry(
                        access_token,
                        chunk_from.isoformat(),
                        chunk_to.isoformat(),
                        metering_point_ids=[mp_id],
                    )

                    # Normalize
                    records = normalize_api_response(api_response, config["metering_point_ids"])

                    # Save to Parquet
                    save_to_parquet(records, config["data_storage_path"])

                    # Mark this metering point as having successful chunk
                    chunk_success = True

                except Exception as e:
                    logger.error(
                        "Failed to fetch/process data for chunk %s to %s: %s",
                        chunk_from,
                        chunk_to,
                        e,
                    )
                    continue

            # Only update state if at least one chunk succeeded for this metering point
            if chunk_success:
                update_last_ingestion_date(mp_id, date_to, config["state_db_path"])
                logger.info("Completed processing for %s", mp_name)
            else:
                logger.warning(
                    "No chunks succeeded for %s (%s). State not updated. "
                    "Will retry from same date range on next run.",
                    mp_name,
                    mp_id,
                )

        except Exception as e:
            logger.error("Failed to process %s: %s", mp_name, e)
            continue

    logger.info("Extraction workflow completed")


if __name__ == "__main__":
    main()
