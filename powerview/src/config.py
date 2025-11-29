"""Configuration management module for Eloverblik data extraction."""

import logging
import os

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def load_config() -> dict:
    """
    Load and validate configuration from .env file and environment variables.

    Loads configuration including:
    - Required: ELOVERBLIK_REFRESH_TOKEN
    - Optional: Up to 6 metering point IDs
    - Optional with defaults: data storage, state DB, logging, backfill days

    Returns:
        Dictionary containing all validated configuration parameters.

    Raises:
        ValueError: If refresh token is missing or no metering points configured.
    """
    load_dotenv()

    config = {
        "refresh_token": os.getenv("ELOVERBLIK_REFRESH_TOKEN"),
        "metering_point_ids": {
            "delivery_to_grid": os.getenv("DELIVERY_TO_GRID_ID"),
            "electric_heating": os.getenv("ELECTRIC_HEATING_ID"),
            "consumed_from_grid": os.getenv("CONSUMED_FROM_GRID_ID"),
            "net_consumption": os.getenv("NET_CONSUMPTION_ID"),
            "hjem_meter": os.getenv("HJEM_METER_ID"),
            "sommerhus_meter": os.getenv("SOMMERHUS_METER_ID"),
        },
        "data_storage_path": os.getenv("DATA_STORAGE_PATH", "./data"),
        "state_db_path": os.getenv("STATE_DB_PATH", "./state.duckdb"),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "initial_backfill_days": int(os.getenv("INITIAL_BACKFILL_DAYS", "1095")),
    }

    # Validate required configuration
    if not config["refresh_token"]:
        logger.error("ELOVERBLIK_REFRESH_TOKEN is required")
        raise ValueError("ELOVERBLIK_REFRESH_TOKEN is required")

    # Filter out None and empty metering point IDs
    config["valid_metering_points"] = {k: v for k, v in config["metering_point_ids"].items() if v}

    if not config["valid_metering_points"]:
        logger.error("At least one metering point ID must be configured")
        raise ValueError("At least one metering point ID must be configured")

    logger.info(
        "Configuration loaded. Tracking %d metering point(s)",
        len(config["valid_metering_points"]),
    )

    return config
