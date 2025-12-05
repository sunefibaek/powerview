"""Configuration management module for Eloverblik data extraction."""

import logging
import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

DEFAULT_METERING_POINTS_FILE = "metering_points.yml"
try:
    PACKAGE_ROOT = Path(__file__).resolve().parents[2]
except IndexError:  # pragma: no cover - fallback for unusual packaging layouts
    PACKAGE_ROOT = Path.cwd()


def _normalize_path(path_value: str | os.PathLike[str]) -> Path:
    """Return an absolute path for the provided string.

    Args:
        path_value: Relative or absolute path value provided via config/env.

    Returns:
        Path: Absolute version of ``path_value``.
    """

    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (Path.cwd() / path).resolve()


def _resolve_metering_points_path(file_path: str | None = None) -> Path:
    """Determine the metering-points file location with sensible fallbacks.

    Args:
        file_path: Optional override path supplied by the caller.

    Returns:
        Path: Resolved path that should be used to load metering points.
    """

    env_path = file_path or os.getenv("METERING_POINTS_FILE")
    if env_path:
        return _normalize_path(env_path)

    default_candidates = [
        (Path.cwd() / DEFAULT_METERING_POINTS_FILE).resolve(),
        (PACKAGE_ROOT / DEFAULT_METERING_POINTS_FILE).resolve(),
    ]

    for candidate in default_candidates:
        if candidate.exists():
            return candidate

    return default_candidates[0]


def load_metering_points(file_path: str | None = None) -> dict[str, dict[str, Any]]:
    """Load metering point metadata from YAML configuration.

    Args:
        file_path: Optional explicit path to a YAML file.

    Returns:
        dict[str, dict[str, Any]]: Mapping of metering point IDs to metadata.

    Raises:
        ValueError: If the file is missing, malformed, or empty.
    """

    resolved_path = _resolve_metering_points_path(file_path)
    if not resolved_path.exists():
        raise ValueError(f"Metering points file not found: {resolved_path}")

    with resolved_path.open("r", encoding="utf-8") as handle:
        try:
            raw_data = yaml.safe_load(handle) or {}
        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse YAML file {resolved_path}: {e}") from e

    if "metering_points" not in raw_data:
        raise ValueError("Metering points file is missing the required 'metering_points' key")
    metering_points_data = raw_data["metering_points"]
    if not isinstance(metering_points_data, dict):
        raise ValueError("The 'metering_points' key must map to a dictionary (mapping) of metering points")

    normalized: dict[str, dict[str, Any]] = {}
    for mp_id, metadata in metering_points_data.items():
        mp_id_str = str(mp_id).strip()
        if not mp_id_str:
            continue

        if metadata is None:
            metadata = {}
        elif not isinstance(metadata, dict):
            raise ValueError(
                f"Metadata for metering point '{mp_id_str}' must be a mapping (dict), got {type(metadata).__name__}"
            )

        name = metadata.get("name") or mp_id_str
        normalized[mp_id_str] = {**metadata, "id": mp_id_str, "name": name}

    if not normalized:
        raise ValueError("At least one metering point ID must be configured")

    logger.info(
        "Loaded %d metering point definition(s) from %s",
        len(normalized),
        resolved_path,
    )
    return normalized


def load_config() -> dict:
    """Load and validate configuration from .env and ``metering_points.yml``.

    Returns:
        dict: Fully validated configuration suitable for pipeline execution.

    Raises:
        ValueError: If the refresh token or metering-point definitions are missing.
    """

    load_dotenv()
    metering_points = load_metering_points()
    metering_point_ids = {mp_data["name"]: mp_id for mp_id, mp_data in metering_points.items()}

    config = {
        "refresh_token": os.getenv("ELOVERBLIK_REFRESH_TOKEN"),
        "metering_points": metering_points,
        "metering_point_ids": dict(metering_point_ids),
        "data_storage_path": os.getenv("DATA_STORAGE_PATH", "./data"),
        # TODO add analytics database path
        "state_db_path": os.getenv("STATE_DB_PATH", "./state.duckdb"),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "initial_backfill_days": int(os.getenv("INITIAL_BACKFILL_DAYS", "1095")),
    }

    if not config["refresh_token"]:
        logger.error("ELOVERBLIK_REFRESH_TOKEN is required")
        raise ValueError("ELOVERBLIK_REFRESH_TOKEN is required")

    config["valid_metering_points"] = config["metering_point_ids"]

    if not config["valid_metering_points"]:
        logger.error("At least one metering point ID must be configured")
        raise ValueError("At least one metering point ID must be configured")

    logger.info(
        "Configuration loaded. Tracking %d metering point(s)",
        len(config["valid_metering_points"]),
    )

    return config
