#!/usr/bin/env python3
"""CLI utility for constructing the DuckDB reporting layer."""

from __future__ import annotations

import argparse
import logging
from typing import Any

from powerview.src.config import load_metering_points
from powerview.src.reporting import build_reporting_layer


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the reporting-layer builder."""

    parser = argparse.ArgumentParser(description="Create or refresh the analytics DuckDB")
    parser.add_argument(
        "--data-path",
        dest="data_path",
        help="Optional override for the Parquet data directory",
    )
    parser.add_argument(
        "--analytics-db",
        dest="analytics_db_path",
        help="Optional override for the analytics DuckDB path",
    )
    parser.add_argument(
        "--metering-points-file",
        dest="metering_points_file",
        help="Explicit path to a metering_points.yml file",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point for the CLI utility."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    args = _parse_args()
    metadata_override: dict[str, dict[str, Any]] | None = None
    if args.metering_points_file:
        metadata_override = load_metering_points(args.metering_points_file)

    build_reporting_layer(
        data_path=args.data_path,
        analytics_db_path=args.analytics_db_path,
        metering_points_override=metadata_override,
    )


if __name__ == "__main__":
    main()
