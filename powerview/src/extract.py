"""Data extraction and normalization module for Eloverblik data."""

import logging
from datetime import UTC, date, datetime, timedelta

logger = logging.getLogger(__name__)


def get_timeframe(
    metering_point_id: str,
    initial_backfill_days: int = 1095,
    db_path: str = "./state.duckdb",
) -> tuple[date, date]:
    """
    Calculate the date range for data extraction based on DuckDB state.

    Logic:
    - If no prior ingestion exists, backfill from initial_backfill_days ago.
    - If prior ingestion exists, start from 7 days before (for overlap).

    Args:
        metering_point_id: The metering point ID.
        initial_backfill_days: Initial backfill period in days (default: 1095).
        db_path: Path to the DuckDB database file.

    Returns:
        Tuple of (date_from, date_to) as date objects.
    """
    from powerview.src.storage import get_last_ingestion_date

    last_date = get_last_ingestion_date(metering_point_id, db_path)

    if last_date is None:
        date_from = datetime.now(UTC).date() - timedelta(days=initial_backfill_days)
        logger.info(
            "No prior ingestion for %s. Starting backfill from %s",
            metering_point_id,
            date_from,
        )
    else:
        date_from = last_date - timedelta(days=7)
        logger.info(
            "Incremental ingestion for %s. Last ingestion was %s, starting from %s",
            metering_point_id,
            last_date,
            date_from,
        )

    date_to = datetime.now(UTC).date()
    return date_from, date_to


def chunk_date_range(
    date_from: date, date_to: date, chunk_days: int = 90
) -> list[tuple[date, date]]:
    """
    Split a date range into chunks to respect API limits.

    Automatically chunks date ranges into 90-day increments to ensure
    reliability and respect the API maximum of 730 days per request.

    Args:
        date_from: Start date.
        date_to: End date.
        chunk_days: Size of each chunk in days (default: 90).

    Returns:
        List of (chunk_from, chunk_to) date tuples.
    """
    chunks = []
    current = date_from

    while current <= date_to:
        chunk_end = min(current + timedelta(days=chunk_days - 1), date_to)
        chunks.append((current, chunk_end))
        if chunk_end >= date_to:
            break
        current = chunk_end + timedelta(days=1)

    logger.info("Split date range into %d chunk(s)", len(chunks))
    return chunks


def normalize_api_response(api_response: dict, metering_point_ids: dict) -> list[dict]:
    """
    Normalize the complex nested Eloverblik API response into flat records.

    Extracts hourly consumption readings from the deeply nested API structure
    and flattens them to one record per reading. Filters to only requested
    metering points.

    Args:
        api_response: The raw response from get_meter_data_with_retry().
        metering_point_ids: Dictionary of metering_point_id values to track.

    Returns:
        List of normalized record dicts with schema:
        metering_point_id, timestamp, consumption_value, quality, unit,
        ingestion_timestamp, ingestion_date
    """
    normalized_records = []
    mp_id_set = set(metering_point_ids.values())

    for result in api_response.get("result", []):
        if not result.get("success"):
            logger.warning("API result not successful: %s", result.get("errorText"))
            continue

        try:
            market_doc = result.get("MyEnergyData_MarketDocument", {})

            for time_series in market_doc.get("TimeSeries", []):
                # Extract metering point ID
                metering_point_id = (
                    time_series.get("MarketEvaluationPoint", {}).get("mRID", {}).get("name")
                )

                # Skip if not in our tracked set
                if metering_point_id not in mp_id_set:
                    continue

                unit = time_series.get("measurement_Unit", {}).get("name", "kWh")

                # Process each period
                for period in time_series.get("Period", []):
                    period_start_str = period.get("timeInterval", {}).get("start")
                    if not period_start_str:
                        continue

                    # Parse period start timestamp
                    try:
                        period_start = datetime.fromisoformat(
                            period_start_str.replace("Z", "+00:00")
                        )
                    except ValueError as e:
                        logger.warning("Failed to parse period start timestamp: %s", e)
                        continue

                    # Process each point (hourly readings)
                    for point in period.get("Point", []):
                        try:
                            position = int(point.get("position", 0))
                            value = float(point.get("out_Quantity.quantity", 0))
                            quality = point.get("out_Quantity.quality", "")

                            # Calculate timestamp: start + (position - 1) hours
                            timestamp = period_start + timedelta(hours=position - 1)

                            normalized_records.append(
                                {
                                    "metering_point_id": metering_point_id,
                                    "timestamp": timestamp,
                                    "consumption_value": value,
                                    "quality": quality,
                                    "unit": unit,
                                    "ingestion_timestamp": datetime.now(UTC),
                                    "ingestion_date": datetime.now(UTC).date(),
                                }
                            )
                        except (ValueError, TypeError) as e:
                            logger.warning("Failed to parse point data: %s", e)
                            continue
        except Exception as e:
            logger.error("Error processing API result: %s", e)
            continue

    logger.info("Normalized %d records from API response", len(normalized_records))
    return normalized_records
