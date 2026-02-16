"""API client module for Energy Data Service electricity prices."""

from __future__ import annotations

import json
import logging
import time
from datetime import date

import requests

logger = logging.getLogger(__name__)

EDS_DATASET_BASE_URL = "https://api.energidataservice.dk/dataset"
TRANSITION_DATE = date(2025, 10, 1)


def _build_params(
    date_from: str,
    date_to: str,
    columns: list[str],
    price_areas: list[str] | None = None,
) -> dict[str, str | int]:
    """Build common query parameters for Energy Data Service dataset requests."""

    params: dict[str, str | int] = {
        "start": date_from,
        "end": date_to,
        "columns": ",".join(columns),
        "limit": 0,
    }

    if price_areas:
        params["filter"] = json.dumps({"PriceArea": price_areas})

    return params


def get_elspot_prices(date_from: str, date_to: str, price_areas: list[str] | None = None) -> dict:
    """
    Retrieve historical spot prices from Elspotprices dataset (pre-2025-10-01).

    Args:
        date_from: Start date in "YYYY-MM-DD" format.
        date_to: End date in "YYYY-MM-DD" format.
        price_areas: List of price areas (e.g., ["DK1", "DK2"]). If None, fetches all.

    Returns:
        Dictionary containing price data from the API.

    Raises:
        requests.HTTPError: If the request fails.
    """
    url = f"{EDS_DATASET_BASE_URL}/Elspotprices"
    params = _build_params(
        date_from,
        date_to,
        columns=["HourUTC", "PriceArea", "SpotPriceDKK", "SpotPriceEUR"],
        price_areas=price_areas,
    )

    logger.info("Requesting Elspotprices data from %s to %s", date_from, date_to)

    # Debug: Print curl equivalent command
    query_str = "&".join([f"{k}={v}" for k, v in params.items()])
    curl_command = f"curl -X 'GET' '{url}?{query_str}' -H 'accept: application/json'"
    logger.debug("Equivalent curl command:\n%s", curl_command)

    response = requests.get(url, params=params)
    response.raise_for_status()

    return response.json()


def get_dayahead_prices(date_from: str, date_to: str, price_areas: list[str] | None = None) -> dict:
    """
    Retrieve day-ahead prices from DayAheadPrices dataset (from 2025-10-01).

    Args:
        date_from: Start date in "YYYY-MM-DD" format.
        date_to: End date in "YYYY-MM-DD" format.
        price_areas: List of price areas (e.g., ["DK1", "DK2"]). If None, fetches all.

    Returns:
        Dictionary containing price data from the API.

    Raises:
        requests.HTTPError: If the request fails.
    """
    url = f"{EDS_DATASET_BASE_URL}/DayAheadPrices"
    params = _build_params(
        date_from,
        date_to,
        columns=["TimeUTC", "PriceArea", "DayAheadPriceDKK", "DayAheadPriceEUR"],
        price_areas=price_areas,
    )

    logger.info("Requesting DayAheadPrices data from %s to %s", date_from, date_to)

    # Debug: Print curl equivalent command
    query_str = "&".join([f"{k}={v}" for k, v in params.items()])
    curl_command = f"curl -X 'GET' '{url}?{query_str}' -H 'accept: application/json'"
    logger.debug("Equivalent curl command:\n%s", curl_command)

    response = requests.get(url, params=params)
    response.raise_for_status()

    return response.json()


def _resolve_dataset(date_from: str, dataset: str) -> str:
    """Resolve which dataset to use based on explicit or automatic selection."""

    if dataset in {"elspot", "dayahead"}:
        return dataset

    if dataset != "auto":
        raise ValueError("dataset must be one of: auto, elspot, dayahead")

    start_date = date.fromisoformat(date_from[:10])
    return "elspot" if start_date < TRANSITION_DATE else "dayahead"


def get_prices_with_retry(
    date_from: str,
    date_to: str,
    price_areas: list[str] | None = None,
    dataset: str = "auto",
    max_retries: int = 3,
) -> dict:
    """
    Fetch price data with retry logic for rate limiting and service errors.

    Automatically selects dataset based on date range if dataset="auto":
    - Elspotprices for dates before 2025-10-01
    - DayAheadPrices for dates from 2025-10-01 onwards

    Handles HTTP 429 (rate limited) and HTTP 503 (service unavailable) by
    waiting 1 minute and retrying. Other HTTP errors are raised immediately.

    Args:
        date_from: Start date in "YYYY-MM-DD" format.
        date_to: End date in "YYYY-MM-DD" format.
        price_areas: List of price areas. If None, fetches all.
        dataset: Dataset to use ("auto", "elspot", or "dayahead").
        max_retries: Maximum number of retries (default: 3).

    Returns:
        Dictionary containing price data.

    Raises:
        requests.HTTPError: If retry exhausted or non-retryable error.
        ValueError: If dataset has an unsupported value.
    """
    resolved_dataset = _resolve_dataset(date_from, dataset)
    retries = 0

    while retries < max_retries:
        try:
            if resolved_dataset == "elspot":
                return get_elspot_prices(date_from, date_to, price_areas)
            return get_dayahead_prices(date_from, date_to, price_areas)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in (429, 503):
                retries += 1
                if retries >= max_retries:
                    logger.error(
                        "Max retries exceeded for dataset=%s date range %s to %s",
                        resolved_dataset,
                        date_from,
                        date_to,
                    )
                    raise
                wait_time = 60
                logger.warning(
                    "Rate limited or service unavailable for %s. Waiting %ss before retry %s/%s",
                    resolved_dataset,
                    wait_time,
                    retries,
                    max_retries,
                )
                time.sleep(wait_time)
            else:
                raise
