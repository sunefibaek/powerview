"""API client module for Eloverblik API."""

import logging
import time

import requests

logger = logging.getLogger(__name__)


def get_metering_points(access_token: str) -> dict:
    """
    Retrieve all available metering points for the authenticated user.

    Args:
        access_token: The access token from get_access_token().

    Returns:
        Dictionary containing metering points data from the API.

    Raises:
        requests.HTTPError: If the request fails.
    """
    url = "https://api.eloverblik.dk/CustomerApi/api/meteringpoints/meteringpoints"
    headers = {"Authorization": f"Bearer {access_token}"}

    logger.info("Requesting metering points from Eloverblik API")
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json()


def get_meter_data(access_token: str, date_from: str, date_to: str) -> dict:
    """
    Retrieve meter data for the specified date range.

    Important: The API returns data for ALL metering points available to the
    authenticated user, not just the ones specified. The extraction pipeline
    filters results to extract only the tracked metering points.

    Args:
        access_token: The access token from get_access_token().
        date_from: Start date in "YYYY-MM-DD" format.
        date_to: End date in "YYYY-MM-DD" format.

    Returns:
        Dictionary containing meter data for all available metering points.

    Raises:
        requests.HTTPError: If the request fails.
    """
    url = "https://api.eloverblik.dk/CustomerApi/api/meterdata/gettimeseries"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "meteringPoints": {"meteringPoint": []},
        "dateFrom": date_from,
        "dateTo": date_to,
    }

    logger.info("Requesting meter data from %s to %s", date_from, date_to)
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    return response.json()


def get_meter_data_with_retry(
    access_token: str,
    date_from: str,
    date_to: str,
    max_retries: int = 3,
) -> dict:
    """
    Fetch meter data with retry logic for rate limiting and service errors.

    Handles HTTP 429 (rate limited) and HTTP 503 (service unavailable) by
    waiting 1 minute and retrying. Other HTTP errors are raised immediately.

    Args:
        access_token: The access token.
        date_from: Start date in "YYYY-MM-DD" format.
        date_to: End date in "YYYY-MM-DD" format.
        max_retries: Maximum number of retries (default: 3).

    Returns:
        Dictionary containing meter data.

    Raises:
        requests.HTTPError: If retry exhausted or non-retryable error.
    """
    retries = 0
    while retries < max_retries:
        try:
            return get_meter_data(access_token, date_from, date_to)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in (429, 503):
                retries += 1
                if retries >= max_retries:
                    logger.error(f"Max retries exceeded for date range {date_from} to {date_to}")
                    raise
                wait_time = 60
                logger.warning(
                    f"Rate limited or service unavailable. "
                    f"Waiting {wait_time}s before retry {retries}/{max_retries}"
                )
                time.sleep(wait_time)
            else:
                raise
