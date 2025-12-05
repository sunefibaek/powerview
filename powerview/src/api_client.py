"""API client module for Eloverblik API."""

import json
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


def get_meter_data(
    access_token: str, date_from: str, date_to: str, metering_point_ids: list[str] | None = None
) -> dict:
    """
    Retrieve meter data for the specified date range with hourly aggregation.

    Args:
        access_token: The access token from get_access_token().
        date_from: Start date in "YYYY-MM-DD" format.
        date_to: End date in "YYYY-MM-DD" format.
        metering_point_ids: List of metering point IDs to fetch. If None or empty,
                           API will return data for all available metering points.

    Returns:
        Dictionary containing meter data for requested metering points.

    Raises:
        requests.HTTPError: If the request fails.
    """
    url = f"https://api.eloverblik.dk/CustomerApi/api/meterdata/gettimeseries/{date_from}/{date_to}/Hour"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "api-version": "1.0",
    }
    payload = {
        "meteringPoints": {"meteringPoint": metering_point_ids or []},
    }

    logger.info("Requesting meter data from %s to %s with Hour aggregation", date_from, date_to)

    # Debug: Print curl equivalent command
    headers_str = " ".join([f'-H "{k}: {v}"' for k, v in headers.items()])
    payload_str = json.dumps(payload)
    curl_command = f"curl -X 'POST' '{url}' {headers_str} -d '{payload_str}'"
    logger.debug("Equivalent curl command:\n%s", curl_command)

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    return response.json()


def get_meter_data_with_retry(
    access_token: str,
    date_from: str,
    date_to: str,
    metering_point_ids: list[str] | None = None,
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
        metering_point_ids: List of metering point IDs to fetch. If None or empty,
                           API will return data for all available metering points.
        max_retries: Maximum number of retries (default: 3).

    Returns:
        Dictionary containing meter data.

    Raises:
        requests.HTTPError: If retry exhausted or non-retryable error.
    """
    retries = 0
    while retries < max_retries:
        try:
            return get_meter_data(access_token, date_from, date_to, metering_point_ids)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in (429, 503):
                retries += 1
                if retries >= max_retries:
                    logger.error("Max retries exceeded for date range %s to %s", date_from, date_to)
                    raise
                wait_time = 60
                logger.warning(
                    "Rate limited or service unavailable. Waiting %ss before retry %s/%s",
                    wait_time,
                    retries,
                    max_retries,
                )
                time.sleep(wait_time)
            else:
                raise
