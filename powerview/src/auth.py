"""Authentication module for Eloverblik API."""

import logging

import requests

logger = logging.getLogger(__name__)


def get_access_token(refresh_token: str) -> str:
    """
    Exchange refresh token for an access token.

    The Eloverblik API uses a refresh token to obtain a temporary access token
    valid for 24 hours. A fresh token should be obtained once per extraction run.

    Args:
        refresh_token: The refresh token from environment variables.

    Returns:
        The access token as a string.

    Raises:
        requests.HTTPError: If the token exchange fails.
    """
    url = "https://api.eloverblik.dk/CustomerApi/api/token"
    headers = {"Authorization": f"Bearer {refresh_token}"}

    logger.info("Requesting access token from Eloverblik API")
    response = requests.post(url, headers=headers)
    response.raise_for_status()

    token = response.json()["result"]
    logger.info("Successfully obtained access token")
    return token
