"""HTTP service handler for generic HTTP requests."""
import time
import logging
import requests

from ..crypto import CryptoManager
from ..key_manager import Secret

logger = logging.getLogger(__name__)


class HttpService:
    """Handles generic HTTP requests."""

    def __init__(self, crypto_manager: CryptoManager):
        """
        Initialize HTTP service handler.

        Args:
            crypto_manager: CryptoManager instance for encryption operations
        """
        self.crypto = crypto_manager

    def handle_request(self, payload: dict, secret: Secret) -> bytes:
        """
        Forward HTTP request to the specified URL.

        Args:
            payload: Decrypted request payload

        Returns:
            Encrypted response from the HTTP endpoint
        """
        logger.info("Handling HTTP request")

        # Build URL from host and endpoint, or use direct URL
        url = payload.get("url")
        if not url:
            # Try to build from host and endpoint
            host = payload.get("host")
            endpoint = payload.get("endpoint", "/")

            if not host:
                logger.error("Missing required 'url' or 'host' parameter in payload")
                raise ValueError(
                    "Missing required 'url' parameter or 'host' parameter in payload"
                )

            # Ensure host doesn't end with / and endpoint starts with /
            host = host.rstrip("/")
            if not endpoint.startswith("/"):
                endpoint = "/" + endpoint

            url = host + endpoint
            logger.info(f"Built URL from host+endpoint: {url}")
        else:
            logger.info(f"Using provided URL: {url}")

        # Get HTTP method (default to POST)
        method = payload.get("method", "POST").upper()
        logger.info(f"HTTP method: {method}")

        # Get headers (default to JSON content type)
        headers = payload.get("headers", {"Content-Type": "application/json"})

        # Get body/data
        body = payload.get("body")

        # Get timeout (default 30 seconds)
        timeout = payload.get("timeout", 30)

        # Prepare request kwargs
        request_kwargs = {
            "method": method,
            "url": url,
            "headers": headers,
            "timeout": timeout,
            "allow_redirects": True  # Allow redirects by default
        }

        # Add body based on type
        if isinstance(body, dict):
            request_kwargs["json"] = body
        elif isinstance(body, str):
            request_kwargs["data"] = body
        elif body is not None:
            # For other types, try to convert to string
            request_kwargs["data"] = str(body)

        # Make HTTP request
        logger.info("Sending %s request to %s", method, url)
        logger.debug(f"Headers: {headers}")
        logger.debug(f"Body type: {type(body)}")
        try:
            resp = requests.request(**request_kwargs)
            logger.info(f"Received response with status code: {resp.status_code}")
        except Exception as e:
            logger.error(f"Error making HTTP request to {url}: {str(e)}")
            raise

        # Build response payload
        try:
            body = resp.json()
        except Exception:
            # If response is not JSON, return as text
            body = resp.text

        response_payload = {
            "status": resp.status_code,
            "body": body,
            "timestamp": int(time.time())
        }

        logger.info("Request handled successfully, returning encrypted response")
        return self.crypto.encrypt(response_payload, secret)
