"""HTTP service handler for generic HTTP requests."""
import base64
import time
import logging
import requests

from .. import config
from ..crypto import CryptoManager
from ..key_manager import Secret

logger = logging.getLogger(__name__)


def _response_headers(resp) -> dict:
    """Upstream response headers, names lower-cased; every Set-Cookie line kept, joined with \\n."""
    headers = {str(k).lower(): str(v) for k, v in dict(resp.headers or {}).items()}
    raw_headers = getattr(getattr(resp, "raw", None), "headers", None)
    getlist = getattr(raw_headers, "getlist", None)
    if callable(getlist):
        cookies = getlist("Set-Cookie")
        if isinstance(cookies, list) and cookies:  # a bare test Mock returns a Mock, not a list
            headers["set-cookie"] = "\n".join(str(c) for c in cookies)
    return headers


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

        raw = payload.get("raw") is True

        # Prepare request kwargs
        request_kwargs = {
            "method": method,
            "url": url,
            "headers": headers,
            "timeout": timeout,
            "allow_redirects": True  # Allow redirects by default
        }
        if raw:
            request_kwargs["stream"] = True

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

        if raw:
            return self._raw_response(resp, secret)

        # Build response payload
        try:
            body = resp.json()
        except Exception:
            # If response is not JSON, return as text
            body = resp.text

        response_payload = {
            "status": resp.status_code,
            "body": body,
            "headers": _response_headers(resp),
            "timestamp": int(time.time())
        }

        logger.info("Request handled successfully, returning encrypted response")
        return self.crypto.encrypt(response_payload, secret)

    def _raw_response(self, resp, secret: Secret) -> bytes:
        """Encrypt the upstream body as exact bytes (base64), enforcing the size cap."""
        limit = config.HTTP_MAX_RESPONSE_BYTES
        try:
            declared = resp.headers.get("Content-Length")
            if declared is not None and declared.isdigit() and int(declared) > limit:
                raise ValueError("response too large")

            chunks = []
            total = 0
            for chunk in resp.iter_content(chunk_size=64 * 1024):
                total += len(chunk)
                if total > limit:
                    raise ValueError("response too large")
                chunks.append(chunk)
            data = b"".join(chunks)
            content_type = resp.headers.get("Content-Type")
        finally:
            resp.close()

        response_payload = {
            "status": resp.status_code,
            "body": None,
            "body_b64": base64.b64encode(data).decode("ascii"),
            "content_type": content_type,
            "timestamp": int(time.time())
        }
        logger.info("Raw request handled successfully (%d bytes)", len(data))
        return self.crypto.encrypt(response_payload, secret)
