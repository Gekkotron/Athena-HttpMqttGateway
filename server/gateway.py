"""Gateway route handler for encrypted HTTP requests."""
import base64
import json
import time
from flask import request, Response

from .crypto import CryptoManager, NoKeyMatched
from .key_manager import Secret
from .services.http_service import HttpService
from . import config


class GatewayHandler:
    """Handles /gateway requests: decrypt, port-check, forward, encrypt."""

    def __init__(self, crypto_manager: CryptoManager):
        self.crypto = crypto_manager
        self.http_service = HttpService(crypto_manager)

    def handle_request(self):
        """Handle an encrypted gateway request for HTTP forwarding."""
        try:
            encrypted_request = base64.b64decode(request.data)
        except Exception:
            return Response("", status=400)

        try:
            payload, secret = self.crypto.decrypt(encrypted_request)
        except NoKeyMatched:
            # No configured secret matched — no way to encrypt a reply the
            # caller could read, so refuse in plaintext.
            return Response("", status=401)
        except Exception:
            return Response("", status=400)

        if not secret.allows(config.HTTP_PORT):
            return self._encrypted_error_response(
                secret, 403, "port not allowed for this secret"
            )

        if not self._validate_timestamp(payload.get("timestamp")):
            return self._encrypted_error_response(secret, 403, "Request expired")

        try:
            response = self.http_service.handle_request(payload, secret)
        except Exception as e:
            return self._encrypted_error_response(secret, 500, str(e))

        encrypted_response = base64.b64encode(response)
        return Response(encrypted_response, mimetype="application/octet-stream")

    def _validate_timestamp(self, timestamp) -> bool:
        if timestamp is None:
            return False
        return abs(time.time() - timestamp) <= config.MAX_AGE_SECONDS

    def _encrypted_error_response(self, secret: Secret, status: int, message: str) -> Response:
        error_payload = {
            "status": status,
            "body": json.dumps({"error": message}),
            "timestamp": int(time.time()),
        }
        encrypted = base64.b64encode(self.crypto.encrypt(error_payload, secret))
        return Response(encrypted, mimetype="application/octet-stream")
