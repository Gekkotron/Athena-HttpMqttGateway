"""Encryption and decryption utilities using AES-GCM with multi-secret support."""
import json
import os
from typing import Tuple

from cryptography.exceptions import InvalidTag

from .key_manager import Secret


class NoKeyMatched(Exception):
    """Raised when no configured secret can decrypt a payload."""


class CryptoManager:
    """Manages AES-GCM encryption across one or more configured secrets."""

    def __init__(self, secrets: list):
        if not secrets:
            raise ValueError("At least one Secret is required.")
        self.secrets = list(secrets)

    def decrypt(self, data: bytes) -> Tuple[dict, Secret]:
        """Trial-decrypt with each secret; return (payload, matched Secret).

        Raises:
            NoKeyMatched: no configured secret authenticated the ciphertext.
        """
        nonce, ciphertext = data[:12], data[12:]
        for secret in self.secrets:
            try:
                plaintext = secret.aesgcm.decrypt(nonce, ciphertext, None)
            except InvalidTag:
                continue
            return json.loads(plaintext), secret
        raise NoKeyMatched("No configured secret could decrypt the payload.")

    def encrypt(self, data: dict, secret: Secret) -> bytes:
        """Encrypt ``data`` under ``secret``. Caller must supply the matched Secret."""
        nonce = os.urandom(12)
        plaintext = json.dumps(data).encode()
        ciphertext = secret.aesgcm.encrypt(nonce, plaintext, None)
        return nonce + ciphertext
