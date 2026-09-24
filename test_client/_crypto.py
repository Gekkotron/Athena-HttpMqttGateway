"""Client-side AES-GCM helper shared by the test clients.

Kept independent of ``server.crypto`` on purpose: a client only ever holds a
single hex secret, and must speak the wire format (12-byte nonce +
ciphertext) without relying on the server's multi-secret internals.
"""
import json
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def load_secret_key(path: str) -> str:
    """Return the first usable hex secret from a line-based secret file.

    Accepts `<hex>` (legacy) and `<hex>:<scope>` (multi-secret) lines and
    ignores blank lines and `#` comments.
    """
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            return line.split(":", 1)[0].strip()
    raise ValueError(f"No secret found in {path}")


class ClientCrypto:
    """Encrypts requests and decrypts responses under one hex secret."""

    def __init__(self, secret_key_hex: str):
        self.aesgcm = AESGCM(bytes.fromhex(secret_key_hex))

    def encrypt(self, data: dict) -> bytes:
        nonce = os.urandom(12)
        return nonce + self.aesgcm.encrypt(nonce, json.dumps(data).encode(), None)

    def decrypt(self, data: bytes) -> dict:
        return json.loads(self.aesgcm.decrypt(data[:12], data[12:], None))
