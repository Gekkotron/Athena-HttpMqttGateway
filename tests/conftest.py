"""Shared fixtures: an app with two secrets and helpers to speak the wire format."""
import base64
import json
import os
import time

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from server import config
from server.app import create_app

# Wildcard secret: every port, every destination.
FULL_KEY = "11" * 32
# HTTP-only secret, and only towards 10.0.0.0/24.
HTTP_ONLY_KEY = "22" * 32
# MQTT-only secret, any destination.
MQTT_ONLY_KEY = "44" * 32
# MQTT-only secret, only towards 10.0.0.0/24.
MQTT_LAN_KEY = "55" * 32
# Key the server does not know.
UNKNOWN_KEY = "33" * 32


class Wire:
    """Client-side encrypt/decrypt for one hex key."""

    def __init__(self, hex_key: str):
        self.aesgcm = AESGCM(bytes.fromhex(hex_key))

    def encrypt(self, payload: dict) -> bytes:
        nonce = os.urandom(12)
        ct = self.aesgcm.encrypt(nonce, json.dumps(payload).encode(), None)
        return base64.b64encode(nonce + ct)

    def decrypt(self, body: bytes) -> dict:
        raw = base64.b64decode(body)
        return json.loads(self.aesgcm.decrypt(raw[:12], raw[12:], None))


@pytest.fixture
def secret_file(tmp_path):
    path = tmp_path / "secret_key.txt"
    path.write_text(
        f"# test secrets\n{FULL_KEY}:*\n{HTTP_ONLY_KEY}:80@10.0.0.0/24\n"
        f"{MQTT_ONLY_KEY}:1883\n{MQTT_LAN_KEY}:1883@10.0.0.0/24\n",
        encoding="utf-8",
    )
    return str(path)


@pytest.fixture
def client(secret_file, monkeypatch):
    monkeypatch.setattr(config, "SECRET_KEY_FILE", secret_file)
    return create_app().test_client()


@pytest.fixture
def full():
    return Wire(FULL_KEY)


@pytest.fixture
def http_only():
    return Wire(HTTP_ONLY_KEY)


@pytest.fixture
def mqtt_only():
    return Wire(MQTT_ONLY_KEY)


@pytest.fixture
def mqtt_lan():
    return Wire(MQTT_LAN_KEY)


@pytest.fixture
def unknown():
    return Wire(UNKNOWN_KEY)


@pytest.fixture
def now():
    return int(time.time())
