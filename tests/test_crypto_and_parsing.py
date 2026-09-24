import pytest

from server.crypto import CryptoManager, NoKeyMatched
from server.gateway import _extract_target_host
from server.key_manager import Secret
from server.services.mqtt_sse_service import _requested_topics


def _secret(byte: int) -> Secret:
    s = Secret(bytes([byte]) * 32)
    s.add_rule(None, None)
    return s


def test_decrypt_returns_the_matching_secret():
    a, b = _secret(1), _secret(2)
    crypto = CryptoManager([a, b])
    payload, matched = crypto.decrypt(crypto.encrypt({"x": 1}, b))
    assert payload == {"x": 1}
    assert matched is b


def test_decrypt_with_unknown_key_raises():
    crypto = CryptoManager([_secret(1)])
    with pytest.raises(NoKeyMatched):
        crypto.decrypt(CryptoManager([_secret(9)]).encrypt({}, _secret(9)))


def test_requires_at_least_one_secret():
    with pytest.raises(ValueError):
        CryptoManager([])


@pytest.mark.parametrize("payload, host", [
    ({"url": "http://10.0.0.1:8080/x"}, "10.0.0.1"),
    ({"host": "http://example.com"}, "example.com"),
    ({"host": "192.168.1.50:8080"}, "192.168.1.50"),
    ({}, None),
])
def test_extract_target_host(payload, host):
    assert _extract_target_host(payload) == host


@pytest.mark.parametrize("payload, expected", [
    ({"topic": "a"}, ["a"]),
    ({"topics": ["a", "b"]}, ["a", "b"]),
    ({"topic": "c", "topics": ["a", "c"]}, ["c", "a"]),
    ({"topics": ["a", "a"]}, ["a"]),
    ({}, None),
    ({"topic": ""}, None),
    ({"topics": []}, None),
    ({"topics": "a"}, None),
    ({"topics": ["a", ""]}, None),
    ({"topics": ["a", 5]}, None),
    ({"topic": 5}, None),
])
def test_requested_topics(payload, expected):
    assert _requested_topics(payload) == expected
