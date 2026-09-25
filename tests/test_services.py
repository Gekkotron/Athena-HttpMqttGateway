"""HttpService / MQTTService: outbound calls stubbed, responses decrypted and checked."""
import json
from types import SimpleNamespace
from unittest import mock

import pytest
import requests

from server.crypto import CryptoManager
from server.key_manager import Secret
from server.services import http_service, mqtt_service
from server.services.http_service import HttpService
from server.services.mqtt_service import MQTTService


@pytest.fixture
def secret():
    s = Secret(b"\x07" * 32)
    s.add_rule(None, None)
    return s


@pytest.fixture
def crypto(secret):
    return CryptoManager([secret])


def _open(crypto, blob):
    payload, _ = crypto.decrypt(blob)
    return payload


# --- HttpService -----------------------------------------------------------

def _response(status=200, json_body=None, text=""):
    resp = mock.Mock(status_code=status, text=text)
    if json_body is None:
        resp.json.side_effect = ValueError("not json")
    else:
        resp.json.return_value = json_body
    return resp


@pytest.fixture
def fake_request(monkeypatch):
    m = mock.Mock(return_value=_response(json_body={"ok": True}))
    monkeypatch.setattr(http_service.requests, "request", m)
    return m


def test_http_uses_url_and_defaults(crypto, secret, fake_request):
    out = _open(crypto, HttpService(crypto).handle_request({"url": "http://h/x"}, secret))

    assert out["status"] == 200 and out["body"] == {"ok": True}
    kwargs = fake_request.call_args.kwargs
    assert kwargs["method"] == "POST"
    assert kwargs["url"] == "http://h/x"
    assert kwargs["headers"] == {"Content-Type": "application/json"}
    assert kwargs["timeout"] == 30
    assert "json" not in kwargs and "data" not in kwargs


@pytest.mark.parametrize("host, endpoint, url", [
    ("http://h/", "api", "http://h/api"),
    ("http://h", "/api", "http://h/api"),
    ("http://h", None, "http://h/"),
])
def test_http_builds_url_from_host_and_endpoint(crypto, secret, fake_request, host, endpoint, url):
    payload = {"host": host, "method": "get"}
    if endpoint is not None:
        payload["endpoint"] = endpoint
    HttpService(crypto).handle_request(payload, secret)
    assert fake_request.call_args.kwargs["url"] == url
    assert fake_request.call_args.kwargs["method"] == "GET"


@pytest.mark.parametrize("body, key, sent", [
    ({"a": 1}, "json", {"a": 1}),
    ("raw", "data", "raw"),
    (42, "data", "42"),
])
def test_http_body_encoding(crypto, secret, fake_request, body, key, sent):
    HttpService(crypto).handle_request({"url": "http://h", "body": body}, secret)
    assert fake_request.call_args.kwargs[key] == sent


def test_http_passes_headers_and_timeout(crypto, secret, fake_request):
    HttpService(crypto).handle_request(
        {"url": "http://h", "headers": {"X": "1"}, "timeout": 5}, secret
    )
    assert fake_request.call_args.kwargs["headers"] == {"X": "1"}
    assert fake_request.call_args.kwargs["timeout"] == 5


def test_http_non_json_response_returned_as_text(crypto, secret, fake_request):
    fake_request.return_value = _response(status=404, text="nope")
    out = _open(crypto, HttpService(crypto).handle_request({"url": "http://h"}, secret))
    assert out["status"] == 404 and out["body"] == "nope"


def test_http_missing_target_raises(crypto, secret, fake_request):
    with pytest.raises(ValueError):
        HttpService(crypto).handle_request({}, secret)
    fake_request.assert_not_called()


def test_http_transport_error_propagates(crypto, secret, fake_request):
    fake_request.side_effect = requests.ConnectionError("down")
    with pytest.raises(requests.ConnectionError):
        HttpService(crypto).handle_request({"url": "http://h"}, secret)


# --- MQTTService -----------------------------------------------------------

@pytest.fixture
def fake_mqtt(monkeypatch):
    client = mock.Mock()
    client.publish.return_value = SimpleNamespace(rc=0)
    monkeypatch.setattr(mqtt_service.mqtt, "Client", mock.Mock(return_value=client))
    return client


def test_mqtt_publish_success(crypto, secret, fake_mqtt):
    payload = {
        "topic": "t", "message": "m", "qos": 1, "retain": True,
        "broker_host": "10.0.0.9", "broker_port": 1884,
        "username": "u", "password": "p",
    }
    out = _open(crypto, MQTTService(crypto).handle_request(payload, secret))

    assert out["status"] == 200
    assert json.loads(out["body"]) == {
        "success": True, "topic": "t", "message": "Published successfully",
    }
    fake_mqtt.username_pw_set.assert_called_once_with("u", "p")
    fake_mqtt.connect.assert_called_once_with("10.0.0.9", 1884, 60)
    fake_mqtt.publish.assert_called_once_with("t", "m", qos=1, retain=True)
    fake_mqtt.disconnect.assert_called_once()


def test_mqtt_publish_without_credentials(crypto, secret, fake_mqtt):
    MQTTService(crypto).handle_request({"topic": "t", "message": "m"}, secret)
    fake_mqtt.username_pw_set.assert_not_called()


def test_mqtt_publish_broker_rejects(crypto, secret, fake_mqtt):
    fake_mqtt.publish.return_value = SimpleNamespace(rc=4)
    out = _open(crypto, MQTTService(crypto).handle_request({"topic": "t", "message": "m"}, secret))
    assert out["status"] == 500
    assert json.loads(out["body"])["message"] == "Failed with code 4"


def test_mqtt_publish_missing_field(crypto, secret, fake_mqtt):
    out = _open(crypto, MQTTService(crypto).handle_request({"topic": "t"}, secret))
    assert out["status"] == 400
    assert "message" in json.loads(out["body"])["error"]


def test_mqtt_publish_connection_error(crypto, secret, fake_mqtt):
    fake_mqtt.connect.side_effect = OSError("refused")
    out = _open(crypto, MQTTService(crypto).handle_request({"topic": "t", "message": "m"}, secret))
    assert out["status"] == 500
    assert "refused" in json.loads(out["body"])["error"]
