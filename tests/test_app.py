"""Endpoint tests. MQTT and outbound HTTP are stubbed; nothing leaves the process."""
import json
from unittest import mock

import pytest

from server.services import mqtt_sse_service


class FakeMQTTClient:
    """Stands in for paho's Client: connects, then disconnects immediately."""

    subscriptions = []

    def username_pw_set(self, *args):
        pass

    def connect(self, *args):
        pass

    def loop_start(self):
        self.on_connect(self, None, None, 0)
        self.on_disconnect(self, None, 0)

    def loop_stop(self):
        pass

    def disconnect(self):
        pass

    def subscribe(self, topics, qos=0):
        FakeMQTTClient.subscriptions.append(topics)


@pytest.fixture
def fake_mqtt(monkeypatch):
    FakeMQTTClient.subscriptions = []
    monkeypatch.setattr(mqtt_sse_service.mqtt, "Client", FakeMQTTClient)
    return FakeMQTTClient


def _sse_frames(response, wire):
    text = response.get_data(as_text=True)
    return [wire.decrypt(line[6:]) for line in text.splitlines() if line.startswith("data: ")]


def _error(response, wire):
    body = wire.decrypt(response.data)["body"]
    return (json.loads(body) if isinstance(body, str) else body)["error"]


def test_health(client):
    assert client.get("/health").get_json()["status"] == "ok"


# --- /mqtt/subscribe -------------------------------------------------------

def test_subscribe_streams_several_topics(client, full, now, fake_mqtt):
    body = full.encrypt({"topics": ["home/#", "lights/+"], "qos": 1, "timestamp": now})
    r = client.post("/mqtt/subscribe", data=body)

    assert r.status_code == 200
    assert r.mimetype == "text/event-stream"
    frames = _sse_frames(r, full)
    assert frames[0]["type"] == "connected"
    assert frames[0]["topics"] == ["home/#", "lights/+"]
    assert frames[-1]["type"] == "disconnected"
    assert fake_mqtt.subscriptions == [[("home/#", 1), ("lights/+", 1)]]


def test_subscribe_legacy_single_topic(client, full, now, fake_mqtt):
    r = client.post("/mqtt/subscribe", data=full.encrypt({"topic": "a/b", "timestamp": now}))
    assert _sse_frames(r, full)[0]["topic"] == "a/b"


def test_subscribe_without_topic_sends_error_frame(client, full, now, fake_mqtt):
    r = client.post("/mqtt/subscribe", data=full.encrypt({"timestamp": now}))
    [frame] = _sse_frames(r, full)
    assert frame["type"] == "error"


@pytest.mark.parametrize("extra", [{}, {"timestamp": 0}])
def test_subscribe_requires_fresh_timestamp(client, full, fake_mqtt, extra):
    r = client.post("/mqtt/subscribe", data=full.encrypt({"topic": "a", **extra}))
    assert _error(r, full) == "Request expired"


def test_subscribe_replay_rejected(client, full, now, fake_mqtt):
    body = full.encrypt({"topic": "a", "timestamp": now})
    assert client.post("/mqtt/subscribe", data=body).mimetype == "text/event-stream"
    assert _error(client.post("/mqtt/subscribe", data=body), full) == "Request replayed"


def test_subscribe_port_scope(client, http_only, now, fake_mqtt):
    r = client.post("/mqtt/subscribe", data=http_only.encrypt({"topic": "a", "timestamp": now}))
    assert _error(r, http_only) == "port not allowed for this secret"


def test_subscribe_unknown_key_is_401(client, unknown, now):
    r = client.post("/mqtt/subscribe", data=unknown.encrypt({"topic": "a", "timestamp": now}))
    assert r.status_code == 401


# --- /mqtt/publish ---------------------------------------------------------

@pytest.fixture
def fake_publish():
    with mock.patch(
        "server.services.mqtt_service.MQTTService.handle_request", return_value=b"b2s="
    ) as m:
        yield m


def test_publish_forwards_to_service(client, full, now, fake_publish):
    r = client.post("/mqtt/publish", data=full.encrypt({"topic": "t", "message": "m", "timestamp": now}))
    assert r.status_code == 200
    fake_publish.assert_called_once()


def test_publish_replay_rejected(client, full, now, fake_publish):
    body = full.encrypt({"topic": "t", "message": "m", "timestamp": now})
    client.post("/mqtt/publish", data=body)
    assert _error(client.post("/mqtt/publish", data=body), full) == "Request replayed"
    assert fake_publish.call_count == 1


def test_publish_expired(client, full, fake_publish):
    r = client.post("/mqtt/publish", data=full.encrypt({"topic": "t", "message": "m", "timestamp": 0}))
    assert _error(r, full) == "Request expired"
    fake_publish.assert_not_called()


def test_publish_malformed_body_is_400(client):
    assert client.post("/mqtt/publish", data=b"not base64!!").status_code == 400


# --- /gateway --------------------------------------------------------------

@pytest.fixture
def fake_http():
    with mock.patch(
        "server.services.http_service.HttpService.handle_request", return_value=b"b2s="
    ) as m:
        yield m


def test_gateway_forwards_allowed_destination(client, http_only, now, fake_http):
    body = http_only.encrypt({"url": "http://10.0.0.7/x", "method": "GET", "timestamp": now})
    assert client.post("/gateway", data=body).status_code == 200
    fake_http.assert_called_once()


def test_gateway_refuses_destination_outside_scope(client, http_only, now, fake_http):
    body = http_only.encrypt({"url": "http://10.0.1.7/x", "method": "GET", "timestamp": now})
    assert _error(client.post("/gateway", data=body), http_only) == "destination not allowed for this secret"
    fake_http.assert_not_called()


def test_gateway_replay_rejected(client, full, now, fake_http):
    body = full.encrypt({"url": "http://10.0.0.7/x", "method": "GET", "timestamp": now})
    client.post("/gateway", data=body)
    assert _error(client.post("/gateway", data=body), full) == "Request replayed"
    assert fake_http.call_count == 1


def test_gateway_unknown_key_is_401(client, unknown, now):
    body = unknown.encrypt({"url": "http://10.0.0.7/", "timestamp": now})
    assert client.post("/gateway", data=body).status_code == 401
