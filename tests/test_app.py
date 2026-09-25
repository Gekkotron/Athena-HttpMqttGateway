"""Endpoint tests. MQTT and outbound HTTP are stubbed; nothing leaves the process."""
import itertools
import json
from types import SimpleNamespace
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


# --- error branches shared by the three encrypted endpoints ----------------

ENDPOINTS = ["/gateway", "/mqtt/publish", "/mqtt/subscribe"]


@pytest.mark.parametrize("path", ENDPOINTS)
def test_bad_base64_is_400(client, path):
    assert client.post(path, data=b"a").status_code == 400  # bad padding


@pytest.mark.parametrize("path", ENDPOINTS)
def test_truncated_ciphertext_is_400(client, path):
    assert client.post(path, data=b"AAAA").status_code == 400  # < 12-byte nonce


def test_publish_port_scope(client, http_only, now):
    r = client.post("/mqtt/publish", data=http_only.encrypt({"topic": "t", "message": "m", "timestamp": now}))
    assert _error(r, http_only) == "port not allowed for this secret"


def test_gateway_port_scope(client, mqtt_only, now):
    r = client.post("/gateway", data=mqtt_only.encrypt({"url": "http://10.0.0.1/", "timestamp": now}))
    assert _error(r, mqtt_only) == "port not allowed for this secret"


@pytest.mark.parametrize("path", ["/mqtt/publish", "/mqtt/subscribe"])
def test_mqtt_destination_scope(client, mqtt_lan, now, fake_mqtt, path):
    body = mqtt_lan.encrypt({"topic": "t", "message": "m", "broker_host": "192.168.1.9", "timestamp": now})
    assert _error(client.post(path, data=body), mqtt_lan) == "destination not allowed for this secret"


def test_publish_service_crash_is_encrypted_500(client, full, now):
    with mock.patch(
        "server.services.mqtt_service.MQTTService.handle_request", side_effect=RuntimeError("boom")
    ):
        r = client.post("/mqtt/publish", data=full.encrypt({"topic": "t", "message": "m", "timestamp": now}))
    assert full.decrypt(r.data)["status"] == 500
    assert _error(r, full) == "boom"


def test_gateway_service_crash_is_encrypted_500(client, full, now):
    with mock.patch(
        "server.services.http_service.HttpService.handle_request", side_effect=RuntimeError("boom")
    ):
        r = client.post("/gateway", data=full.encrypt({"url": "http://10.0.0.1/", "timestamp": now}))
    assert full.decrypt(r.data)["status"] == 500
    assert _error(r, full) == "boom"


# --- SSE stream behaviour --------------------------------------------------

def _stream(client, wire, now, **payload):
    r = client.post("/mqtt/subscribe", data=wire.encrypt({"topic": "t", "timestamp": now, **payload}))
    return _sse_frames(r, wire)


def test_sse_forwards_messages_json_and_text(client, full, now, monkeypatch):
    class Chatty(FakeMQTTClient):
        def loop_start(self):
            self.on_connect(self, None, None, 0)
            for body in (b'{"t": 21.5}', b"plain", b"\xff\xfe"):
                self.on_message(self, None, SimpleNamespace(topic="t", payload=body, qos=0, retain=False))

    monkeypatch.setattr(mqtt_sse_service.mqtt, "Client", Chatty)
    frames = _stream(client, full, now)
    assert [f["type"] for f in frames] == ["connected", "message", "message", "error"]
    assert frames[1]["payload"] == {"t": 21.5}
    assert frames[2]["payload"] == "plain"


def test_sse_broker_refuses_connection(client, full, now, monkeypatch):
    class Refused(FakeMQTTClient):
        def loop_start(self):
            self.on_connect(self, None, None, 5)

    monkeypatch.setattr(mqtt_sse_service.mqtt, "Client", Refused)
    [frame] = _stream(client, full, now)
    assert frame == {"type": "error", "message": "Connection failed with code 5"}


def test_sse_connect_exception_and_credentials(client, full, now, monkeypatch):
    calls = []

    class Unreachable(FakeMQTTClient):
        def username_pw_set(self, *args):
            calls.append(args)

        def connect(self, *args):
            raise OSError("no route")

    monkeypatch.setattr(mqtt_sse_service.mqtt, "Client", Unreachable)
    [frame] = _stream(client, full, now, username="u", password="p")
    assert frame["type"] == "error" and "no route" in frame["message"]
    assert calls == [("u", "p")]


def test_sse_sends_keepalive_while_idle(client, full, now, monkeypatch):
    # time.time is the global function (also read by ReplayGuard), so start
    # the fake clock at "now"; each call then jumps 20 s past the keepalive.
    clock = itertools.count(now, 20)

    class Quiet(FakeMQTTClient):
        def loop_start(self):
            self.on_connect(self, None, None, 0)

    real_get = mqtt_sse_service.queue.Queue.get
    state = {"n": 0}

    def get(self, timeout=None):
        state["n"] += 1
        if state["n"] == 2:  # after "connected": pretend one idle second
            raise mqtt_sse_service.queue.Empty
        if state["n"] == 3:
            return {"type": "disconnected", "message": "bye"}
        return real_get(self, timeout=timeout)

    monkeypatch.setattr(mqtt_sse_service.mqtt, "Client", Quiet)
    monkeypatch.setattr(mqtt_sse_service.queue.Queue, "get", get)
    monkeypatch.setattr(mqtt_sse_service.time, "time", lambda: next(clock))
    r = client.post("/mqtt/subscribe", data=full.encrypt({"topic": "t", "timestamp": now}))
    assert ": keepalive" in r.get_data(as_text=True)


# --- gateway error tagging -------------------------------------------------


@pytest.mark.parametrize("path, payload", [
    ("/gateway", {"url": "http://10.0.0.1/"}),
    ("/mqtt/publish", {"topic": "t", "message": "m"}),
    ("/mqtt/subscribe", {"topic": "t"}),
])
def test_gateway_errors_are_tagged(client, full, path, payload):
    r = client.post(path, data=full.encrypt({**payload, "timestamp": 0}))  # expired
    assert full.decrypt(r.data)["source"] == "gateway"


def test_upstream_response_is_not_tagged(client, full, now, monkeypatch):
    from server.services import http_service
    resp = mock.Mock(status_code=403, text="denied")
    resp.json.side_effect = ValueError
    monkeypatch.setattr(http_service.requests, "request", mock.Mock(return_value=resp))
    r = client.post("/gateway", data=full.encrypt({"url": "http://10.0.0.1/", "timestamp": now}))
    out = full.decrypt(r.data)
    assert out["status"] == 403 and "source" not in out


def test_health_reports_version(client):
    assert client.get("/health").get_json()["version"] == "1.1.0"
