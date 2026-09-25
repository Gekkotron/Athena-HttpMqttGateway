"""MQTT SSE subscription service handler."""
import base64
import json
import time
import queue
import threading
from typing import Generator
import paho.mqtt.client as mqtt

from ..crypto import CryptoManager
from ..key_manager import Secret
from .. import config


def _requested_topics(payload: dict):
    """Return the list of topics to subscribe to, or None if invalid.

    Accepts ``topics`` (list of strings) and/or the legacy single ``topic``.
    """
    topics = payload.get("topics")
    if topics is None:
        topics = []
    elif not isinstance(topics, list):
        return None
    topic = payload.get("topic")
    if topic:
        topics = [topic] + topics
    if not topics or not all(isinstance(t, str) and t for t in topics):
        return None
    return list(dict.fromkeys(topics))  # dedupe, keep order


class MQTTSSEService:
    """Handles MQTT subscription via Server-Sent Events."""

    def __init__(self, crypto_manager: CryptoManager):
        """
        Initialize MQTT SSE service handler.

        Args:
            crypto_manager: CryptoManager instance for encryption operations
        """
        self.crypto = crypto_manager

    def subscribe_stream(self, payload: dict, secret: Secret) -> Generator[str, None, None]:
        """
        Subscribe to one or more MQTT topics and stream messages via SSE.

        Args:
            payload: Decrypted request payload with topic(s) and broker details

        Yields:
            SSE formatted messages with encrypted MQTT data
        """
        # Message queue for thread-safe communication
        message_queue = queue.Queue()

        # Extract MQTT parameters
        broker_host = payload.get("broker_host", config.MQTT_BROKER_HOST)
        broker_port = payload.get("broker_port", config.MQTT_BROKER_PORT)
        topics = _requested_topics(payload)
        username = payload.get("username")
        password = payload.get("password")
        qos = payload.get("qos", 0)

        if topics is None:
            yield self._format_sse_error(
                "Missing or invalid field: topic / topics (non-empty strings)", secret
            )
            return

        # Create MQTT client
        client = mqtt.Client()

        # Set up callbacks
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                client.subscribe([(t, qos) for t in topics])
                message_queue.put({
                    "type": "connected",
                    "topic": topics[0],
                    "topics": topics,
                    "message": f"Successfully connected and subscribed to {', '.join(topics)}"
                })
            else:
                message_queue.put({
                    "type": "error",
                    "message": f"Connection failed with code {rc}",
                    "code": rc,
                })

        def on_message(client, userdata, msg):
            try:
                # Parsed payload: JSON if possible, else text; None when the
                # bytes are not UTF-8. The raw bytes always travel alongside
                # (payload_b64) so clients can keep exact numbers or binary data.
                try:
                    payload_str = msg.payload.decode("utf-8")
                except UnicodeDecodeError:
                    payload_data = None
                else:
                    try:
                        payload_data = json.loads(payload_str)
                    except json.JSONDecodeError:
                        payload_data = payload_str

                message_queue.put({
                    "type": "message",
                    "topic": msg.topic,
                    "payload": payload_data,
                    "payload_b64": base64.b64encode(msg.payload).decode("ascii"),
                    "qos": msg.qos,
                    "retain": msg.retain,
                    "timestamp": int(time.time())
                })
            except Exception as e:
                message_queue.put({
                    "type": "error",
                    "message": f"Error processing message: {str(e)}"
                })

        def on_disconnect(client, userdata, rc):
            message_queue.put({
                "type": "disconnected",
                "message": f"Disconnected from broker (code {rc})"
            })

        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect

        # Set credentials if provided
        if username and password:
            client.username_pw_set(username, password)

        # Connect to broker in a separate thread
        try:
            client.connect(broker_host, broker_port, 60)
            client.loop_start()

            # Send keepalive and stream messages
            last_keepalive = time.time()
            keepalive_interval = 15  # seconds

            while True:
                try:
                    # Get message from queue with timeout
                    msg = message_queue.get(timeout=1)

                    # Encrypt the message
                    encrypted_msg = self._encrypt_message(msg, secret)

                    # Format as SSE
                    yield f"data: {encrypted_msg}\n\n"

                    # If error or disconnect, stop streaming
                    if msg.get("type") in ["error", "disconnected"]:
                        break

                except queue.Empty:
                    # Send keepalive comment to prevent timeout
                    current_time = time.time()
                    if current_time - last_keepalive >= keepalive_interval:
                        yield ": keepalive\n\n"
                        last_keepalive = current_time
                    continue

        except Exception as e:
            yield self._format_sse_error(f"MQTT connection error: {str(e)}", secret)
        finally:
            # Clean up
            client.loop_stop()
            client.disconnect()

    def _encrypt_message(self, message: dict, secret: Secret) -> str:
        """Encrypt ``message`` under the caller's matched secret."""
        encrypted_data = self.crypto.encrypt(message, secret)
        return base64.b64encode(encrypted_data).decode("utf-8")

    def _format_sse_error(self, error_message: str, secret: Secret) -> str:
        """Format an SSE error frame encrypted under the caller's secret."""
        error_data = {
            "type": "error",
            "message": error_message,
            "timestamp": int(time.time())
        }
        encrypted_error = self._encrypt_message(error_data, secret)
        return f"data: {encrypted_error}\n\n"
