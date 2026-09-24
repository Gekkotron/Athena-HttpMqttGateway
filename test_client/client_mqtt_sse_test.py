"""Interactive client for MQTT SSE subscription.

This is an interactive demonstration client that subscribes to an MQTT topic
and continuously displays incoming messages until manually interrupted (Ctrl+C).

For automated testing, use client_mqtt_sse_automated_test.py instead.
"""
import base64
import json
import time
import sys
import os
import sseclient
import requests

# Add parent directory to path to import crypto module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.config import SECRET_KEY_FILE
from test_client._crypto import ClientCrypto, load_secret_key


def subscribe_to_mqtt_topic(
    server_url: str,
    topic: str,
    broker_host: str = None,
    broker_port: int = None,
    username: str = None,
    password: str = None,
    qos: int = 0
):
    """
    Subscribe to MQTT topic via SSE endpoint.

    Args:
        server_url: Gateway server URL
        topic: MQTT topic to subscribe to
        broker_host: Optional MQTT broker hostname
        broker_port: Optional MQTT broker port
        username: Optional MQTT username
        password: Optional MQTT password
        qos: Quality of Service level (0, 1, or 2)
    """
    # Load secret key
    crypto = ClientCrypto(load_secret_key(SECRET_KEY_FILE))

    # Build request payload
    payload = {
        "topic": topic,
        "qos": qos,
        "timestamp": int(time.time())
    }

    if broker_host:
        payload["broker_host"] = broker_host
    if broker_port:
        payload["broker_port"] = broker_port
    if username:
        payload["username"] = username
    if password:
        payload["password"] = password

    # Encrypt payload
    encrypted_payload = crypto.encrypt(payload)
    encoded_payload = base64.b64encode(encrypted_payload)

    # Make SSE request
    print(f"Subscribing to topic: {topic}")
    print(f"Server URL: {server_url}/mqtt/subscribe")
    print("-" * 60)

    try:
        response = requests.post(
            f"{server_url}/mqtt/subscribe",
            data=encoded_payload,
            headers={"Content-Type": "application/octet-stream"},
            stream=True
        )

        if response.status_code != 200:
            print(f"Error: HTTP {response.status_code}")
            print(response.text)
            return

        # Process SSE events
        client = sseclient.SSEClient(response)
        for event in client.events():
            if event.data:
                try:
                    # Decode and decrypt the message
                    encrypted_data = base64.b64decode(event.data)
                    decrypted_message = crypto.decrypt(encrypted_data)

                    msg_type = decrypted_message.get("type")
                    timestamp = decrypted_message.get("timestamp")

                    if msg_type == "connected":
                        print(f"[{timestamp}] CONNECTED: {decrypted_message.get('message')}")

                    elif msg_type == "message":
                        mqtt_topic = decrypted_message.get("topic")
                        mqtt_payload = decrypted_message.get("payload")
                        mqtt_qos = decrypted_message.get("qos")
                        mqtt_retain = decrypted_message.get("retain")

                        print(f"\n[{timestamp}] MESSAGE RECEIVED:")
                        print(f"  Topic: {mqtt_topic}")
                        print(f"  Payload: {mqtt_payload}")
                        print(f"  QoS: {mqtt_qos}, Retain: {mqtt_retain}")

                    elif msg_type == "error":
                        print(f"[{timestamp}] ERROR: {decrypted_message.get('message')}")
                        break

                    elif msg_type == "disconnected":
                        print(f"[{timestamp}] DISCONNECTED: {decrypted_message.get('message')}")
                        break

                except Exception as e:
                    print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        print("\n\nSubscription terminated by user")
    except Exception as e:
        print(f"Connection error: {e}")


if __name__ == "__main__":
    # Example usage
    SERVER_URL = os.getenv("GATEWAY_URL", "http://localhost:10000")

    # Test with default MQTT broker from config
    # Subscribe to test topic
    subscribe_to_mqtt_topic(
        server_url=SERVER_URL,
        topic="test/topic",
        qos=0
    )

    # Or with custom broker:
    # subscribe_to_mqtt_topic(
    #     server_url=SERVER_URL,
    #     topic="home/sensors/temperature",
    #     broker_host="192.168.1.100",
    #     broker_port=1883,
    #     username="mqtt_user",
    #     password="mqtt_password",
    #     qos=1
    # )
