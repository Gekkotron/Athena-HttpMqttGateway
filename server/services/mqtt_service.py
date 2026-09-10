"""MQTT service handler."""
import json
import time
import paho.mqtt.client as mqtt

from ..crypto import CryptoManager
from ..key_manager import Secret
from .. import config


class MQTTService:
    """Handles MQTT publish requests."""

    def __init__(self, crypto_manager: CryptoManager):
        """
        Initialize MQTT service handler.

        Args:
            crypto_manager: CryptoManager instance for encryption operations
        """
        self.crypto = crypto_manager

    def handle_request(self, payload: dict, secret: Secret) -> bytes:
        """
        Publish message to MQTT broker.
        
        Args:
            payload: Decrypted request payload with topic and message
            
        Returns:
            Encrypted response confirming publication
        """
        try:
            # Extract MQTT parameters
            broker_host = payload.get("broker_host", config.MQTT_BROKER_HOST)
            broker_port = payload.get("broker_port", config.MQTT_BROKER_PORT)
            topic = payload["topic"]
            message = payload["message"]
            username = payload.get("username")
            password = payload.get("password")
            qos = payload.get("qos", 0)
            retain = payload.get("retain", False)
            
            # Create MQTT client
            client = mqtt.Client()
            
            # Set credentials if provided
            if username and password:
                client.username_pw_set(username, password)
            
            # Connect and publish
            client.connect(broker_host, broker_port, 60)
            result = client.publish(topic, message, qos=qos, retain=retain)
            client.disconnect()
            
            # Build response payload
            response_payload = {
                "status": 200 if result.rc == 0 else 500,
                "body": json.dumps({
                    "success": result.rc == 0,
                    "topic": topic,
                    "message": (
                        "Published successfully" 
                        if result.rc == 0 
                        else f"Failed with code {result.rc}"
                    )
                }),
                "timestamp": int(time.time())
            }
            
        except KeyError as e:
            response_payload = {
                "status": 400,
                "body": json.dumps({
                    "error": f"Missing required field: {str(e)}"
                }),
                "timestamp": int(time.time())
            }
        except Exception as e:
            response_payload = {
                "status": 500,
                "body": json.dumps({"error": f"MQTT error: {str(e)}"}),
                "timestamp": int(time.time())
            }
        
        return self.crypto.encrypt(response_payload, secret)
