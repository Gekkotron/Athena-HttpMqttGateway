"""Configuration settings for the encryption gateway."""
import os


SECRET_KEY_FILE = os.getenv("SECRET_KEY_FILE", "data/secret_key.txt")
MAX_AGE_SECONDS = int(os.getenv("MAX_AGE_SECONDS", "60"))
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "10000"))

# Cap on the upstream body returned by /gateway in raw mode ("raw": true).
HTTP_MAX_RESPONSE_BYTES = int(os.getenv("HTTP_MAX_RESPONSE_BYTES", str(25 * 1024 * 1024)))

# MQTT Configuration
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "192.168.1.91")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))

# Endpoint family "ports" used to scope multi-secret access. See README.
HTTP_PORT = 80
MQTT_PORT = 1883
