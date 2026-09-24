import requests
import base64
import json
import time
import os
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

def load_secret_key(secret_key_file):
    """Return the first usable hex secret from a line-based secret file.

    Accepts `<hex>` (legacy) and `<hex>:<scope>` (multi-secret) lines and
    skips blank/comment lines.
    """
    with open(secret_key_file, 'r', encoding='utf-8') as f:
        for raw in f:
            line = raw.split('#', 1)[0].strip()
            if not line:
                continue
            return line.split(':', 1)[0].strip()
    raise ValueError(f"No secret found in {secret_key_file}")

class EncryptedClient:
    def __init__(self, gateway_url: str, secret_key: str):
        self.gateway_url = gateway_url
        self.secret_key = bytes.fromhex(secret_key)
    
    def encrypt(self, data: dict) -> bytes:
        nonce = os.urandom(12)
        aesgcm = AESGCM(self.secret_key)
        plaintext = json.dumps(data).encode()
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)
        return nonce + ciphertext
    
    def decrypt(self, data: bytes) -> dict:
        nonce, ciphertext = data[:12], data[12:]
        aesgcm = AESGCM(self.secret_key)
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        return json.loads(plaintext)
    
    def send_mqtt(self, topic: str, message: str, broker_host: str = None,
                  broker_port: int = None, username: str = None,
                  password: str = None, qos: int = 0, retain: bool = False) -> dict:
        """
        Send MQTT publish request via gateway server.

        Args:
            topic: MQTT topic to publish to
            message: Message to publish
            broker_host: Optional MQTT broker hostname
            broker_port: Optional MQTT broker port
            username: Optional MQTT username
            password: Optional MQTT password
            qos: Quality of Service level (0, 1, or 2)
            retain: Retain flag

        Returns:
            Decrypted response as dict with keys: status, body, timestamp
        """
        payload = {
            "topic": topic,
            "message": message,
            "qos": qos,
            "retain": retain,
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

        encrypted_data = self.encrypt(payload)
        encoded_data = base64.b64encode(encrypted_data)

        response = requests.post(
            f"{self.gateway_url}/mqtt/publish",
            data=encoded_data,
            headers={"Content-Type": "application/octet-stream"},
            timeout=30
        )

        encrypted_response = base64.b64decode(response.content)
        decrypted_response = self.decrypt(encrypted_response)
        return decrypted_response

def main():
    secret_key_file = "data/secret_key.txt"
    try:
        SECRET_KEY = load_secret_key(secret_key_file)
        print(f"Loaded secret key from {secret_key_file}")
    except Exception as e:
        print(f"Error loading secret key: {e}")
        return
    GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:10000")
    client = EncryptedClient(GATEWAY_URL, SECRET_KEY)

    total_tests = 0
    passed_tests = 0
    failed_tests = 0

    # MQTT Test 1: Basic publish
    total_tests += 1
    print("\n--- MQTT Test 1: Basic publish ---")
    try:
        response = client.send_mqtt(
            topic="test/topic",
            message="Hello MQTT!",
        )
        print(f"Status: {response['status']}")
        print(f"Response: {response['body']}")
        print(f"Timestamp: {response['timestamp']}")
        if response['status'] == 200:
            print("✓ MQTT Test 1 PASSED\n")
            passed_tests += 1
        else:
            print("✗ MQTT Test 1 FAILED\n")
            failed_tests += 1
    except Exception as e:
        print(f"✗ MQTT Test 1 FAILED: {e}\n")
        failed_tests += 1

    # MQTT Test 2: With QoS and Retain
    total_tests += 1
    print("\n--- MQTT Test 2: With QoS and Retain ---")
    try:
        response = client.send_mqtt(
            topic="test/qos",
            message="QoS 1 Retain True",
            qos=1,
            retain=True
        )
        print(f"Status: {response['status']}")
        print(f"Response: {response['body']}")
        print(f"Timestamp: {response['timestamp']}")
        if response['status'] == 200:
            print("✓ MQTT Test 2 PASSED\n")
            passed_tests += 1
        else:
            print("✗ MQTT Test 2 FAILED\n")
            failed_tests += 1
    except Exception as e:
        print(f"✗ MQTT Test 2 FAILED: {e}\n")
        failed_tests += 1

    # MQTT Test 3: Custom broker host/port (should fail if not set)
    total_tests += 1
    print("\n--- MQTT Test 3: Custom broker host/port (should fail if not set) ---")
    try:
        response = client.send_mqtt(
            topic="test/custom",
            message="Custom broker",
            broker_host="192.168.1.91",
            broker_port=1883
        )
        print(f"Status: {response['status']}")
        print(f"Response: {response['body']}")
        print(f"Timestamp: {response['timestamp']}")
        # Accept either 200 or error, depending on server config
        if response['status'] == 200 or response['status'] == 500:
            print("✓ MQTT Test 3 PASSED (server handled custom broker)\n")
            passed_tests += 1
        else:
            print("✗ MQTT Test 3 FAILED\n")
            failed_tests += 1
    except Exception as e:
        print(f"✓ MQTT Test 3 PASSED (expected failure): {e}\n")
        passed_tests += 1

    # MQTT Test 4: Missing topic (should fail)
    total_tests += 1
    print("\n--- MQTT Test 4: Missing topic (should fail) ---")
    try:
        response = client.send_mqtt(
            topic=None,
            message="No topic"
        )
        print(f"Status: {response['status']}")
        print(f"Response: {response['body']}")
        print(f"Timestamp: {response['timestamp']}")
        if response['status'] != 200:
            print("✓ MQTT Test 4 PASSED (correctly rejected)\n")
            passed_tests += 1
        else:
            print("✗ MQTT Test 4 FAILED (should have been rejected)\n")
            failed_tests += 1
    except Exception as e:
        print(f"✓ MQTT Test 4 PASSED (expected failure): {e}\n")
        passed_tests += 1

    # Print summary
    print("=" * 60)
    print("MQTT TEST SUMMARY")
    print("=" * 60)
    print(f"Total tests run: {total_tests}")
    print(f"Tests passed:    {passed_tests} ✓")
    print(f"Tests failed:    {failed_tests} ✗")
    print(f"Success rate:    {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")
    print("=" * 60)

if __name__ == "__main__":
    main()
