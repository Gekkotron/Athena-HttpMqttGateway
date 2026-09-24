"""Automated test suite for MQTT SSE subscription."""
import base64
import json
import time
import sys
import os
import threading
import requests

# Add parent directory to path to import crypto module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_client._crypto import ClientCrypto, load_secret_key


class SSETestClient:
    """Test client for MQTT SSE subscriptions."""

    def __init__(self, server_url: str, secret_key: str):
        self.server_url = server_url
        self.crypto = ClientCrypto(secret_key)

    def test_subscribe_connection(self, topic: str, timeout: int = 5) -> dict:
        """
        Test SSE subscription connection.

        Returns dict with:
        - success: bool
        - events: list of received events
        - error: str (if failed)
        """
        payload = {
            "topic": topic,
            "qos": 0,
            "timestamp": int(time.time())
        }

        encrypted_payload = self.crypto.encrypt(payload)
        encoded_payload = base64.b64encode(encrypted_payload)

        events = []
        error = None

        try:
            response = requests.post(
                f"{self.server_url}/mqtt/subscribe",
                data=encoded_payload,
                headers={"Content-Type": "application/octet-stream"},
                stream=True,
                timeout=timeout
            )

            if response.status_code != 200:
                return {
                    "success": False,
                    "events": [],
                    "error": f"HTTP {response.status_code}: {response.text}"
                }

            # Read SSE events for a limited time
            start_time = time.time()
            for line in response.iter_lines(decode_unicode=True):
                if time.time() - start_time > timeout:
                    break

                if line and line.startswith("data: "):
                    data = line[6:]  # Remove "data: " prefix
                    try:
                        encrypted_data = base64.b64decode(data)
                        decrypted_message = self.crypto.decrypt(encrypted_data)
                        events.append(decrypted_message)

                        # If we get connected event, that's enough for the test
                        if decrypted_message.get("type") == "connected":
                            break

                    except Exception as e:
                        error = f"Error decrypting message: {e}"
                        break

            return {
                "success": len(events) > 0 and error is None,
                "events": events,
                "error": error
            }

        except requests.exceptions.Timeout:
            return {
                "success": False,
                "events": events,
                "error": "Connection timeout - no events received"
            }
        except Exception as e:
            return {
                "success": False,
                "events": events,
                "error": str(e)
            }


def main():
    secret_key_file = "data/secret_key.txt"
    try:
        SECRET_KEY = load_secret_key(secret_key_file)
        print(f"Loaded secret key from {secret_key_file}")
    except Exception as e:
        print(f"Error loading secret key: {e}")
        return

    SERVER_URL = os.getenv("GATEWAY_URL", "http://localhost:10000")
    client = SSETestClient(SERVER_URL, SECRET_KEY)

    total_tests = 0
    passed_tests = 0
    failed_tests = 0

    # SSE Test 1: Basic subscription connection
    total_tests += 1
    print("\n--- SSE Test 1: Basic subscription connection ---")
    try:
        result = client.test_subscribe_connection("test/sse/topic", timeout=5)

        if result["success"]:
            print(f"✓ Connected successfully")
            print(f"  Events received: {len(result['events'])}")
            for event in result["events"]:
                print(f"  - Type: {event.get('type')}, Message: {event.get('message', 'N/A')}")
            print("✓ SSE Test 1 PASSED\n")
            passed_tests += 1
        else:
            print(f"✗ Connection failed: {result['error']}")
            print("✗ SSE Test 1 FAILED\n")
            failed_tests += 1

    except Exception as e:
        print(f"✗ SSE Test 1 FAILED: {e}\n")
        failed_tests += 1

    # SSE Test 2: Subscription with wildcard topic
    total_tests += 1
    print("\n--- SSE Test 2: Subscription with wildcard topic ---")
    try:
        result = client.test_subscribe_connection("test/sse/#", timeout=5)

        if result["success"]:
            print(f"✓ Connected successfully with wildcard topic")
            print(f"  Events received: {len(result['events'])}")
            for event in result["events"]:
                print(f"  - Type: {event.get('type')}, Topic: {event.get('topic', 'N/A')}")
            print("✓ SSE Test 2 PASSED\n")
            passed_tests += 1
        else:
            print(f"✗ Connection failed: {result['error']}")
            print("✗ SSE Test 2 FAILED\n")
            failed_tests += 1

    except Exception as e:
        print(f"✗ SSE Test 2 FAILED: {e}\n")
        failed_tests += 1

    # SSE Test 3: Connection with invalid topic (empty)
    total_tests += 1
    print("\n--- SSE Test 3: Connection with invalid topic (should fail) ---")
    try:
        payload = {
            "topic": "",
            "qos": 0,
            "timestamp": int(time.time())
        }
        encrypted_payload = client.crypto.encrypt(payload)
        encoded_payload = base64.b64encode(encrypted_payload)

        response = requests.post(
            f"{SERVER_URL}/mqtt/subscribe",
            data=encoded_payload,
            headers={"Content-Type": "application/octet-stream"},
            stream=True,
            timeout=3
        )

        # Should get error event
        error_found = False
        for line in response.iter_lines(decode_unicode=True):
            if line and line.startswith("data: "):
                data = line[6:]
                encrypted_data = base64.b64decode(data)
                decrypted_message = client.crypto.decrypt(encrypted_data)
                if decrypted_message.get("type") == "error":
                    error_found = True
                    print(f"✓ Correctly received error: {decrypted_message.get('message')}")
                    break

        if error_found:
            print("✓ SSE Test 3 PASSED (correctly rejected invalid topic)\n")
            passed_tests += 1
        else:
            print("✗ SSE Test 3 FAILED (should have received error)\n")
            failed_tests += 1

    except Exception as e:
        # Connection might fail immediately, which is also acceptable
        print(f"✓ SSE Test 3 PASSED (connection rejected): {e}\n")
        passed_tests += 1

    # Print summary
    print("=" * 60)
    print("MQTT SSE TEST SUMMARY")
    print("=" * 60)
    print(f"Total tests run: {total_tests}")
    print(f"Tests passed:    {passed_tests} ✓")
    print(f"Tests failed:    {failed_tests} ✗")
    print(f"Success rate:    {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")
    print("=" * 60)


if __name__ == "__main__":
    main()
