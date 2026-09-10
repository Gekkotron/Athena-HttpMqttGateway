import requests
import base64
import json
import time
import os
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from dotenv import load_dotenv


class EncryptedHttpClient:
    def __init__(self, gateway_url: str, secret_key: str):
        """
        Initialize encrypted HTTP client.

        Args:
            gateway_url: URL of the gateway server (e.g., "http://localhost:10000")
            secret_key: 64-character hex string (32 bytes)
        """
        self.gateway_url = gateway_url.rstrip("/")
        self.secret_key = bytes.fromhex(secret_key)

    def encrypt(self, data: dict) -> bytes:
        """Encrypt data using AES-GCM."""
        nonce = os.urandom(12)
        aesgcm = AESGCM(self.secret_key)
        plaintext = json.dumps(data).encode()
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)
        return nonce + ciphertext

    def decrypt(self, data: bytes) -> dict:
        """Decrypt data using AES-GCM."""
        nonce, ciphertext = data[:12], data[12:]
        aesgcm = AESGCM(self.secret_key)
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        return json.loads(plaintext)

    def send_request(self, url: str, method: str = "GET",
                     headers: dict = None, body=None, timeout: int = 30) -> dict:
        """
        Send encrypted HTTP request via gateway.

        Args:
            url: Full target URL
            method: HTTP method (GET, POST, etc.)
            headers: Optional HTTP headers dict
            body: Optional request body (str or dict)
            timeout: Request timeout in seconds

        Returns:
            Decrypted response dict with keys: status, body, timestamp
        """
        payload = {
            "url": url,
            "method": method,
            "timestamp": int(time.time()),
        }
        if headers:
            payload["headers"] = headers
        if body is not None:
            payload["body"] = body

        encrypted_data = self.encrypt(payload)
        encoded_data = base64.b64encode(encrypted_data)

        response = requests.post(
            f"{self.gateway_url}/gateway",
            data=encoded_data,
            headers={"Content-Type": "application/octet-stream"},
            timeout=timeout,
        )

        encrypted_response = base64.b64decode(response.content)
        return self.decrypt(encrypted_response)


def load_secret_key(path: str = "data/secret_key.txt") -> str:
    """Load the first usable hex secret from a line-based secret file.

    Accepts `<hex>` (legacy) and `<hex>:<scope>` (multi-secret) lines and
    skips blank/comment lines.
    """
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            return line.split(":", 1)[0].strip()
    raise ValueError(f"No secret found in {path}")


def main():
    load_dotenv()

    try:
        SECRET_KEY = load_secret_key()
        print("Loaded secret key from data/secret_key.txt")
    except FileNotFoundError:
        print("Error: data/secret_key.txt not found! Run the server first.")
        return

    GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:10000")
    # A public URL to test HTTP forwarding against
    TEST_URL = os.getenv("TEST_URL", "https://httpbin.org/get")

    client = EncryptedHttpClient(GATEWAY_URL, SECRET_KEY)

    total_tests = 0
    passed_tests = 0
    failed_tests = 0

    # Test 1: Valid GET request
    total_tests += 1
    print("Test 1: Valid GET request")
    try:
        response = client.send_request(url=TEST_URL, method="GET")
        print(f"Status: {response['status']}")
        if response["status"] == 200:
            print("PASS\n")
            passed_tests += 1
        else:
            print("FAIL\n")
            failed_tests += 1
    except Exception as e:
        print(f"FAIL: {e}\n")
        failed_tests += 1

    # Test 2: Valid POST request
    total_tests += 1
    print("Test 2: Valid POST request")
    try:
        response = client.send_request(
            url="https://httpbin.org/post",
            method="POST",
            headers={"Content-Type": "application/json"},
            body={"key": "value"},
        )
        print(f"Status: {response['status']}")
        if response["status"] == 200:
            print("PASS\n")
            passed_tests += 1
        else:
            print("FAIL\n")
            failed_tests += 1
    except Exception as e:
        print(f"FAIL: {e}\n")
        failed_tests += 1

    # Test 3: Expired timestamp (should be rejected)
    total_tests += 1
    print("Test 3: Expired timestamp (70 seconds old)")
    try:
        payload = {
            "url": TEST_URL,
            "method": "GET",
            "timestamp": int(time.time()) - 70,
        }
        encrypted_data = client.encrypt(payload)
        encoded_data = base64.b64encode(encrypted_data)
        resp = requests.post(
            f"{client.gateway_url}/gateway",
            data=encoded_data,
            headers={"Content-Type": "application/octet-stream"},
            timeout=30,
        )
        decrypted = client.decrypt(base64.b64decode(resp.content))
        if decrypted["status"] == 403 and "expired" in str(decrypted["body"]).lower():
            print("PASS (correctly rejected)\n")
            passed_tests += 1
        else:
            print("FAIL (should have been rejected)\n")
            failed_tests += 1
    except Exception as e:
        print(f"FAIL: {e}\n")
        failed_tests += 1

    # Test 4: Future timestamp (should be rejected)
    total_tests += 1
    print("Test 4: Future timestamp (70 seconds ahead)")
    try:
        payload = {
            "url": TEST_URL,
            "method": "GET",
            "timestamp": int(time.time()) + 70,
        }
        encrypted_data = client.encrypt(payload)
        encoded_data = base64.b64encode(encrypted_data)
        resp = requests.post(
            f"{client.gateway_url}/gateway",
            data=encoded_data,
            headers={"Content-Type": "application/octet-stream"},
            timeout=30,
        )
        decrypted = client.decrypt(base64.b64decode(resp.content))
        if decrypted["status"] == 403 and "expired" in str(decrypted["body"]).lower():
            print("PASS (correctly rejected)\n")
            passed_tests += 1
        else:
            print("FAIL (should have been rejected)\n")
            failed_tests += 1
    except Exception as e:
        print(f"FAIL: {e}\n")
        failed_tests += 1

    # Test 5: Wrong secret key
    total_tests += 1
    print("Test 5: Wrong secret key")
    try:
        wrong_client = EncryptedHttpClient(
            GATEWAY_URL,
            "0" * 64,
        )
        wrong_client.send_request(url=TEST_URL)
        print("FAIL (should have failed decryption)\n")
        failed_tests += 1
    except Exception:
        print("PASS (correctly failed)\n")
        passed_tests += 1

    # Test 6: Missing URL field
    total_tests += 1
    print("Test 6: Missing URL field")
    try:
        payload = {
            "method": "GET",
            "timestamp": int(time.time()),
        }
        encrypted_data = client.encrypt(payload)
        encoded_data = base64.b64encode(encrypted_data)
        resp = requests.post(
            f"{client.gateway_url}/gateway",
            data=encoded_data,
            headers={"Content-Type": "application/octet-stream"},
            timeout=30,
        )
        decrypted = client.decrypt(base64.b64decode(resp.content))
        if decrypted["status"] == 403:
            print("PASS (correctly rejected)\n")
            passed_tests += 1
        else:
            print("FAIL\n")
            failed_tests += 1
    except Exception as e:
        print(f"FAIL: {e}\n")
        failed_tests += 1

    # Summary
    print("=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Total: {total_tests}  Passed: {passed_tests}  Failed: {failed_tests}")
    print(f"Rate: {passed_tests / total_tests * 100:.0f}%" if total_tests else "N/A")
    print("=" * 60)


if __name__ == "__main__":
    main()
