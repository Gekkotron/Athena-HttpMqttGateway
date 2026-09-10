"""Health check for the HTTP encryption gateway.

Runs two checks:
  1. Plain GET /health on the gateway (proves Flask is up).
  2. Encrypted round-trip via /gateway to a well-known URL
     (proves the crypto path is intact end-to-end).

Exit codes:
  0 - all checks passed
  1 - one or more checks failed
  2 - configuration error (missing secret key, unreachable URL, ...)

Environment variables:
  GATEWAY_URL          Gateway base URL (default: http://localhost:10000)
  SECRET_KEY_FILE      Path to the hex secret key. If unset, the first existing
                       of `data/secret_key.txt` (Docker mount) and
                       `server/secret_key.txt` (dev) is used.
  HEALTH_CHECK_URL     URL used for the encrypted round-trip
                       (default: https://httpbin.org/get)
  HEALTH_TIMEOUT       Per-request timeout in seconds (default: 10)
  GATEWAY_RESTART_CMD  Shell command used by restart() to bounce the app.
                       Default: 'docker compose restart gateway'
"""
import base64
import json
import os
import shlex
import subprocess
import sys
import time

import requests
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from dotenv import load_dotenv

from ._env import env_float, env_str


def _encrypt(secret_key: bytes, data: dict) -> bytes:
    nonce = os.urandom(12)
    aesgcm = AESGCM(secret_key)
    ciphertext = aesgcm.encrypt(nonce, json.dumps(data).encode(), None)
    return nonce + ciphertext


def _decrypt(secret_key: bytes, data: bytes) -> dict:
    nonce, ciphertext = data[:12], data[12:]
    aesgcm = AESGCM(secret_key)
    plaintext = aesgcm.decrypt(nonce, ciphertext, None)
    return json.loads(plaintext)


def check_health_endpoint(gateway_url: str, timeout: float) -> tuple[bool, str]:
    """Ping the plain /health endpoint."""
    try:
        resp = requests.get(f"{gateway_url}/health", timeout=timeout)
    except requests.RequestException as exc:
        return False, f"unreachable: {exc}"

    if resp.status_code != 200:
        return False, f"HTTP {resp.status_code}: {resp.text[:120]}"

    try:
        body = resp.json()
    except ValueError:
        return False, f"non-JSON body: {resp.text[:120]}"

    if body.get("status") != "ok":
        return False, f"unexpected body: {body}"

    return True, f"200 ok (version={body.get('version', '?')})"


def check_encrypted_roundtrip(
    gateway_url: str, secret_key: bytes, target_url: str, timeout: float
) -> tuple[bool, str]:
    """Send an encrypted request through /gateway and verify the response."""
    payload = {
        "url": target_url,
        "method": "GET",
        "timestamp": int(time.time()),
    }
    try:
        encrypted = base64.b64encode(_encrypt(secret_key, payload))
        resp = requests.post(
            f"{gateway_url}/gateway",
            data=encrypted,
            headers={"Content-Type": "application/octet-stream"},
            timeout=timeout,
        )
    except requests.RequestException as exc:
        return False, f"unreachable: {exc}"

    if resp.status_code != 200:
        return False, f"HTTP {resp.status_code} at gateway"

    try:
        decrypted = _decrypt(secret_key, base64.b64decode(resp.content))
    except Exception as exc:
        return False, f"decryption failed: {exc}"

    inner_status = decrypted.get("status")
    if inner_status != 200:
        body_preview = str(decrypted.get("body"))[:120]
        return False, f"inner status {inner_status}: {body_preview}"

    return True, f"round-trip 200 via {target_url}"


DEFAULT_RESTART_CMD = "docker compose restart gateway"


def restart(verbose: bool = True) -> tuple[bool, str]:
    """Run the configured restart command for the HTTP gateway."""
    cmd = os.getenv("GATEWAY_RESTART_CMD", DEFAULT_RESTART_CMD)
    if verbose:
        print(f"[gateway] restarting via: {cmd}")

    try:
        result = subprocess.run(
            shlex.split(cmd),
            capture_output=True,
            text=True,
            timeout=60,
        )
    except FileNotFoundError as exc:
        return False, f"restart command not found: {exc}"
    except subprocess.TimeoutExpired:
        return False, "restart command timed out after 60s"

    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[:200]
        return False, f"restart exited {result.returncode}: {detail}"

    return True, "restart command succeeded"


def _load_secret_key(path: str) -> bytes:
    """Return the first usable hex secret from a line-based secret file."""
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            return bytes.fromhex(line.split(":", 1)[0].strip())
    raise ValueError(f"No secret found in {path}")


# Order matters: Docker layout (bind-mounted `./data:/app/data`) first,
# then the plain-Python dev layout. An explicit SECRET_KEY_FILE overrides both.
_KEY_FILE_CANDIDATES = ("data/secret_key.txt", "server/secret_key.txt")


def _resolve_key_file(explicit: str) -> str | None:
    """Return the path to use for the secret key, or None if none is found.

    If SECRET_KEY_FILE is set, honour it exactly (returned as-is; the caller
    surfaces FileNotFoundError with the same path for a helpful message).
    Otherwise search the well-known candidate locations.
    """
    if explicit:
        return explicit
    for candidate in _KEY_FILE_CANDIDATES:
        if os.path.isfile(candidate):
            return candidate
    return None


def run(verbose: bool = True) -> int:
    """Run the gateway health check. Returns a process exit code."""
    load_dotenv()

    gateway_url = env_str("GATEWAY_URL", "http://localhost:10000").rstrip("/")
    explicit_key_file = env_str("SECRET_KEY_FILE")
    target_url = env_str("HEALTH_CHECK_URL", "https://httpbin.org/get")
    timeout = env_float("HEALTH_TIMEOUT", 10)

    if verbose:
        print(f"[gateway] checking {gateway_url}")

    key_file = _resolve_key_file(explicit_key_file)
    if key_file is None:
        candidates = ", ".join(_KEY_FILE_CANDIDATES)
        print(
            f"[gateway] CONFIG ERROR: no secret key file found "
            f"(searched: {candidates}). Set SECRET_KEY_FILE to override."
        )
        return 2
    if verbose and not explicit_key_file:
        print(f"[gateway] using secret key from {key_file} (auto-detected)")

    try:
        secret_key = _load_secret_key(key_file)
    except FileNotFoundError:
        print(f"[gateway] CONFIG ERROR: secret key file not found: {key_file}")
        return 2
    except ValueError as exc:
        print(f"[gateway] CONFIG ERROR: invalid secret key: {exc}")
        return 2

    checks = [
        ("liveness (/health)", lambda: check_health_endpoint(gateway_url, timeout)),
        (
            "encrypted round-trip",
            lambda: check_encrypted_roundtrip(gateway_url, secret_key, target_url, timeout),
        ),
    ]

    failed = 0
    for name, run_check in checks:
        ok, detail = run_check()
        marker = "PASS" if ok else "FAIL"
        if verbose:
            print(f"[gateway] {marker} {name}: {detail}")
        if not ok:
            failed += 1

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(run())
