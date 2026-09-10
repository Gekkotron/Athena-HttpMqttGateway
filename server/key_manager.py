"""Secret key loading, generation, and per-secret port scoping."""
import os
from typing import Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class Secret:
    """A single AES-GCM secret and the ports it is allowed to reach."""

    def __init__(self, key: bytes, allowed_ports: Optional[frozenset]):
        if len(key) != 32:
            raise ValueError("Secret key must be exactly 32 bytes (256-bit AES).")
        self.key = key
        self.allowed_ports = allowed_ports  # None = wildcard (all ports)
        self.aesgcm = AESGCM(key)

    def allows(self, port: int) -> bool:
        return self.allowed_ports is None or port in self.allowed_ports


def load_or_generate_secrets(key_file: str) -> list:
    """Load all secrets from ``key_file`` or auto-generate a wildcard one."""
    if os.path.exists(key_file):
        secrets = _parse_secrets_file(key_file)
        if not secrets:
            raise ValueError(f"No secrets found in {key_file}")
        return secrets
    return [_generate_and_save(key_file)]


def _parse_secrets_file(path: str) -> list:
    secrets = []
    with open(path, "r", encoding="utf-8") as f:
        for lineno, raw in enumerate(f, start=1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            secrets.append(_parse_secret_line(line, lineno))
    return secrets


def _parse_secret_line(line: str, lineno: int) -> Secret:
    if ":" in line:
        hex_part, scope_part = line.split(":", 1)
        hex_part = hex_part.strip()
        scope_part = scope_part.strip()
    else:
        # Back-compat: a bare hex line (legacy single-secret file) = full access.
        hex_part = line
        scope_part = "*"
    try:
        key = bytes.fromhex(hex_part)
    except ValueError as e:
        raise ValueError(f"Invalid hex secret on line {lineno}") from e
    return Secret(key, _parse_scope(scope_part, lineno))


def _parse_scope(scope: str, lineno: int):
    if scope == "*":
        return None
    try:
        return frozenset(int(p.strip()) for p in scope.split(",") if p.strip())
    except ValueError as e:
        raise ValueError(f"Invalid port list on line {lineno}: {scope!r}") from e


def _generate_and_save(key_file: str) -> Secret:
    key = os.urandom(32)
    hex_key = key.hex()

    os.makedirs(os.path.dirname(key_file) or ".", exist_ok=True)
    with open(key_file, "w", encoding="utf-8") as f:
        f.write(f"{hex_key}:*\n")

    print("=" * 80)
    print("NEW SECRET KEY GENERATED!")
    print("=" * 80)
    print(f"Secret key: {hex_key}")
    print(f"Saved to: {key_file}")
    print("Scope: * (full access to every port)")
    print("=" * 80)
    print("Please update your client with this secret key.")
    print("=" * 80)

    return Secret(key, None)
