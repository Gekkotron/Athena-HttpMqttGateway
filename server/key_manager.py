"""Secret key loading, generation, and per-secret port + destination scoping."""
import ipaddress
import os
from typing import Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class Secret:
    """A single AES-GCM secret with per-endpoint and per-destination scoping."""

    def __init__(
        self,
        key: bytes,
        allowed_ports: Optional[frozenset],
        allowed_destinations: Optional[list] = None,
    ):
        if len(key) != 32:
            raise ValueError("Secret key must be exactly 32 bytes (256-bit AES).")
        self.key = key
        self.allowed_ports = allowed_ports  # None = every port allowed
        # None = every destination allowed. Otherwise a list of ip_network
        # objects (single IPs are stored as /32 or /128 networks).
        self.allowed_destinations = allowed_destinations
        self.aesgcm = AESGCM(key)

    def allows(self, port: int) -> bool:
        return self.allowed_ports is None or port in self.allowed_ports

    def allows_destination(self, host: str) -> bool:
        """True when ``host`` is inside this secret's allowed destinations.

        Wildcard secrets always allow. Otherwise ``host`` must parse as an IP
        (v4 or v6) and fall inside one of the configured networks; a hostname
        sent to an IP-scoped secret is refused (no DNS resolution).
        """
        if self.allowed_destinations is None:
            return True
        if not host:
            return False
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            return False
        return any(ip in net for net in self.allowed_destinations)


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
    ports, dests = _parse_scope(scope_part, lineno)
    return Secret(key, ports, dests)


def _parse_scope(scope: str, lineno: int):
    """Split ``ports[@destinations]`` into (allowed_ports, allowed_destinations).

    Both halves return ``None`` when unrestricted (``*`` or omitted).
    """
    if "@" in scope:
        ports_part, dests_part = scope.split("@", 1)
    else:
        ports_part = scope
        dests_part = "*"
    ports = _parse_ports(ports_part.strip(), lineno)
    dests = _parse_destinations(dests_part.strip(), lineno)
    return ports, dests


def _parse_ports(ports: str, lineno: int):
    if ports == "*":
        return None
    try:
        return frozenset(int(p.strip()) for p in ports.split(",") if p.strip())
    except ValueError as e:
        raise ValueError(f"Invalid port list on line {lineno}: {ports!r}") from e


def _parse_destinations(dests: str, lineno: int):
    if dests == "*" or dests == "":
        return None
    parsed = []
    for entry in dests.split(","):
        entry = entry.strip()
        if not entry:
            continue
        try:
            # strict=False lets a plain IP without /prefix parse as /32 or /128.
            parsed.append(ipaddress.ip_network(entry, strict=False))
        except ValueError as e:
            raise ValueError(
                f"Invalid destination on line {lineno}: {entry!r} "
                f"(must be an IP address or CIDR range; hostnames not allowed)"
            ) from e
    if not parsed:
        return None
    return parsed


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
    print("Scope: * (full access to every port and destination)")
    print("=" * 80)
    print("Please update your client with this secret key.")
    print("=" * 80)

    return Secret(key, None, None)
