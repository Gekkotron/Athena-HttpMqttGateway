"""Secret key loading, generation, and per-secret port + destination scoping."""
import ipaddress
import os
from typing import Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class Secret:
    """A single AES-GCM secret plus a list of (ports, destinations) rules.

    A request is permitted if ANY rule matches both the endpoint port and
    the target host. Wildcard ports (``None``) match every port; wildcard
    destinations (``None``) match every host.

    Multiple file lines that share the same hex key are collapsed into one
    Secret with several rules -- that is how a caller expresses "port A only
    to host X, port B only to host Y" under a single key.
    """

    def __init__(self, key: bytes):
        if len(key) != 32:
            raise ValueError("Secret key must be exactly 32 bytes (256-bit AES).")
        self.key = key
        self.aesgcm = AESGCM(key)
        # Each entry: (allowed_ports: frozenset|None, allowed_destinations: list|None)
        self.rules = []

    def add_rule(self, allowed_ports: Optional[frozenset],
                 allowed_destinations: Optional[list]) -> None:
        self.rules.append((allowed_ports, allowed_destinations))

    def allows_port(self, port: int) -> bool:
        """True when some rule contains this port (destination ignored)."""
        for ports, _dests in self.rules:
            if ports is None or port in ports:
                return True
        return False

    def permits(self, port: int, host: Optional[str]) -> bool:
        """True when some rule permits this (port, host) pair.

        The host must parse as an IP (v4 or v6) unless the matching rule has
        a wildcard destination; a hostname sent to an IP-scoped rule is
        refused (no DNS resolution).
        """
        parsed_ip = None
        if host:
            try:
                parsed_ip = ipaddress.ip_address(host)
            except ValueError:
                parsed_ip = None
        for ports, dests in self.rules:
            if ports is not None and port not in ports:
                continue
            if dests is None:
                return True
            if parsed_ip is None:
                continue
            if any(parsed_ip in net for net in dests):
                return True
        return False


def load_or_generate_secrets(key_file: str) -> list:
    """Load all secrets from ``key_file`` or auto-generate a wildcard one."""
    if os.path.exists(key_file):
        secrets = _parse_secrets_file(key_file)
        if not secrets:
            raise ValueError(f"No secrets found in {key_file}")
        return secrets
    return [_generate_and_save(key_file)]


def _parse_secrets_file(path: str) -> list:
    """Return the file's secrets, grouping lines that share the same key."""
    by_key = {}
    order = []
    with open(path, "r", encoding="utf-8") as f:
        for lineno, raw in enumerate(f, start=1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            key, ports, dests = _parse_secret_line(line, lineno)
            if key not in by_key:
                by_key[key] = Secret(key)
                order.append(key)
            by_key[key].add_rule(ports, dests)
    return [by_key[k] for k in order]


def _parse_secret_line(line: str, lineno: int):
    """Return (key_bytes, allowed_ports, allowed_destinations) for one line."""
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
    if len(key) != 32:
        raise ValueError(
            f"Secret on line {lineno} must be 32 bytes (64 hex chars)"
        )
    ports, dests = _parse_scope(scope_part, lineno)
    return key, ports, dests


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

    secret = Secret(key)
    secret.add_rule(None, None)
    return secret
