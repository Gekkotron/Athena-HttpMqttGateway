"""CLI: append a freshly generated random secret to the secret-key file.

Usage:
    python -m server.add_secret [--scope SCOPE] [--dest DEST]
                                [--key-file PATH] [--comment TEXT]

Examples:
    python -m server.add_secret --scope 80                       # HTTP gateway only
    python -m server.add_secret --scope 80,1883                  # HTTP + MQTT
    python -m server.add_secret --scope '*'                      # full access
    python -m server.add_secret --scope 80 --dest 192.168.1.50   # only that host
    python -m server.add_secret --scope 1883 --dest 192.168.1.0/24
    python -m server.add_secret --scope '80@192.168.1.50'        # embedded form

The new secret is printed to stdout so you can copy it into a client.
"""
import argparse
import os
import sys

# Support both `python -m server.add_secret` (package context set) and
# `python server/add_secret.py` (bare-script invocation): if we were not
# imported as part of a package, put the project root on sys.path so the
# absolute imports below resolve.
if __package__ in (None, ""):
    sys.path.insert(
        0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )

from server import config
from server.key_manager import _parse_scope, _parse_secrets_file  # reuse parsers


def _validate_scope(scope: str) -> str:
    """Reuse the file-format parser so CLI scopes match runtime semantics."""
    _parse_scope(scope, lineno=0)  # raises ValueError on bad input
    return scope


def _compose_scope(scope: str, dest: str) -> str:
    """Combine ``--scope`` and ``--dest`` into the on-disk scope string."""
    if not dest:
        return scope
    if "@" in scope:
        raise ValueError(
            "--dest cannot be used when --scope already contains '@...'; "
            "pick one form."
        )
    return f"{scope}@{dest}"


def add_secret(key_file: str, scope: str, comment: str = "") -> str:
    """Append a new random secret and return its hex representation."""
    _validate_scope(scope)

    # Guard: if the file already exists, make sure it parses cleanly. We do
    # not want to append to a file that is currently broken.
    if os.path.exists(key_file):
        _parse_secrets_file(key_file)

    hex_key = os.urandom(32).hex()

    os.makedirs(os.path.dirname(key_file) or ".", exist_ok=True)

    # Ensure we start on a fresh line even if the previous line was missing \n.
    prefix = ""
    if os.path.exists(key_file) and os.path.getsize(key_file) > 0:
        with open(key_file, "rb") as f:
            f.seek(-1, os.SEEK_END)
            if f.read(1) != b"\n":
                prefix = "\n"

    with open(key_file, "a", encoding="utf-8") as f:
        if comment:
            f.write(f"{prefix}# {comment}\n")
            prefix = ""
        f.write(f"{prefix}{hex_key}:{scope}\n")

    return hex_key


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a random 256-bit secret and append it to the key file."
    )
    parser.add_argument(
        "--scope",
        default="*",
        help="Access scope: '*' for full access, or a comma-separated port list "
             "(e.g. 80, 1883, or 80,1883). Defaults to '*'. May also include an "
             "embedded destination filter: '80@192.168.1.50'.",
    )
    parser.add_argument(
        "--dest",
        default="",
        help="Optional destination filter as a comma-separated list of IPs or "
             "CIDR ranges (e.g. 192.168.1.50 or 192.168.1.0/24). Hostnames are "
             "not accepted. Cannot be combined with an @ suffix in --scope.",
    )
    parser.add_argument(
        "--key-file",
        default=config.SECRET_KEY_FILE,
        help=f"Path to the secret-key file (default: {config.SECRET_KEY_FILE}).",
    )
    parser.add_argument(
        "--comment",
        default="",
        help="Optional label written as a '# comment' line above the new secret.",
    )
    args = parser.parse_args(argv)

    try:
        scope = _compose_scope(args.scope, args.dest)
        hex_key = add_secret(args.key_file, scope, args.comment)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    print("=" * 80)
    print("NEW SECRET APPENDED")
    print("=" * 80)
    print(f"Secret key: {hex_key}")
    print(f"Scope:      {scope}")
    print(f"File:       {args.key_file}")
    print("=" * 80)
    print("Configure your client with this key.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
