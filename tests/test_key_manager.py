import pytest

from server.key_manager import load_or_generate_secrets

KEY_A = "aa" * 32
KEY_B = "bb" * 32


def _load(tmp_path, text):
    path = tmp_path / "secrets.txt"
    path.write_text(text, encoding="utf-8")
    return load_or_generate_secrets(str(path))


def test_bare_hex_line_is_full_access(tmp_path):
    [secret] = _load(tmp_path, f"{KEY_A}\n")
    assert secret.allows_port(80) and secret.allows_port(1883)
    assert secret.permits(1883, "example.com")


def test_ignores_comments_and_blank_lines(tmp_path):
    secrets = _load(tmp_path, f"# header\n\n{KEY_A}:*  # inline\n")
    assert len(secrets) == 1


def test_port_scope(tmp_path):
    [secret] = _load(tmp_path, f"{KEY_A}:80\n")
    assert secret.allows_port(80)
    assert not secret.allows_port(1883)


def test_destination_scope_is_ip_or_cidr_only(tmp_path):
    [secret] = _load(tmp_path, f"{KEY_A}:80@192.168.1.0/24,10.0.0.5\n")
    assert secret.permits(80, "192.168.1.42")
    assert secret.permits(80, "10.0.0.5")
    assert not secret.permits(80, "10.0.0.6")
    assert not secret.permits(80, "router.local")  # no DNS resolution
    assert not secret.permits(80, None)
    assert not secret.permits(1883, "192.168.1.42")


def test_duplicate_key_lines_merge_into_one_secret(tmp_path):
    secrets = _load(
        tmp_path,
        f"{KEY_A}:80@10.0.0.1\n{KEY_B}:*\n{KEY_A}:1883@10.0.0.2\n",
    )
    assert len(secrets) == 2
    merged = secrets[0]
    assert merged.permits(80, "10.0.0.1")
    assert merged.permits(1883, "10.0.0.2")
    assert not merged.permits(80, "10.0.0.2")
    assert not merged.permits(1883, "10.0.0.1")


@pytest.mark.parametrize("line", [
    "zz" * 32,                 # not hex
    "aa" * 16,                 # too short
    f"{KEY_A}:http",           # bad port list
    f"{KEY_A}:80@router.local",  # hostname destination
])
def test_invalid_lines_raise(tmp_path, line):
    with pytest.raises(ValueError):
        _load(tmp_path, line + "\n")


def test_empty_file_raises(tmp_path):
    with pytest.raises(ValueError):
        _load(tmp_path, "# nothing\n")


def test_generates_wildcard_secret_when_missing(tmp_path):
    path = tmp_path / "sub" / "secret_key.txt"
    [secret] = load_or_generate_secrets(str(path))
    assert path.read_text().strip() == f"{secret.key.hex()}:*"
    assert secret.permits(1883, "anything")
