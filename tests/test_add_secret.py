import re

import pytest

from server.add_secret import add_secret, main
from server.key_manager import load_or_generate_secrets

HEX64 = re.compile(r"^[0-9a-f]{64}$")


def test_add_secret_creates_file_and_dirs(tmp_path):
    path = tmp_path / "sub" / "keys.txt"
    key = add_secret(str(path), "80")
    assert HEX64.match(key)
    assert path.read_text() == f"{key}:80\n"


def test_add_secret_appends_with_comment(tmp_path):
    path = tmp_path / "keys.txt"
    path.write_text("aa" * 32 + ":*")  # no trailing newline
    key = add_secret(str(path), "1883@10.0.0.0/24", comment="phone")
    assert path.read_text() == f"{'aa' * 32}:*\n# phone\n{key}:1883@10.0.0.0/24\n"
    secrets = load_or_generate_secrets(str(path))
    assert len(secrets) == 2
    assert secrets[1].permits(1883, "10.0.0.8")


def test_add_secret_rejects_bad_scope(tmp_path):
    path = tmp_path / "keys.txt"
    with pytest.raises(ValueError):
        add_secret(str(path), "80@router.local")
    assert not path.exists()


def test_add_secret_refuses_to_append_to_broken_file(tmp_path):
    path = tmp_path / "keys.txt"
    path.write_text("not-hex\n")
    with pytest.raises(ValueError):
        add_secret(str(path), "*")
    assert path.read_text() == "not-hex\n"


def test_cli_combines_scope_and_dest(tmp_path, capsys):
    path = tmp_path / "keys.txt"
    assert main(["--scope", "80", "--dest", "192.168.1.50", "--key-file", str(path)]) == 0
    line = path.read_text().strip()
    assert line.endswith(":80@192.168.1.50")
    assert line.split(":")[0] in capsys.readouterr().out


def test_cli_defaults_to_full_access(tmp_path):
    path = tmp_path / "keys.txt"
    assert main(["--key-file", str(path)]) == 0
    assert path.read_text().strip().endswith(":*")


@pytest.mark.parametrize("argv", [
    ["--scope", "80@10.0.0.1", "--dest", "10.0.0.2"],  # both destination forms
    ["--scope", "http"],
])
def test_cli_errors_exit_2(tmp_path, capsys, argv):
    path = tmp_path / "keys.txt"
    assert main(argv + ["--key-file", str(path)]) == 2
    assert capsys.readouterr().err.startswith("error:")
    assert not path.exists()
