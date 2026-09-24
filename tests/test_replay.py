import time
from unittest import mock

from server.replay import ReplayGuard


def test_accepts_fresh_request():
    assert ReplayGuard(60).check(b"n" * 12, time.time()) is None


def test_rejects_missing_or_non_numeric_timestamp():
    guard = ReplayGuard(60)
    assert guard.check(b"a" * 12, None) == "Request expired"
    assert guard.check(b"b" * 12, "123") == "Request expired"


def test_rejects_timestamps_outside_window_both_ways():
    guard = ReplayGuard(60)
    now = time.time()
    assert guard.check(b"a" * 12, now - 61) == "Request expired"
    assert guard.check(b"b" * 12, now + 61) == "Request expired"


def test_rejects_same_nonce_twice():
    guard = ReplayGuard(60)
    now = time.time()
    assert guard.check(b"n" * 12, now) is None
    assert guard.check(b"n" * 12, now) == "Request replayed"


def test_expired_request_does_not_burn_the_nonce():
    guard = ReplayGuard(60)
    now = time.time()
    assert guard.check(b"n" * 12, now - 3600) == "Request expired"
    assert guard.check(b"n" * 12, now) is None


def test_prunes_nonces_once_their_window_closes():
    guard = ReplayGuard(60)
    now = time.time()
    guard.check(b"old" * 4, now - 50)  # expires at now + 10
    guard.check(b"new" * 4, now)       # expires at now + 60
    with mock.patch("server.replay.time.time", return_value=now + 11):
        guard.check(b"x" * 12, now + 11)
    assert b"old" * 4 not in guard._seen
    assert b"new" * 4 in guard._seen
