"""Replay protection: timestamp window plus a cache of recently seen nonces."""
import threading
import time


class ReplayGuard:
    """Rejects requests that are outside the time window or already seen.

    The AES-GCM nonce (first 12 bytes of the ciphertext) is authenticated, so
    a replayed request necessarily carries the same nonce. A nonce only needs
    remembering until its timestamp falls out of the window -- after that the
    timestamp check rejects it on its own.
    """

    def __init__(self, max_age_seconds: int):
        self.max_age = max_age_seconds
        self._seen = {}  # nonce -> expiry (unix time)
        self._lock = threading.Lock()

    def check(self, nonce: bytes, timestamp):
        """Return an error message if the request must be rejected, else None.

        A request that passes is recorded, so the same nonce fails next time.
        """
        now = time.time()
        if not isinstance(timestamp, (int, float)) or abs(now - timestamp) > self.max_age:
            return "Request expired"

        with self._lock:
            self._prune(now)
            if nonce in self._seen:
                return "Request replayed"
            self._seen[nonce] = timestamp + self.max_age
        return None

    def _prune(self, now: float):
        expired = [n for n, expiry in self._seen.items() if expiry < now]
        for n in expired:
            del self._seen[n]
