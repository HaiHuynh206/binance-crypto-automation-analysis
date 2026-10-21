"""HTTP client for Binance public REST endpoints.

Two concerns are handled here:

* client-side rate limiting in terms of Binance request "weight";
* retry with exponential backoff for transient failures (5xx, timeouts, 429/418).
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque

import requests

log = logging.getLogger(__name__)


class RateLimiter:
    """Sliding-window limiter that tracks Binance request weight."""

    def __init__(self, budget_per_minute: int, window_seconds: float = 60.0) -> None:
        self.budget = max(int(budget_per_minute), 1)
        self.window = window_seconds
        self._used: deque[tuple[float, int]] = deque()
        self._lock = threading.Lock()

    def _trim(self, now: float) -> None:
        while self._used and now - self._used[0][0] > self.window:
            self._used.popleft()

    def _current(self) -> int:
        return sum(weight for _, weight in self._used)

    def acquire(self, weight: int = 1) -> None:
        weight = max(int(weight), 1)
        while True:
            with self._lock:
                now = time.monotonic()
                self._trim(now)
                if not self._used or self._current() + weight <= self.budget:
                    self._used.append((now, weight))
                    return
                wait = self.window - (now - self._used[0][0]) + 0.05
            log.debug("Rate limit reached, sleeping %.2fs", wait)
            time.sleep(max(wait, 0.05))


class BinanceHTTP:
    def __init__(
        self,
        *,
        timeout_seconds: float = 20.0,
        max_retries: int = 5,
        weight_per_minute: int = 2400,
        session: requests.Session | None = None,
    ) -> None:
        self.timeout = timeout_seconds
        self.max_retries = max_retries
        self.limiter = RateLimiter(weight_per_minute)
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "crypto-ingest/0.1"})

    def get(self, url: str, params: dict, *, weight: int = 1) -> list | dict:
        self.limiter.acquire(weight)
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                last_error = exc
                log.warning("Request error (attempt %s/%s): %s", attempt, self.max_retries, exc)
            else:
                if response.status_code in (418, 429):
                    retry_after = float(response.headers.get("Retry-After", "5"))
                    log.warning(
                        "Rate limited with HTTP %s, sleeping %.1fs",
                        response.status_code,
                        retry_after,
                    )
                    time.sleep(retry_after)
                    last_error = RuntimeError(f"HTTP {response.status_code}")
                    continue
                if response.status_code >= 500:
                    last_error = RuntimeError(f"HTTP {response.status_code}: {response.text[:200]}")
                    log.warning("Server error (attempt %s): %s", attempt, last_error)
                elif response.status_code >= 400:
                    # Client errors (bad symbol, invalid range) are not retryable.
                    raise RuntimeError(
                        f"HTTP {response.status_code} for {url}: {response.text[:300]}"
                    )
                else:
                    payload = response.json()
                    used = response.headers.get("X-MBX-USED-WEIGHT-1M")
                    if used:
                        log.debug("Binance used weight: %s/6000", used)
                    return payload

            if attempt < self.max_retries:
                time.sleep(min(2 ** (attempt - 1), 30))

        raise RuntimeError(f"Request failed after {self.max_retries} attempts: {last_error}")
