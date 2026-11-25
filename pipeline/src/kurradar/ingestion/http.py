"""Polite HTTP for every source: a rate limit shared across threads, and retries with backoff.

429, 5xx and transport errors are retried. Any other status is returned to the caller,
which decides what it means (e.g. TCMB's 404 is a normal "nothing published").
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import httpx

USER_AGENT = "KurRadar/0.1 (+open-source FX research pipeline)"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class FetchError(RuntimeError):
    pass


def default_http() -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(30.0, connect=15.0),
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
    )


class PoliteClient:
    def __init__(
        self,
        http: httpx.Client | None = None,
        min_interval: float = 0.25,
        max_retries: int = 4,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        error: type[FetchError] = FetchError,
    ) -> None:
        self._http = http or default_http()
        self._min_interval = min_interval
        self._max_retries = max_retries
        self._sleep = sleep
        self._clock = clock
        self._error = error
        self._lock = threading.Lock()
        self._next_slot = 0.0

    def _wait_for_slot(self) -> None:
        with self._lock:
            now = self._clock()
            wait = self._next_slot - now
            self._next_slot = max(now, self._next_slot) + self._min_interval
        if wait > 0:
            self._sleep(wait)

    def get(self, url: str, label: str, params: dict | None = None) -> httpx.Response:
        """GET with retries; ``label`` names the request in error messages."""
        for attempt in range(1, self._max_retries + 2):
            self._wait_for_slot()
            try:
                resp = self._http.get(url, params=params)
            except httpx.TransportError as exc:
                if attempt > self._max_retries:
                    raise self._error(f"{label}: network error after {attempt} tries") from exc
                self._sleep(min(2**attempt, 30))
                continue
            if resp.status_code in RETRYABLE_STATUS and attempt <= self._max_retries:
                self._sleep(min(2**attempt, 30))
                continue
            return resp
        raise AssertionError("unreachable")
