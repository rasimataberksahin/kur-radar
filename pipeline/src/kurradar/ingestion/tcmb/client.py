"""HTTP client for https://www.tcmb.gov.tr/kurlar/YYYYMM/DDMMYYYY.xml.

A 404 means "no bulletin that day" (weekend, holiday, archive gap) and is a normal
answer, not an error. Requests are rate-limited across threads to stay polite.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

import httpx

BASE_URL = "https://www.tcmb.gov.tr/kurlar"
USER_AGENT = "KurRadar/0.1 (+open-source FX research pipeline)"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class TcmbFetchError(RuntimeError):
    pass


@dataclass(frozen=True)
class FetchResult:
    day: date
    http_status: int
    content: bytes | None  # None when nothing was published that day

    @property
    def published(self) -> bool:
        return self.content is not None


def bulletin_url(day: date, base_url: str = BASE_URL) -> str:
    return f"{base_url}/{day:%Y%m}/{day:%d%m%Y}.xml"


def _looks_like_xml(content: bytes) -> bool:
    head = content.lstrip(b"\xef\xbb\xbf \t\r\n")[:200]
    return head.startswith(b"<?xml") or head.startswith(b"<Tarih_Date")


class TcmbClient:
    def __init__(
        self,
        http: httpx.Client | None = None,
        base_url: str = BASE_URL,
        min_interval: float = 0.25,
        max_retries: int = 4,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._http = http or httpx.Client(
            timeout=httpx.Timeout(30.0, connect=15.0),
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        )
        self._base_url = base_url.rstrip("/")
        self._min_interval = min_interval
        self._max_retries = max_retries
        self._sleep = sleep
        self._clock = clock
        self._lock = threading.Lock()
        self._next_slot = 0.0

    def _wait_for_slot(self) -> None:
        with self._lock:
            now = self._clock()
            wait = self._next_slot - now
            self._next_slot = max(now, self._next_slot) + self._min_interval
        if wait > 0:
            self._sleep(wait)

    def fetch(self, day: date) -> FetchResult:
        url = bulletin_url(day, self._base_url)
        for attempt in range(1, self._max_retries + 2):
            self._wait_for_slot()
            try:
                resp = self._http.get(url)
            except httpx.TransportError as exc:
                if attempt > self._max_retries:
                    raise TcmbFetchError(f"{day}: network error after {attempt} tries") from exc
                self._sleep(min(2**attempt, 30))
                continue

            if resp.status_code == 404:
                return FetchResult(day, 404, None)
            if resp.status_code in RETRYABLE_STATUS and attempt <= self._max_retries:
                self._sleep(min(2**attempt, 30))
                continue
            if resp.status_code != 200:
                raise TcmbFetchError(f"{day}: unexpected HTTP {resp.status_code}")
            if not _looks_like_xml(resp.content):
                # e.g. a maintenance page served with 200 — never land it as data
                raise TcmbFetchError(f"{day}: HTTP 200 but body is not a TCMB XML bulletin")
            return FetchResult(day, 200, resp.content)
        raise AssertionError("unreachable")
