"""HTTP client for https://www.tcmb.gov.tr/kurlar/YYYYMM/DDMMYYYY.xml.

A 404 means "no bulletin that day" (weekend, holiday, archive gap) and is a normal
answer, not an error. Requests are rate-limited across threads to stay polite.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

import httpx

from ..http import USER_AGENT, FetchError, PoliteClient

BASE_URL = "https://www.tcmb.gov.tr/kurlar"
__all__ = ["BASE_URL", "USER_AGENT", "FetchResult", "TcmbClient", "TcmbFetchError", "bulletin_url"]


class TcmbFetchError(FetchError):
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
        self._base_url = base_url.rstrip("/")
        self._polite = PoliteClient(
            http, min_interval, max_retries, sleep, clock, error=TcmbFetchError
        )

    def fetch(self, day: date) -> FetchResult:
        resp = self._polite.get(bulletin_url(day, self._base_url), label=str(day))
        if resp.status_code == 404:
            return FetchResult(day, 404, None)
        if resp.status_code != 200:
            raise TcmbFetchError(f"{day}: unexpected HTTP {resp.status_code}")
        if not _looks_like_xml(resp.content):
            # e.g. a maintenance page served with 200 — never land it as data
            raise TcmbFetchError(f"{day}: HTTP 200 but body is not a TCMB XML bulletin")
        return FetchResult(day, 200, resp.content)
