"""Daily ounce-gold history (USD per troy ounce) behind a swappable provider adapter.

Turkish gram-gold history is not published anywhere without registration, so dbt rebuilds
it as ``ounce close × TCMB USD/TRY ÷ 31.1034768`` and calibrates it against Truncgil's live
board. The default provider is Yahoo Finance's public chart endpoint for COMEX gold
futures (``GC=F``, since 2000-08-30). It is unofficial, so it sits behind ``OunceProvider``
and can be replaced without touching the loader or the table.

Yahoo quirks handled here (each pinned by a test):
- prices are float32 artefacts (273.8999938964844) → recovered to the shortest decimal
- holidays come back as bars whose fields are all null → skipped and counted
- half-day sessions are stamped 09:30 instead of midnight → the exchange-local date is used
- the close is the settlement price and can fall outside the session's high/low (441 days,
  mostly 2001–2010) → kept, but ``ohlc_consistent`` is false
- the last bar is the session still trading → ``is_final`` is false until a later fetch
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Protocol
from zoneinfo import ZoneInfo

import psycopg

from ..http import FetchError, PoliteClient
from ..landing import LandedFile
from ..manifest import loaded_period, record_file, upsert_sql
from . import GoldPayloadError

COLUMNS = ["open", "high", "low", "close", "volume", "ohlc_consistent", "is_final"]
# a bar that was final never goes back to "still trading" (e.g. reloading an older file)
_UPSERT = upsert_sql(
    "raw.gold_ounce_daily",
    ["provider", "symbol", "price_date"],
    COLUMNS,
    extra_guard="EXCLUDED.is_final OR NOT t.is_final",
)


@dataclass(frozen=True)
class OunceBar:
    price_date: date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal
    volume: int | None

    @property
    def ohlc_consistent(self) -> bool:
        if None in (self.open, self.high, self.low):
            return False
        return self.low <= min(self.open, self.close) and self.high >= max(self.open, self.close)


@dataclass(frozen=True)
class OunceHistory:
    bars: list[OunceBar]
    skipped: int  # bars with no close (holidays)


class OunceProvider(Protocol):
    source: str  # landing / manifest source name
    provider: str
    symbol: str

    def fetch(self, start: date | None) -> bytes: ...
    def parse(self, content: bytes) -> OunceHistory: ...


def _from_float32(x: float | None) -> Decimal | None:
    """Shortest decimal that rounds to the same float32 (273.8999938964844 → 273.9)."""
    if x is None:
        return None
    target = struct.unpack("f", struct.pack("f", x))[0]
    for digits in range(0, 7):
        cand = round(x, digits)
        if struct.unpack("f", struct.pack("f", cand))[0] == target:
            return Decimal(repr(cand))
    return Decimal(repr(x)).quantize(Decimal("0.0001"))


class YahooChartProvider:
    source = "yahoo_gcf"
    provider = "yahoo"
    BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

    def __init__(self, symbol: str = "GC=F", polite: PoliteClient | None = None) -> None:
        self.symbol = symbol
        self._polite = polite or PoliteClient(min_interval=2.0)

    def fetch(self, start: date | None) -> bytes:
        period1 = (
            0
            if start is None
            else int(datetime(start.year, start.month, start.day, tzinfo=UTC).timestamp())
        )
        resp = self._polite.get(
            f"{self.BASE_URL}/{self.symbol}",
            label=f"yahoo {self.symbol}",
            params={
                "period1": period1,
                "period2": int(datetime.now(UTC).timestamp()),
                "interval": "1d",
                "events": "history",
            },
        )
        if resp.status_code != 200:
            raise FetchError(f"yahoo {self.symbol}: unexpected HTTP {resp.status_code}")
        if not resp.content.lstrip().startswith(b"{"):
            raise FetchError(f"yahoo {self.symbol}: HTTP 200 but body is not JSON")
        return resp.content

    def parse(self, content: bytes) -> OunceHistory:
        try:
            chart = json.loads(content)["chart"]
        except (ValueError, KeyError, TypeError) as exc:
            raise GoldPayloadError(f"not a Yahoo chart response: {exc}") from exc
        if chart.get("error"):
            raise GoldPayloadError(f"Yahoo error: {chart['error']}")
        try:
            result = chart["result"][0]
            meta = result["meta"]
            stamps = result.get("timestamp") or []
            quote = result["indicators"]["quote"][0]
        except (KeyError, IndexError, TypeError) as exc:
            raise GoldPayloadError(f"unexpected Yahoo chart shape: {exc}") from exc
        if meta.get("symbol") != self.symbol:
            raise GoldPayloadError(f"asked for {self.symbol}, got {meta.get('symbol')}")
        if meta.get("currency") != "USD":
            raise GoldPayloadError(f"{self.symbol} is quoted in {meta.get('currency')}, not USD")
        if meta.get("dataGranularity") not in (None, "1d"):
            raise GoldPayloadError(f"granularity {meta.get('dataGranularity')}, expected 1d")
        tz = ZoneInfo(meta.get("exchangeTimezoneName") or "America/New_York")
        series = {k: quote.get(k) or [] for k in ("open", "high", "low", "close", "volume")}
        if any(len(v) != len(stamps) for v in series.values()):
            raise GoldPayloadError("timestamp and price arrays differ in length")

        bars, skipped, seen = [], 0, set()
        for i, ts in enumerate(stamps):
            day = datetime.fromtimestamp(ts, tz).date()
            if day in seen:
                raise GoldPayloadError(f"two bars for {day}")
            seen.add(day)
            close = _from_float32(series["close"][i])
            if close is None:
                skipped += 1
                continue
            if close <= 0:
                raise GoldPayloadError(f"non-positive close on {day}: {close}")
            vol = series["volume"][i]
            bars.append(
                OunceBar(
                    day,
                    _from_float32(series["open"][i]),
                    _from_float32(series["high"][i]),
                    _from_float32(series["low"][i]),
                    close,
                    int(vol) if vol is not None else None,
                )
            )
        if not bars:
            raise GoldPayloadError("no priced bars in the response")
        return OunceHistory(bars, skipped)


@dataclass(frozen=True)
class OunceLoad:
    sha256: str
    skipped: bool
    bars_in_file: int = 0
    rows_changed: int = 0
    first: date | None = None
    last: date | None = None


def load_ounce_history(
    conn: psycopg.Connection, provider: OunceProvider, landed: LandedFile
) -> OunceLoad:
    """Upsert every bar of one landed response.

    ``landed.period_date`` is the (Istanbul) day it was fetched: a bar dated before it is a
    closed session (``is_final``); the bar for that day itself may still be trading.
    """
    with conn.transaction():
        if loaded_period(conn, landed.sha256) is not None:
            return OunceLoad(landed.sha256, skipped=True)
        history = provider.parse(landed.path.read_bytes())
        record_file(conn, landed, len(history.bars), history.skipped)
        changed = 0
        with conn.cursor() as cur:
            for bar in history.bars:
                cur.execute(
                    _UPSERT,
                    {
                        "provider": provider.provider,
                        "symbol": provider.symbol,
                        "price_date": bar.price_date,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                        "ohlc_consistent": bar.ohlc_consistent,
                        "is_final": bar.price_date < landed.period_date,
                        "source_sha256": landed.sha256,
                    },
                )
                changed += cur.rowcount
    bars = history.bars
    return OunceLoad(
        landed.sha256, False, len(bars), changed, bars[0].price_date, bars[-1].price_date
    )
