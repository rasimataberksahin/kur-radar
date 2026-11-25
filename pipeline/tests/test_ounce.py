import json
from datetime import date
from decimal import Decimal

import httpx
import pytest
from conftest import fixture_bytes

from kurradar.ingestion.gold import GoldPayloadError
from kurradar.ingestion.gold.ounce import YahooChartProvider, _from_float32
from kurradar.ingestion.http import FetchError, PoliteClient

CHART = fixture_bytes("yahoo_gcf_2026-11-25.json")


def chart(mutate) -> bytes:
    doc = json.loads(CHART)
    mutate(doc["chart"])
    return json.dumps(doc).encode()


def provider(handler=None) -> YahooChartProvider:
    http = httpx.Client(transport=httpx.MockTransport(handler or (lambda r: httpx.Response(500))))
    return YahooChartProvider(polite=PoliteClient(http=http, min_interval=0, sleep=lambda s: None))


@pytest.mark.parametrize(
    "raw, expected",
    [
        (273.8999938964844, "273.9"),
        (260.29998779296875, "260.3"),
        (3561.89990234375, "3561.9"),
        (3559.300048828125, "3559.3"),
        (261.0, "261.0"),
        (None, None),
    ],
)
def test_float32_artefacts_are_recovered(raw, expected):
    assert _from_float32(raw) == (Decimal(expected) if expected else None)


def test_parses_real_chart():
    h = provider().parse(CHART)
    days = [b.price_date for b in h.bars]
    assert days == [
        date(2000, 8, 30),
        date(2000, 8, 31),
        date(2001, 2, 13),
        date(2006, 11, 24),  # half-day session stamped 09:30 New York time
        date(2026, 11, 24),
        date(2026, 11, 25),
    ]
    assert h.skipped == 1  # 2000-09-04 (Labor Day): every field null
    first = h.bars[0]
    assert (first.open, first.close, first.volume) == (Decimal("273.9"), Decimal("273.9"), 0)


def test_settlement_outside_high_low_is_kept_but_flagged():
    bars = {b.price_date: b for b in provider().parse(CHART).bars}
    odd = bars[date(2001, 2, 13)]
    assert (odd.high, odd.low, odd.close) == (Decimal("261.0"), Decimal("261.0"), Decimal("260.3"))
    assert not odd.ohlc_consistent
    assert bars[date(2026, 11, 24)].ohlc_consistent


def _set_meta(key, value):
    return lambda c: c["result"][0]["meta"].__setitem__(key, value)


def _dup_last(c):
    r = c["result"][0]
    r["timestamp"].append(r["timestamp"][-1] + 60)
    for v in r["indicators"]["quote"][0].values():
        v.append(v[-1])


@pytest.mark.parametrize(
    "mutate, message",
    [
        (_set_meta("symbol", "SI=F"), "asked for GC=F"),
        (_set_meta("currency", "EUR"), "not USD"),
        (_set_meta("dataGranularity", "1wk"), "expected 1d"),
        (lambda c: c.__setitem__("error", {"code": "Not Found"}), "Yahoo error"),
        (lambda c: c["result"][0]["indicators"]["quote"][0]["close"].pop(), "differ in length"),
        (_dup_last, "two bars for 2026-11-25"),
        (
            lambda c: c["result"][0]["indicators"]["quote"][0].__setitem__("close", [None] * 7),
            "no priced bars",
        ),
        (
            lambda c: c["result"][0]["indicators"]["quote"][0]["close"].__setitem__(0, -1.0),
            "non-positive close",
        ),
    ],
)
def test_rejects_untrustworthy_charts(mutate, message):
    with pytest.raises(GoldPayloadError, match=message):
        provider().parse(chart(mutate))


def test_rejects_non_chart_json():
    with pytest.raises(GoldPayloadError, match="not a Yahoo chart"):
        provider().parse(b'{"finance": {"error": "x"}}')


def test_fetch_asks_for_full_or_incremental_history():
    seen = []

    def handler(req):
        seen.append(req.url)
        return httpx.Response(200, content=CHART)

    p = provider(handler)
    assert p.fetch(None) == CHART
    p.fetch(date(2026, 11, 15))
    assert seen[0].path == "/v8/finance/chart/GC=F"
    assert seen[0].params["period1"] == "0" and seen[0].params["interval"] == "1d"
    assert seen[1].params["period1"] == "1794700800"  # 2026-11-15T00:00Z


def test_fetch_refuses_html():
    with pytest.raises(FetchError, match="not JSON"):
        provider(lambda r: httpx.Response(200, text="<html>consent</html>")).fetch(None)
