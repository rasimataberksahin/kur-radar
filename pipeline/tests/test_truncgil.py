import json
from datetime import datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from conftest import fixture_bytes

from kurradar.ingestion.gold import GoldPayloadError
from kurradar.ingestion.gold.truncgil import ISTANBUL, TruncgilClient, parse_snapshot
from kurradar.ingestion.http import FetchError, PoliteClient

BOARD = fixture_bytes("truncgil_2026-11-25.json")
OBSERVED = datetime(2026, 11, 25, 14, 38, 1, tzinfo=ISTANBUL)


def board(**changes) -> bytes:
    doc = json.loads(BOARD)
    for k, v in changes.items():
        if v is None:
            doc.pop(k, None)
        else:
            doc[k] = v
    return json.dumps(doc).encode()


def by_code(snap):
    return {r["code"]: r for r in snap.rows}


def test_parses_real_board():
    snap = parse_snapshot(BOARD, fetched_at=OBSERVED + timedelta(minutes=1))
    assert snap.observed_at == OBSERVED
    rows = by_code(snap)
    assert set(rows) == {"USD", "GRA", "HAS", "CEYREKALTIN", "ONS", "XU100", "GUMUS"}
    gra = rows["GRA"]
    assert (gra["instrument"], gra["name"]) == ("Gold", "GRAMALTIN")
    assert (gra["buying"], gra["selling"], gra["change_pct"]) == (
        Decimal("5721.38"),
        Decimal("5722.25"),
        Decimal("-1.05"),
    )
    assert rows["USD"]["name"] is None  # currencies have no Name


def test_zero_price_means_no_quote():
    ons = by_code(parse_snapshot(BOARD))["ONS"]
    assert ons["buying"] is None and ons["selling"] is None
    assert ons["change_pct"] == Decimal("-1.16")  # a change can still be published


def test_missing_buying_key_is_null():
    xu = by_code(parse_snapshot(BOARD))["XU100"]
    assert xu["buying"] is None and xu["selling"] == Decimal("10740")


def test_numbers_are_exact_decimals():
    raw = board(GRA={"Type": "Gold", "Buying": 0.1, "Selling": 0.30000000000000004})
    gra = by_code(parse_snapshot(raw))["GRA"]
    assert gra["selling"] == Decimal("0.30000000000000004")


@pytest.mark.parametrize(
    "content, message",
    [
        (b"<html>bakim</html>", "not valid JSON"),
        (board(Update_Date=None), "missing Update_Date"),
        (board(Update_Date="25.11.2026 14:38"), "bad Update_Date"),
        (board(GRA=None), "no usable GRA"),
        (board(GRA={"Type": "Gold", "Buying": 0, "Selling": 0}), "no usable GRA"),
        (board(GRA={"Type": "Gold", "Buying": "5721,38", "Selling": 1}), "not a number"),
        (board(GRA={"Type": "Gold", "Buying": True, "Selling": 1}), "not a number"),
        (board(GRA={"Type": "Gold", "Buying": -1, "Selling": 1}), "negative"),
        (board(GRA={"Buying": 1, "Selling": 1}), "no Type"),
        (board(**{"gr a": {"Type": "Gold"}}), "invalid instrument code"),
    ],
)
def test_rejects_untrustworthy_boards(content, message):
    with pytest.raises(GoldPayloadError, match=message):
        parse_snapshot(content)


def test_duplicate_json_keys_are_rejected():
    raw = BOARD.replace(b'"HAS"', b'"GRA"', 1)
    with pytest.raises(GoldPayloadError, match="duplicate JSON key 'GRA'"):
        parse_snapshot(raw)


def test_future_update_date_is_rejected_beyond_clock_skew():
    parse_snapshot(BOARD, fetched_at=OBSERVED - timedelta(minutes=9))
    with pytest.raises(GoldPayloadError, match="in the future"):
        parse_snapshot(BOARD, fetched_at=OBSERVED - timedelta(minutes=11))


def test_unknown_scalar_fields_are_ignored_and_reported():
    snap = parse_snapshot(board(Meta_Version="5"))
    assert snap.ignored_keys == ["Meta_Version"]


def make_client(handler, sleeps=None):
    polite = PoliteClient(
        http=httpx.Client(transport=httpx.MockTransport(handler)),
        min_interval=0,
        sleep=(sleeps.append if sleeps is not None else lambda s: None),
    )
    return TruncgilClient(polite, url="https://truncgil.test/today.json")


def test_client_returns_bytes_and_retries_5xx():
    calls, sleeps = [], []

    def handler(req):
        calls.append(1)
        return httpx.Response(503) if len(calls) == 1 else httpx.Response(200, content=BOARD)

    assert make_client(handler, sleeps).fetch() == BOARD
    assert sleeps == [2]


@pytest.mark.parametrize(
    "response, message",
    [
        (httpx.Response(200, text="<html>Cloudflare</html>"), "not JSON"),
        (httpx.Response(403), "HTTP 403"),
    ],
)
def test_client_refuses_non_data(response, message):
    with pytest.raises(FetchError, match=message):
        make_client(lambda req: response).fetch()
