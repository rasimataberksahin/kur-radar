"""Gold loaders against a real Postgres (TEST_DATABASE_URL)."""

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from conftest import fixture_bytes

from kurradar.ingestion.gold import GoldPayloadError
from kurradar.ingestion.gold.ingest import (
    OUNCE_OVERLAP_DAYS,
    ingest_ounce,
    ingest_snapshot,
    reload_all,
)
from kurradar.ingestion.gold.ounce import YahooChartProvider, load_ounce_history
from kurradar.ingestion.gold.status import coverage
from kurradar.ingestion.gold.truncgil import load_snapshot
from kurradar.ingestion.landing import land
from kurradar.ingestion.tcmb.loader import load_bulletin

pytestmark = pytest.mark.db
BOARD = fixture_bytes("truncgil_2026-11-25.json")
CHART = fixture_bytes("yahoo_gcf_2026-11-25.json")
FETCH_DAY = date(2026, 11, 25)


def one(conn, sql, *params):
    return conn.execute(sql, params).fetchone()[0]


def edit_board(**entries) -> bytes:
    doc = json.loads(BOARD)
    doc.update(entries)
    return json.dumps(doc).encode()


def edit_close(day_index: int, close: float) -> bytes:
    doc = json.loads(CHART)
    doc["chart"]["result"][0]["indicators"]["quote"][0]["close"][day_index] = close
    return json.dumps(doc).encode()


class FakeProvider(YahooChartProvider):
    def __init__(self, content=CHART):
        super().__init__()
        self.content, self.starts = content, []

    def fetch(self, start):
        self.starts.append(start)
        return self.content


class FakeBoard:
    def __init__(self, content=BOARD):
        self.content = content

    def fetch(self):
        return self.content


# ---------- Truncgil board ----------


def test_snapshot_load_then_reload_is_noop(db_conn, tmp_path):
    f = land(tmp_path, "truncgil", FETCH_DAY, BOARD, "json")
    r1 = load_snapshot(db_conn, f)
    assert (r1.skipped, r1.rows_in_file, r1.rows_changed) == (False, 7, 7)
    assert load_snapshot(db_conn, f).skipped
    assert one(db_conn, "SELECT count(*) FROM raw.truncgil_quotes") == 7
    assert one(db_conn, "SELECT count(*) FROM raw.ingest_manifest") == 1


def test_same_update_date_with_new_bytes_rewrites_only_changed_quotes(db_conn, tmp_path):
    load_snapshot(db_conn, land(tmp_path, "truncgil", FETCH_DAY, BOARD, "json"))
    gra = {"Type": "Gold", "Name": "GRAMALTIN", "Buying": 5730, "Selling": 5731, "Change": -0.9}
    v2 = land(tmp_path, "truncgil", FETCH_DAY, edit_board(GRA=gra), "json")
    r = load_snapshot(db_conn, v2)
    assert r.rows_changed == 1
    assert one(db_conn, "SELECT count(*) FROM raw.truncgil_quotes") == 7
    assert one(db_conn, "SELECT buying FROM raw.truncgil_quotes WHERE code = 'GRA'") == Decimal(
        "5730"
    )


def test_a_moved_board_adds_a_new_snapshot(db_conn, tmp_path):
    load_snapshot(db_conn, land(tmp_path, "truncgil", FETCH_DAY, BOARD, "json"))
    later = edit_board(Update_Date="2026-11-25 14:53:01")
    load_snapshot(db_conn, land(tmp_path, "truncgil", FETCH_DAY, later, "json"))
    assert one(db_conn, "SELECT count(DISTINCT observed_at) FROM raw.truncgil_quotes") == 2


def test_invalid_board_loads_nothing(db_conn, tmp_path):
    bad = land(tmp_path, "truncgil", FETCH_DAY, edit_board(GRA={"Type": "Gold"}), "json")
    with pytest.raises(GoldPayloadError):
        load_snapshot(db_conn, bad)
    assert one(db_conn, "SELECT count(*) FROM raw.ingest_manifest") == 0
    assert one(db_conn, "SELECT count(*) FROM raw.truncgil_quotes") == 0


def test_snapshot_lands_under_the_istanbul_fetch_day(db_conn, tmp_path):
    # 23:30 UTC on the 24th is already the 25th in Istanbul (UTC+3)
    now = datetime(2026, 11, 24, 23, 30, tzinfo=UTC)
    board = edit_board(Update_Date="2026-11-25 02:29:00")
    ingest_snapshot(db_conn, tmp_path, FakeBoard(board), now=now)
    assert one(db_conn, "SELECT period_date FROM raw.ingest_manifest") == FETCH_DAY
    assert list((tmp_path / "truncgil" / "2026" / "11").glob("truncgil_2026-11-25_*.json"))


# ---------- ounce history ----------


def test_ounce_bars_before_the_fetch_day_are_final(db_conn, tmp_path):
    p = FakeProvider()
    r = load_ounce_history(db_conn, p, land(tmp_path, p.source, FETCH_DAY, CHART, "json"))
    assert (r.bars_in_file, r.rows_changed, r.first, r.last) == (
        6,
        6,
        date(2000, 8, 30),
        FETCH_DAY,
    )
    rows = dict(db_conn.execute("SELECT price_date, is_final FROM raw.gold_ounce_daily"))
    assert rows[date(2026, 11, 24)] is True and rows[FETCH_DAY] is False
    assert one(db_conn, "SELECT skipped_count FROM raw.ingest_manifest") == 1


def test_next_fetch_finalises_the_trading_bar_and_never_reverts_it(db_conn, tmp_path):
    p = FakeProvider()
    load_ounce_history(db_conn, p, land(tmp_path, p.source, FETCH_DAY, CHART, "json"))
    settled = edit_close(-1, 3550.0)
    r = load_ounce_history(
        db_conn, p, land(tmp_path, p.source, FETCH_DAY + timedelta(days=1), settled, "json")
    )
    assert r.rows_changed == 1  # only the bar that was still trading
    q = "SELECT close, is_final FROM raw.gold_ounce_daily WHERE price_date = %s"
    assert db_conn.execute(q, (FETCH_DAY,)).fetchone() == (Decimal("3550.000000"), True)

    # an older, intraday file (fetched on the 25th, different bytes) arrives late
    intraday = edit_close(-1, 3540.0)
    load_ounce_history(db_conn, p, land(tmp_path, p.source, FETCH_DAY, intraday, "json"))
    assert db_conn.execute(q, (FETCH_DAY,)).fetchone() == (Decimal("3550.000000"), True)


def test_ingest_ounce_is_full_first_then_incremental_with_overlap(db_conn, tmp_path):
    p = FakeProvider()
    ingest_ounce(db_conn, tmp_path, p, today=FETCH_DAY)
    second = ingest_ounce(db_conn, tmp_path, p, today=FETCH_DAY)
    ingest_ounce(db_conn, tmp_path, p, full=True, today=FETCH_DAY)
    assert p.starts == [None, FETCH_DAY - timedelta(days=OUNCE_OVERLAP_DAYS), None]
    assert second.skipped  # identical bytes


def test_reload_is_a_noop_and_reports_invalid_files(db_conn, tmp_path):
    p = FakeProvider()
    ingest_ounce(db_conn, tmp_path, p, today=FETCH_DAY)
    ingest_snapshot(db_conn, tmp_path, FakeBoard(), now=datetime(2026, 11, 25, 12, 0, tzinfo=UTC))
    land(tmp_path, "truncgil", FETCH_DAY, b'{"Update_Date": "2026-11-25 15:00:00"}', "json")
    out = reload_all(db_conn, tmp_path, p)
    assert out["rows_changed"] == 0
    assert len(out["invalid"]) == 1 and "no usable GRA" in out["invalid"][0][1]


def test_status_cross_checks_gram_gold_against_tcmb(db_conn, tmp_path, new_bulletin):
    load_bulletin(db_conn, land(tmp_path, "tcmb", date(2026, 11, 24), new_bulletin, "xml"))
    p = FakeProvider()
    ingest_ounce(db_conn, tmp_path, p, today=FETCH_DAY)
    ingest_snapshot(db_conn, tmp_path, FakeBoard(), now=datetime(2026, 11, 25, 12, 0, tzinfo=UTC))
    c = coverage(db_conn)
    assert (c["truncgil_snapshots"], c["ounce_bars"], c["ounce_not_final"]) == (1, 6, 1)
    assert c["ounce_ohlc_inconsistent"] == 1
    day, implied = c["implied_gram_gold"]
    # 3561.9 USD/oz × 50.6286 TRY/USD ÷ 31.1034768 g/oz
    assert (day, implied) == (date(2026, 11, 24), Decimal("5797.87"))
