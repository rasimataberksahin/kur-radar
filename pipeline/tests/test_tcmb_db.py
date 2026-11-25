"""Loader/backfill tests against a real Postgres (TEST_DATABASE_URL)."""

from datetime import date
from decimal import Decimal

import httpx
import pytest

from kurradar.db import migrate
from kurradar.ingestion.landing import land
from kurradar.ingestion.tcmb.backfill import plan_days, run_backfill
from kurradar.ingestion.tcmb.client import TcmbClient
from kurradar.ingestion.tcmb.loader import load_bulletin, record_not_published
from kurradar.ingestion.tcmb.parser import TcmbPayloadError
from kurradar.ingestion.tcmb.status import coverage

pytestmark = pytest.mark.db
D = date(2026, 11, 24)


def one(conn, sql, *params):
    return conn.execute(sql, params).fetchone()[0]


def test_migrations_are_idempotent(db_conn):
    assert migrate(db_conn) == []
    assert one(db_conn, "SELECT count(*) FROM raw.schema_migrations") == 2


def test_load_then_reload_is_noop(db_conn, tmp_path, new_bulletin):
    f = land(tmp_path, "tcmb", D, new_bulletin, "xml")
    r1 = load_bulletin(db_conn, f)
    assert (r1.skipped, r1.rows_in_file, r1.rows_changed) == (False, 3, 3)
    stamp = one(db_conn, "SELECT max(loaded_at) FROM raw.tcmb_rates")

    r2 = load_bulletin(db_conn, f)
    assert r2.skipped
    assert one(db_conn, "SELECT count(*) FROM raw.tcmb_rates") == 3
    assert one(db_conn, "SELECT count(*) FROM raw.ingest_manifest") == 1
    assert one(db_conn, "SELECT max(loaded_at) FROM raw.tcmb_rates") == stamp
    assert one(db_conn, "SELECT status FROM raw.fetch_log WHERE target_date = %s", D) == "loaded"


def test_revision_rewrites_only_changed_currencies(db_conn, tmp_path, new_bulletin):
    v1 = land(tmp_path, "tcmb", D, new_bulletin, "xml")
    load_bulletin(db_conn, v1)
    revised = new_bulletin.replace(b"<ForexBuying>50.5376", b"<ForexBuying>50.6000")
    v2 = land(tmp_path, "tcmb", D, revised, "xml")
    r = load_bulletin(db_conn, v2)

    assert (r.rows_in_file, r.rows_changed) == (3, 1)
    usd = db_conn.execute(
        "SELECT forex_buying, source_sha256 FROM raw.tcmb_rates WHERE currency_code = 'USD'"
    ).fetchone()
    assert usd == (Decimal("50.600000"), v2.sha256)
    assert (
        one(db_conn, "SELECT count(*) FROM raw.tcmb_rates WHERE source_sha256 = %s", v1.sha256) == 2
    )
    assert (
        one(db_conn, "SELECT file_sha256 FROM raw.fetch_log WHERE target_date = %s", D) == v2.sha256
    )


def test_bad_file_rolls_back_completely(db_conn, tmp_path, new_bulletin):
    f = land(tmp_path, "tcmb", date(2026, 11, 25), new_bulletin, "xml")  # wrong day
    with pytest.raises(TcmbPayloadError):
        load_bulletin(db_conn, f)
    for t in ("raw.ingest_manifest", "raw.tcmb_rates", "raw.fetch_log"):
        assert one(db_conn, f"SELECT count(*) FROM {t}") == 0


def test_not_published_never_downgrades_a_loaded_day(db_conn, tmp_path, new_bulletin):
    load_bulletin(db_conn, land(tmp_path, "tcmb", D, new_bulletin, "xml"))
    record_not_published(db_conn, D, 404)
    assert one(db_conn, "SELECT status FROM raw.fetch_log WHERE target_date = %s", D) == "loaded"


def test_plan_skips_loaded_and_final_misses_but_rechecks_recent_ones(db_conn):
    db_conn.execute(
        """
        INSERT INTO raw.ingest_manifest VALUES (repeat('a', 64), 'tcmb', 'x', '2026-11-02', 1);
        INSERT INTO raw.fetch_log
            (source, target_date, status, http_status, file_sha256, checked_at)
        VALUES ('tcmb', '2026-11-02', 'loaded', 200, repeat('a', 64), now()),
               ('tcmb', '2026-11-03', 'not_published', 404, NULL, '2026-11-20'),  -- final
               ('tcmb', '2026-11-04', 'not_published', 404, NULL, '2026-11-05');  -- recheck
        """
    )
    db_conn.commit()
    days = plan_days(db_conn, date(2026, 11, 1), date(2026, 11, 8))
    # 1st and 7th/8th are weekend days and skipped
    assert days == [date(2026, 11, 4), date(2026, 11, 5), date(2026, 11, 6)]
    assert len(plan_days(db_conn, date(2026, 11, 1), date(2026, 11, 8), refresh=True)) == 5
    assert len(plan_days(db_conn, date(2026, 11, 1), date(2026, 11, 8), include_weekends=True)) == 6


def test_backfill_end_to_end_twice(db_conn, tmp_path, new_bulletin):
    def handler(req: httpx.Request):
        if req.url.path.endswith("24112026.xml"):
            return httpx.Response(200, content=new_bulletin)
        return httpx.Response(404)

    client = TcmbClient(
        http=httpx.Client(transport=httpx.MockTransport(handler)),
        base_url="https://tcmb.test/kurlar",
        min_interval=0,
    )
    days = plan_days(db_conn, date(2026, 11, 23), D)
    s1 = run_backfill(client, db_conn, tmp_path, days, workers=2)
    assert s1["loaded"] == 1 and s1["not_published"] == 1

    s2 = run_backfill(client, db_conn, tmp_path, days, workers=2)  # --refresh style rerun
    assert s2["unchanged"] == 1 and s2["rows_changed"] == 0
    assert one(db_conn, "SELECT count(*) FROM raw.tcmb_rates") == 3
    assert len(list(tmp_path.rglob("*.xml"))) == 1

    cov = coverage(db_conn)
    assert cov["bulletin_days"] == 1 and cov["not_published_days"] == 1
    assert cov["latest_usd"][0] == D


def test_backfill_keeps_going_past_a_bad_day_and_retries_it(db_conn, tmp_path, new_bulletin):
    def handler(req: httpx.Request):
        if req.url.path.endswith("24112026.xml"):
            return httpx.Response(200, content=new_bulletin)
        return httpx.Response(200, content=new_bulletin)  # 23rd: wrong date inside

    client = TcmbClient(
        http=httpx.Client(transport=httpx.MockTransport(handler)),
        base_url="https://tcmb.test/kurlar",
        min_interval=0,
    )
    failures: list = []
    days = [date(2026, 11, 23), D]
    stats = run_backfill(client, db_conn, tmp_path, days, failures=failures)
    assert stats["failed"] == 1 and stats["loaded"] == 1
    assert failures[0][0] == date(2026, 11, 23) and "expected 2026-11-23" in failures[0][1]
    assert plan_days(db_conn, date(2026, 11, 23), D) == [date(2026, 11, 23)]


def test_empty_bulletin_day_is_recorded_as_not_published(db_conn, tmp_path):
    empty = b'<?xml version="1.0"?><Tarih_Date Tarih="17.08.1999"></Tarih_Date>'
    client = TcmbClient(
        http=httpx.Client(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, content=empty))
        ),
        base_url="https://tcmb.test/kurlar",
        min_interval=0,
    )
    stats = run_backfill(client, db_conn, tmp_path, [date(1999, 8, 17)])
    assert stats["empty_bulletin"] == 1 and stats["failed"] == 0
    status, http = db_conn.execute(
        "SELECT status, http_status FROM raw.fetch_log WHERE target_date = '1999-08-17'"
    ).fetchone()
    assert (status, http) == ("not_published", 200)
    assert one(db_conn, "SELECT count(*) FROM raw.ingest_manifest") == 0


def test_reserved_earlier_bulletin_is_not_published_not_loaded(db_conn, tmp_path, new_bulletin):
    # real case: 28.01.1998 serves the identical bytes of 27.01.1998
    nxt = date(2026, 11, 25)
    client = TcmbClient(
        http=httpx.Client(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, content=new_bulletin))
        ),
        base_url="https://tcmb.test/kurlar",
        min_interval=0,
    )
    stats = run_backfill(client, db_conn, tmp_path, [D, nxt], workers=1)
    assert stats["loaded"] == 1 and stats["stale_bulletin"] == 1 and stats["failed"] == 0
    rows = db_conn.execute(
        "SELECT target_date, status FROM raw.fetch_log ORDER BY target_date"
    ).fetchall()
    assert rows == [(D, "loaded"), (nxt, "not_published")]
    assert coverage(db_conn)["misattributed_days"] == 0


def test_stale_detection_corrects_a_wrongly_loaded_day(db_conn, tmp_path, new_bulletin):
    nxt = date(2026, 11, 25)
    load_bulletin(db_conn, land(tmp_path, "tcmb", D, new_bulletin, "xml"))
    # simulate the pre-fix state: the next day marked loaded, pointing at D's file
    db_conn.execute(
        "INSERT INTO raw.fetch_log (source, target_date, status, http_status, file_sha256) "
        "SELECT 'tcmb', %s, 'loaded', 200, file_sha256 FROM raw.ingest_manifest",
        (nxt,),
    )
    db_conn.commit()
    assert coverage(db_conn)["misattributed_days"] == 1
    record_not_published(db_conn, nxt, 404)  # a plain 404 must not downgrade…
    assert coverage(db_conn)["misattributed_days"] == 1
    record_not_published(db_conn, nxt, 200, correct_loaded=True)  # …proof of staleness does
    assert coverage(db_conn)["misattributed_days"] == 0
