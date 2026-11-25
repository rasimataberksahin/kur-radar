"""Idempotent TCMB loader: SHA-256 manifest + per-(day, currency) upsert in one transaction."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import psycopg

from ..landing import LandedFile
from . import SOURCE
from .parser import COLUMNS, StaleBulletinError, parse_bulletin

log = logging.getLogger(__name__)

_KEY = ["rate_date", "currency_code"]
_ALL = [*_KEY, *COLUMNS, "source_sha256"]
_UPSERT = (
    f"INSERT INTO raw.tcmb_rates AS t ({', '.join(_ALL)}) "
    f"VALUES ({', '.join(f'%({c})s' for c in _ALL)}) "
    f"ON CONFLICT (rate_date, currency_code) DO UPDATE SET "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in [*COLUMNS, "source_sha256"])
    + ", loaded_at = now() WHERE "
    + " OR ".join(f"t.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in COLUMNS)
)
_LOG_LOADED = """
INSERT INTO raw.fetch_log (source, target_date, status, http_status, file_sha256, checked_at)
VALUES (%s, %s, 'loaded', 200, %s, now())
ON CONFLICT (source, target_date) DO UPDATE
SET status = 'loaded', http_status = 200, file_sha256 = EXCLUDED.file_sha256, checked_at = now()
"""


@dataclass(frozen=True)
class LoadResult:
    rate_date: date
    sha256: str
    skipped: bool
    rows_in_file: int = 0
    rows_changed: int = 0


def load_bulletin(conn: psycopg.Connection, landed: LandedFile) -> LoadResult:
    """Load one landed bulletin. Already-loaded bytes (same SHA-256) are a no-op.

    A revised bulletin for the same day only rewrites currencies whose values changed;
    ``source_sha256`` records which file every row currently comes from.
    """
    with conn.transaction():
        seen = conn.execute(
            "SELECT period_date FROM raw.ingest_manifest WHERE file_sha256 = %s", (landed.sha256,)
        ).fetchone()
        if seen:
            if seen[0] != landed.period_date:
                # identical bytes were already loaded for another day: a re-served bulletin
                raise StaleBulletinError(
                    f"{landed.period_date} returned the bulletin already loaded for {seen[0]}"
                )
            conn.execute(_LOG_LOADED, (SOURCE, landed.period_date, landed.sha256))
            return LoadResult(landed.period_date, landed.sha256, skipped=True)

        bulletin = parse_bulletin(landed.path.read_bytes(), expected_date=landed.period_date)
        conn.execute(
            "INSERT INTO raw.ingest_manifest "
            "(file_sha256, source, file_path, period_date, row_count, skipped_count) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (
                landed.sha256,
                SOURCE,
                landed.path.as_posix(),
                landed.period_date,
                len(bulletin.rows),
                len(bulletin.reference_only),
            ),
        )
        changed = 0
        with conn.cursor() as cur:
            for row in bulletin.rows:
                cur.execute(_UPSERT, {**row, "source_sha256": landed.sha256})
                changed += cur.rowcount
        conn.execute(_LOG_LOADED, (SOURCE, landed.period_date, landed.sha256))
    return LoadResult(landed.period_date, landed.sha256, False, len(bulletin.rows), changed)


def record_not_published(
    conn: psycopg.Connection, day: date, http_status: int, *, correct_loaded: bool = False
) -> None:
    """Remember that ``day`` had no bulletin.

    A transient 404 never downgrades a loaded day. ``correct_loaded=True`` is for proof
    that the earlier "loaded" was wrong (the day only ever served another day's bulletin).
    """
    guard = "" if correct_loaded else "WHERE raw.fetch_log.status = 'not_published'"
    with conn.transaction():
        conn.execute(
            f"""
            INSERT INTO raw.fetch_log (source, target_date, status, http_status, checked_at)
            VALUES (%s, %s, 'not_published', %s, now())
            ON CONFLICT (source, target_date) DO UPDATE
            SET status = 'not_published', http_status = EXCLUDED.http_status,
                file_sha256 = NULL, checked_at = now()
            {guard}
            """,
            (SOURCE, day, http_status),
        )
