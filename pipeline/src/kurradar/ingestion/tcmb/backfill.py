"""Plan and run TCMB backfills: fetch (parallel, rate-limited) → land → load (serial)."""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

import psycopg

from ..landing import land
from . import SOURCE
from .client import FetchResult, TcmbClient, TcmbFetchError
from .loader import load_bulletin, record_not_published
from .parser import NoBulletinError, StaleBulletinError, TcmbPayloadError

log = logging.getLogger(__name__)

# A "not published" answer is trusted as final once we checked at least this many
# days after the bulletin date; younger misses are re-asked (late publication).
NOT_PUBLISHED_FINAL_AFTER_DAYS = 7


def candidate_days(start: date, end: date, include_weekends: bool = False) -> list[date]:
    if end < start:
        raise ValueError("end must be on or after start")
    days, d = [], start
    while d <= end:
        if include_weekends or d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def plan_days(
    conn: psycopg.Connection,
    start: date,
    end: date,
    include_weekends: bool = False,
    refresh: bool = False,
) -> list[date]:
    """Days in ``start..end`` that still need a request."""
    days = candidate_days(start, end, include_weekends)
    if refresh or not days:
        return days
    rows = conn.execute(
        """
        SELECT target_date, status, checked_at::date
        FROM raw.fetch_log
        WHERE source = %s AND target_date BETWEEN %s AND %s
        """,
        (SOURCE, start, end),
    ).fetchall()
    done = set()
    for target, status, checked in rows:
        final_miss = (checked - target).days >= NOT_PUBLISHED_FINAL_AFTER_DAYS
        if status == "loaded" or (status == "not_published" and final_miss):
            done.add(target)
    return [d for d in days if d not in done]


def run_backfill(
    client: TcmbClient,
    conn: psycopg.Connection,
    raw_dir: Path,
    days: Iterable[date],
    workers: int = 4,
    progress_every: int = 250,
    failures: list[tuple[date, str]] | None = None,
) -> Counter[str]:
    """Returns counts; per-day failures are collected (and logged) instead of aborting."""
    stats: Counter[str] = Counter()
    failures = failures if failures is not None else []
    days = list(days)

    def safe_fetch(day: date) -> FetchResult | Exception:
        try:
            return client.fetch(day)
        except TcmbFetchError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        # map() keeps date order, so loads (and logs) progress chronologically
        results = zip(days, pool.map(safe_fetch, days), strict=True)
        for i, (day, result) in enumerate(results, start=1):
            try:
                if isinstance(result, Exception):
                    raise result
                _handle(conn, raw_dir, result, stats)
            except (TcmbFetchError, TcmbPayloadError) as exc:
                # nothing is written to fetch_log, so the day is retried on the next run
                stats["failed"] += 1
                failures.append((day, str(exc)))
                log.warning("tcmb %s failed: %s", day, exc)
            if i % progress_every == 0:
                log.info("tcmb backfill %d/%d (%s) %s", i, len(days), day, dict(stats))
    return stats


def _handle(conn: psycopg.Connection, raw_dir: Path, result: FetchResult, stats: Counter) -> None:
    if not result.published:
        record_not_published(conn, result.day, result.http_status)
        stats["not_published"] += 1
        return
    assert result.content is not None
    landed = land(raw_dir, SOURCE, result.day, result.content, "xml")
    try:
        loaded = load_bulletin(conn, landed)
    except NoBulletinError as exc:
        # empty, or an earlier day's bulletin served again: no bulletin for this day
        record_not_published(conn, result.day, result.http_status, correct_loaded=True)
        stats["stale_bulletin" if isinstance(exc, StaleBulletinError) else "empty_bulletin"] += 1
        return
    stats["unchanged" if loaded.skipped else "loaded"] += 1
    stats["rows_changed"] += loaded.rows_changed
