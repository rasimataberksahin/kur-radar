"""One-call ingestion jobs (fetch → land → load), used by the CLI and later by Dagster."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg

from ..landing import iter_landed, land
from . import GoldPayloadError
from .ounce import OunceLoad, OunceProvider, load_ounce_history
from .truncgil import ISTANBUL, SOURCE, SnapshotLoad, TruncgilClient, load_snapshot

# re-ask this many days before the newest stored bar, so the bar that was still trading
# last time becomes final and late corrections are picked up
OUNCE_OVERLAP_DAYS = 10


def ingest_snapshot(
    conn: psycopg.Connection, raw_dir: Path, client: TruncgilClient, now: datetime | None = None
) -> SnapshotLoad:
    now = now or datetime.now(ISTANBUL)
    content = client.fetch()
    landed = land(raw_dir, SOURCE, now.astimezone(ISTANBUL).date(), content, "json")
    return load_snapshot(conn, landed, fetched_at=now)


def ingest_ounce(
    conn: psycopg.Connection,
    raw_dir: Path,
    provider: OunceProvider,
    full: bool = False,
    today: date | None = None,
) -> OunceLoad:
    today = today or datetime.now(ISTANBUL).date()
    start = None
    if not full:
        last = conn.execute(
            "SELECT max(price_date) FROM raw.gold_ounce_daily WHERE provider = %s AND symbol = %s",
            (provider.provider, provider.symbol),
        ).fetchone()[0]
        start = last - timedelta(days=OUNCE_OVERLAP_DAYS) if last else None
    landed = land(raw_dir, provider.source, today, provider.fetch(start), "json")
    return load_ounce_history(conn, provider, landed)


def reload_all(conn: psycopg.Connection, raw_dir: Path, provider: OunceProvider) -> dict[str, Any]:
    """Re-verify hashes and load every landed gold file again (loaded ones skip)."""
    out: dict[str, Any] = {"rows_changed": 0, "invalid": []}
    for landed in iter_landed(raw_dir, SOURCE):
        try:
            out["rows_changed"] += load_snapshot(conn, landed).rows_changed
        except GoldPayloadError as exc:
            out["invalid"].append((landed.path.name, str(exc)))
    for landed in iter_landed(raw_dir, provider.source):
        try:
            out["rows_changed"] += load_ounce_history(conn, provider, landed).rows_changed
        except GoldPayloadError as exc:
            out["invalid"].append((landed.path.name, str(exc)))
    return out
