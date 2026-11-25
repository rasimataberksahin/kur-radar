"""Truncgil Finans live board (https://finans.truncgil.com/v4/today.json), no key needed.

One JSON object: ``Update_Date`` (Europe/Istanbul wall time) plus one entry per instrument,
e.g. ``"GRA": {"Type": "Gold", "Name": "GRAMALTIN", "Buying": 5721.38, "Selling": 5722.25,
"Change": -1.05}``. The board has no history, so we poll it and keep every distinct snapshot.

Quirks handled here (each pinned by a test):
- a price published as ``0`` means "no quote" (e.g. ONS, BRENT) → NULL
- some entries have no ``Buying`` key at all (XU100, DBITCOIN) → NULL
- the "Gold" type also holds non-gold instruments (XU100, BRENT, DBITCOIN); everything is
  kept raw and dbt selects the products it models
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import psycopg

from ..http import FetchError, PoliteClient
from ..landing import LandedFile
from ..manifest import loaded_period, record_file, upsert_sql
from . import GoldPayloadError

log = logging.getLogger(__name__)

SOURCE = "truncgil"
URL = "https://finans.truncgil.com/v4/today.json"
ISTANBUL = ZoneInfo("Europe/Istanbul")
REQUIRED_CODE = "GRA"  # gram gold: without it the board is useless for us
MAX_CLOCK_SKEW = timedelta(minutes=10)

_CODE_RE = re.compile(r"^[A-Z0-9_]{1,32}$")
COLUMNS = ["instrument", "name", "buying", "selling", "change_pct"]
_UPSERT = upsert_sql("raw.truncgil_quotes", ["observed_at", "code"], COLUMNS)


@dataclass(frozen=True)
class Snapshot:
    observed_at: datetime
    rows: list[dict[str, Any]]
    ignored_keys: list[str] = field(default_factory=list)


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in pairs:
        if k in out:
            raise GoldPayloadError(f"duplicate JSON key {k!r}")
        out[k] = v
    return out


def _price(code: str, entry: dict, key: str, *, signed: bool = False) -> Decimal | None:
    v = entry.get(key)
    if v is None:
        return None
    if not isinstance(v, Decimal):  # bools, strings, objects
        raise GoldPayloadError(f"{code}.{key} is not a number: {v!r}")
    if not signed:
        if v < 0:
            raise GoldPayloadError(f"{code}.{key} is negative: {v}")
        if v == 0:
            return None  # the board's way of saying "no quote"
    return v


def parse_snapshot(content: bytes, fetched_at: datetime | None = None) -> Snapshot:
    try:
        doc = json.loads(
            content.decode("utf-8-sig"),
            object_pairs_hook=_no_duplicate_keys,
            parse_float=Decimal,
            parse_int=Decimal,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GoldPayloadError(f"not valid JSON: {exc}") from exc
    if not isinstance(doc, dict) or "Update_Date" not in doc:
        raise GoldPayloadError("missing Update_Date")
    try:
        naive = datetime.strptime(str(doc["Update_Date"]), "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise GoldPayloadError(f"bad Update_Date {doc['Update_Date']!r}") from exc
    observed_at = naive.replace(tzinfo=ISTANBUL)
    if fetched_at is not None and observed_at > fetched_at + MAX_CLOCK_SKEW:
        raise GoldPayloadError(f"Update_Date {observed_at} is in the future")

    rows, ignored = [], []
    for code, entry in doc.items():
        if code == "Update_Date":
            continue
        if not isinstance(entry, dict):
            ignored.append(code)  # a new top-level field we don't model yet
            continue
        if not _CODE_RE.match(code):
            raise GoldPayloadError(f"invalid instrument code {code!r}")
        kind = entry.get("Type")
        if not isinstance(kind, str) or not kind:
            raise GoldPayloadError(f"{code} has no Type")
        name = entry.get("Name")
        rows.append(
            {
                "observed_at": observed_at,
                "code": code,
                "instrument": kind,
                "name": name.strip() or None if isinstance(name, str) else None,
                "buying": _price(code, entry, "Buying"),
                "selling": _price(code, entry, "Selling"),
                "change_pct": _price(code, entry, "Change", signed=True),
            }
        )
    gram = next((r for r in rows if r["code"] == REQUIRED_CODE), None)
    if gram is None or gram["buying"] is None or gram["selling"] is None:
        raise GoldPayloadError(f"board has no usable {REQUIRED_CODE} (gram gold) quote")
    return Snapshot(observed_at, rows, ignored)


class TruncgilClient:
    def __init__(self, polite: PoliteClient | None = None, url: str = URL) -> None:
        self._polite = polite or PoliteClient(min_interval=1.0)
        self._url = url

    def fetch(self) -> bytes:
        resp = self._polite.get(self._url, label="truncgil")
        if resp.status_code != 200:
            raise FetchError(f"truncgil: unexpected HTTP {resp.status_code}")
        if not resp.content.lstrip(b"\xef\xbb\xbf \t\r\n").startswith(b"{"):
            raise FetchError("truncgil: HTTP 200 but body is not JSON")
        return resp.content


@dataclass(frozen=True)
class SnapshotLoad:
    sha256: str
    skipped: bool
    observed_at: datetime | None = None
    rows_in_file: int = 0
    rows_changed: int = 0


def load_snapshot(
    conn: psycopg.Connection, landed: LandedFile, fetched_at: datetime | None = None
) -> SnapshotLoad:
    """Load one landed board. Identical bytes are a no-op; otherwise upsert changed rows."""
    with conn.transaction():
        if loaded_period(conn, landed.sha256) is not None:
            return SnapshotLoad(landed.sha256, skipped=True)
        snap = parse_snapshot(landed.path.read_bytes(), fetched_at)
        record_file(conn, landed, len(snap.rows), len(snap.ignored_keys))
        changed = 0
        with conn.cursor() as cur:
            for row in snap.rows:
                cur.execute(_UPSERT, {**row, "source_sha256": landed.sha256})
                changed += cur.rowcount
    if snap.ignored_keys:
        log.warning("truncgil: ignored non-instrument keys %s", snap.ignored_keys)
    return SnapshotLoad(landed.sha256, False, snap.observed_at, len(snap.rows), changed)
