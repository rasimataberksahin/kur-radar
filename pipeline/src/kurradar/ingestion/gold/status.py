"""Coverage and sanity report for the gold raw layer."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import psycopg

from . import GRAMS_PER_TROY_OUNCE


def coverage(conn: psycopg.Connection) -> dict[str, Any]:
    snaps = conn.execute(
        """
        SELECT count(DISTINCT observed_at), min(observed_at), max(observed_at),
               count(DISTINCT code), now() - max(observed_at)
        FROM raw.truncgil_quotes
        """
    ).fetchone()
    gram = conn.execute(
        """
        SELECT observed_at, buying, selling FROM raw.truncgil_quotes
        WHERE code = 'GRA' ORDER BY observed_at DESC LIMIT 1
        """
    ).fetchone()
    ounce = conn.execute(
        """
        SELECT count(*), min(price_date), max(price_date),
               count(*) FILTER (WHERE NOT is_final),
               count(*) FILTER (WHERE NOT ohlc_consistent)
        FROM raw.gold_ounce_daily
        """
    ).fetchone()
    # sanity: gram gold implied by the last final ounce close and that day's TCMB USD selling
    implied = conn.execute(
        """
        SELECT o.price_date, o.close, r.forex_selling
        FROM raw.gold_ounce_daily o
        JOIN raw.tcmb_rates r ON r.currency_code = 'USD' AND r.rate_date = o.price_date
        WHERE o.is_final AND r.forex_selling IS NOT NULL
        ORDER BY o.price_date DESC LIMIT 1
        """
    ).fetchone()
    out: dict[str, Any] = {
        "truncgil_snapshots": snaps[0],
        "truncgil_first": snaps[1],
        "truncgil_last": snaps[2],
        "truncgil_instruments": snaps[3],
        "truncgil_board_age": snaps[4],
        "latest_gram_gold": gram,
        "ounce_bars": ounce[0],
        "ounce_first": ounce[1],
        "ounce_last": ounce[2],
        "ounce_not_final": ounce[3],
        "ounce_ohlc_inconsistent": ounce[4],
    }
    if implied:
        day, close, usd = implied
        out["implied_gram_gold"] = (
            day,
            (close * usd / Decimal(str(GRAMS_PER_TROY_OUNCE))).quantize(Decimal("0.01")),
        )
    return out
