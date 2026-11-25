"""Coverage report for the TCMB raw layer."""

from __future__ import annotations

from typing import Any

import psycopg

from . import SOURCE


def coverage(conn: psycopg.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT count(*) FILTER (WHERE status = 'loaded'),
               count(*) FILTER (WHERE status = 'not_published'),
               min(target_date) FILTER (WHERE status = 'loaded'),
               max(target_date) FILTER (WHERE status = 'loaded')
        FROM raw.fetch_log WHERE source = %s
        """,
        (SOURCE,),
    ).fetchone()
    loaded, missing, first, last = row if row else (0, 0, None, None)

    # weekdays inside the loaded range we have never asked about at all
    unasked = 0
    if first and last:
        unasked = conn.execute(
            """
            SELECT count(*)
            FROM generate_series(%s::date, %s::date, interval '1 day') AS g(d)
            WHERE extract(isodow FROM g.d) < 6
              AND NOT EXISTS (
                SELECT 1 FROM raw.fetch_log f
                WHERE f.source = %s AND f.target_date = g.d::date)
            """,
            (first, last, SOURCE),
        ).fetchone()[0]

    stats = conn.execute(
        "SELECT count(*), count(DISTINCT currency_code) FROM raw.tcmb_rates"
    ).fetchone()
    usd = conn.execute(
        """
        SELECT rate_date, forex_buying, forex_selling FROM raw.tcmb_rates
        WHERE currency_code = 'USD' ORDER BY rate_date DESC LIMIT 1
        """
    ).fetchone()
    # invariant: a "loaded" day must point at a file that belongs to that same day
    misattributed = conn.execute(
        """
        SELECT count(*) FROM raw.fetch_log f
        JOIN raw.ingest_manifest m USING (file_sha256)
        WHERE f.source = %s AND f.status = 'loaded' AND f.target_date <> m.period_date
        """,
        (SOURCE,),
    ).fetchone()[0]
    return {
        "bulletin_days": loaded,
        "not_published_days": missing,
        "first_day": first,
        "last_day": last,
        "unasked_weekdays_in_range": unasked,
        "misattributed_days": misattributed,
        "rate_rows": stats[0],
        "currencies": stats[1],
        "latest_usd": usd,
    }
