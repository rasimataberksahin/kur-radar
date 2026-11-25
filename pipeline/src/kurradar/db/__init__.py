"""Database helpers: forward-only SQL migrations tracked in raw.schema_migrations."""

from __future__ import annotations

from importlib.resources import files

import psycopg

_TRACKING = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.schema_migrations (
    name        text        PRIMARY KEY,
    applied_at  timestamptz NOT NULL DEFAULT now()
);
"""


def migrate(conn: psycopg.Connection) -> list[str]:
    """Apply pending migrations in name order; returns the names applied now."""
    applied: list[str] = []
    with conn.transaction():
        conn.execute(_TRACKING)
        # serialise concurrent migrators (e.g. two Dagster runs starting together)
        conn.execute("SELECT pg_advisory_xact_lock(hashtext('kurradar.migrate'))")
        done = {r[0] for r in conn.execute("SELECT name FROM raw.schema_migrations")}
        scripts = sorted(
            (p for p in files("kurradar.db.migrations").iterdir() if p.name.endswith(".sql")),
            key=lambda p: p.name,
        )
        for script in scripts:
            if script.name in done:
                continue
            conn.execute(script.read_text(encoding="utf-8"))
            conn.execute("INSERT INTO raw.schema_migrations (name) VALUES (%s)", (script.name,))
            applied.append(script.name)
    return applied
