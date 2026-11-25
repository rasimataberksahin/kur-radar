"""raw.ingest_manifest helpers shared by every source's loader."""

from __future__ import annotations

from datetime import date

import psycopg

from .landing import LandedFile


def loaded_period(conn: psycopg.Connection, sha256: str) -> date | None:
    """period_date of an already-loaded file with these bytes, or None if never loaded."""
    row = conn.execute(
        "SELECT period_date FROM raw.ingest_manifest WHERE file_sha256 = %s", (sha256,)
    ).fetchone()
    return row[0] if row else None


def record_file(
    conn: psycopg.Connection, landed: LandedFile, row_count: int, skipped_count: int = 0
) -> None:
    conn.execute(
        "INSERT INTO raw.ingest_manifest "
        "(file_sha256, source, file_path, period_date, row_count, skipped_count) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (
            landed.sha256,
            landed.source,
            landed.path.as_posix(),
            landed.period_date,
            row_count,
            skipped_count,
        ),
    )


def upsert_sql(table: str, key: list[str], columns: list[str], extra_guard: str = "") -> str:
    """INSERT … ON CONFLICT that rewrites a row only when one of ``columns`` changed."""
    all_cols = [*key, *columns, "source_sha256"]
    changed = " OR ".join(f"t.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in columns)
    guard = f"({changed})" + (f" AND ({extra_guard})" if extra_guard else "")
    return (
        f"INSERT INTO {table} AS t ({', '.join(all_cols)}) "
        f"VALUES ({', '.join(f'%({c})s' for c in all_cols)}) "
        f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET "
        + ", ".join(f"{c} = EXCLUDED.{c}" for c in [*columns, "source_sha256"])
        + f", loaded_at = now() WHERE {guard}"
    )
