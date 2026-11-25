"""kurradar CLI.

kurradar migrate
kurradar tcmb backfill --start 1996-01-01 [--end YYYY-MM-DD] [--refresh] [--workers 4]
kurradar tcmb reload          # re-verify + load every landed file (hash-checked)
kurradar tcmb status
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from zoneinfo import ZoneInfo

import psycopg

from .config import load_settings
from .db import migrate
from .ingestion.landing import iter_landed
from .ingestion.tcmb import SOURCE
from .ingestion.tcmb.backfill import plan_days, run_backfill
from .ingestion.tcmb.client import TcmbClient
from .ingestion.tcmb.loader import load_bulletin, record_not_published
from .ingestion.tcmb.parser import NoBulletinError, TcmbPayloadError
from .ingestion.tcmb.status import coverage

ISTANBUL = ZoneInfo("Europe/Istanbul")


def _today() -> date:
    return datetime.now(ISTANBUL).date()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="kurradar")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="apply database migrations")

    tcmb = sub.add_parser("tcmb", help="TCMB daily exchange-rate bulletin")
    tsub = tcmb.add_subparsers(dest="tcmb_cmd", required=True)
    bf = tsub.add_parser("backfill", help="fetch, land and load a date range")
    bf.add_argument("--start", type=date.fromisoformat, required=True)
    bf.add_argument("--end", type=date.fromisoformat, default=None, help="default: today")
    bf.add_argument("--include-weekends", action="store_true")
    bf.add_argument("--refresh", action="store_true", help="re-request already loaded days")
    bf.add_argument("--workers", type=int, default=4)
    tsub.add_parser("reload", help="load every landed file again (already-loaded ones skip)")
    tsub.add_parser("status", help="coverage report")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request is noise
    s = load_settings()
    if not s.database_url:
        print("DATABASE_URL is not set (copy .env.example to .env)", file=sys.stderr)
        return 2

    with psycopg.connect(s.database_url) as conn:
        applied = migrate(conn)
        if applied:
            print("applied migrations:", ", ".join(applied))
        if args.cmd == "migrate":
            return 0

        if args.tcmb_cmd == "backfill":
            end = min(args.end or _today(), _today())
            days = plan_days(conn, args.start, end, args.include_weekends, args.refresh)
            print(f"{len(days)} day(s) to request between {args.start} and {end}")
            client = TcmbClient(min_interval=s.tcmb_min_interval)
            failures: list = []
            stats = run_backfill(
                client, conn, s.raw_data_dir, days, workers=args.workers, failures=failures
            )
            print(dict(stats))
            for day, reason in failures[:20]:
                print(f"  failed {day}: {reason}")
            if failures:
                return 1
        elif args.tcmb_cmd == "reload":
            changed, bad = 0, 0
            for landed in iter_landed(s.raw_data_dir, SOURCE):
                try:
                    changed += load_bulletin(conn, landed).rows_changed
                except NoBulletinError:
                    record_not_published(conn, landed.period_date, 200, correct_loaded=True)
                except TcmbPayloadError as exc:
                    bad += 1
                    print(f"  invalid {landed.path.name}: {exc}")
            print(f"reload done, {changed} row(s) changed, {bad} invalid file(s)")
            if bad:
                return 1
        elif args.tcmb_cmd == "status":
            for k, v in coverage(conn).items():
                print(f"{k:28} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
