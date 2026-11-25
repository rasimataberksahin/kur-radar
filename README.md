# KurRadar

A desktop app and data platform for Turkish lira exchange rates and gold:
TCMB's daily bulletins since 1996 and gold prices are loaded into a reproducible pipeline,
modelled with dbt, orchestrated with Dagster, and fed to ML models that forecast
**rises and falls**. The models' performance is monitored over time, and everything is shown
in a .NET + React desktop app.

**No account, API key or registration is needed. Every source is public.**

| Stage | Status | Delivers |
|---|---|---|
| 0. Skeleton | ✅ | Monorepo, Docker Compose Postgres, CI (ruff + pytest + Postgres) |
| 1. TCMB ingestion | ✅ | Full archive since 1996, SHA-256 landing, idempotent load, 404/holiday handling |
| 2. Gold ingestion | — | Truncgil 15-min snapshots (gram/çeyrek/…), historical ounce gold |
| 3. dbt | — | Staging → marts, redenomination, cross rates, feature table, gold spread |
| 4. Dagster | — | Daily (after TCMB's ~15:30 bulletin) and 15-min schedules, backfills, asset checks |
| 5. ML | — | Direction (up/down probability), magnitude (quantiles + conformal), volatility regime; walk-forward backtests; MLflow |
| 6. Monitoring | — | Forecast-vs-actual store, hit-rate / calibration / coverage trends, drift alerts |
| 7. .NET 10 API | — | ASP.NET Core, OpenAPI, xUnit + Testcontainers |
| 8. Desktop app | — | Electron + React + TypeScript, TR/EN, light/dark, Windows installer |

```
pipeline/            Python package `kurradar` (ingestion now; Dagster + ML later)
  src/kurradar/db/   forward-only SQL migrations
  src/kurradar/ingestion/   landing + per-source adapters (tcmb/)
  tests/             unit + Postgres integration tests, trimmed real-format fixtures
docker/              Postgres init (creates the kurradar_test database)
data/raw/            landed source files — git-ignored, never committed
```

---

## Stage 0 — Skeleton

```powershell
copy .env.example .env
docker compose up -d --wait          # Postgres 16 on 127.0.0.1:55460 (+ kurradar_test)
python -m venv .venv
.venv\Scripts\pip install -e "pipeline[dev]"
.venv\Scripts\pytest pipeline -q -rs
```

CI (`.github/workflows/ci.yml`) runs `ruff check`, `ruff format --check` and the full test
suite against a Postgres service container. The .NET and desktop jobs arrive with their
stages.

## Stage 1 — TCMB exchange-rate ingestion

**Source:** `https://www.tcmb.gov.tr/kurlar/YYYYMM/DDMMYYYY.xml`, one bulletin per business
day with forex and banknote buy/sell prices, cross rates and about 20 currencies. The archive
starts around mid-1996 and has gaps.

### Flow

```
plan_days ──► fetch (4 threads, shared rate limit) ──► land (SHA-256) ──► load (1 txn/day)
   ▲                                                                         │
   └──────────────────────────── raw.fetch_log ◄─────────────────────────────┘
```

1. **Plan.** Candidate days are weekdays, or every day with `--include-weekends`. A day is skipped
   when `raw.fetch_log` says it was **loaded**, or when it was **not published** and we checked
   at least 7 days later. Younger misses are asked again, which covers late publication and
   today's bulletin before 15:30.
2. **Fetch.** A 404 is a normal answer ("no bulletin that day"). 429, 5xx and network errors get
   exponential backoff. An HTML page served with 200 (for example a maintenance page) is refused
   and never landed. A shared rate limit (`TCMB_MIN_INTERVAL_SECONDS`) keeps the crawl polite.
3. **Land.** The bytes are stored untouched as
   `data/raw/tcmb/YYYY/MM/tcmb_YYYY-MM-DD_<sha12>.xml`. The same bytes always map to the same
   file, so nothing is rewritten. A revised bulletin lands as a second file next to the original.
4. **Load.** Each day loads in one transaction:
   - If the file's SHA-256 is already in `raw.ingest_manifest`, the load is a **no-op**.
   - Otherwise the bulletin is parsed and validated, the manifest row is written, and rows are
     upserted per `(rate_date, currency_code)` with `WHERE … IS DISTINCT FROM`. Only currencies
     whose values changed are rewritten, and `source_sha256` records which file each row came
     from.
   - Any validation error rolls back the whole day.
   - A bad day doesn't stop a backfill. It is reported as `failed`, left out of `fetch_log`, and
     retried on the next run.

### Validation (`parser.py`)

- XML is parsed with `defusedxml`, so DTDs and entity expansion are refused.
- The root must be `<Tarih_Date>`, and its `Tarih` must equal the requested day.
- Currency codes must be three letters with no duplicates. `Unit` must be a positive integer
  (except legacy reference-only entries, see below), and `Isim` (the Turkish name) is optional.
- Values must be numeric and non-negative. Empty values become `NULL`.
- Both encodings are handled: **ISO-8859-9** before 2005 and UTF-8 (with or without a BOM) after.

### Quirks found in the real archive

The first full backfill surfaced these. Each one is handled explicitly and pinned by a test:

| Quirk | Where | Handling |
|---|---|---|
| Legacy euro-zone currencies (DEM, FRF, IEP…) listed with an empty `Unit` and only a fixed `CrossRateEuro` | 2002–2008, about 1,700 days | Not a TRY rate, so the entry is skipped. The count goes to `ingest_manifest.skipped_count` (20,555 entries in total). |
| XDR published with an empty `<Isim>` | 2014–2016 | `name_tr` is optional (migration `002`) |
| Footnote marker `(*)` instead of a value | EUR on 31.12.2001 | Becomes `NULL` |
| Well-formed bulletin with no currencies | 17.08.1999 (Marmara earthquake) | `EmptyBulletinError`, recorded as `not_published` (HTTP 200) |
| A day re-serves the **identical bytes** of the previous day's bulletin | 28.01.1998 returns 27.01.1998 | `StaleBulletinError` is raised on the fresh-load path *and* the "SHA already loaded" path, and the day is recorded as `not_published`. `status` checks the invariant `misattributed_days = 0`. |

Result of the full backfill: **7,628 bulletin days from 16.04.1996 to today**, 435 days not
published, 165,454 rate rows across 43 currencies (≈84 MB of raw XML). No weekday in range is
left unasked, and no day is misattributed. A rerun requests only today, and `reload` changes 0 rows.

### Raw tables

| Table | Grain | Notes |
|---|---|---|
| `raw.tcmb_rates` | day × currency | Values **exactly as published**. Before 2005 they are in old lira (USD 1,336,300). The ÷1,000,000 redenomination and `unit` (JPY = 100) normalisation happen in dbt (stage 3). |
| `raw.ingest_manifest` | landed file | SHA-256, path, day, row count |
| `raw.fetch_log` | source × day | `loaded` / `not_published`, HTTP status, last check |
| `raw.schema_migrations` | migration | Forward-only migrations, serialised with an advisory lock |

### Commands (run from `pipeline/` or the repo root)

```powershell
.venv\Scripts\kurradar migrate
.venv\Scripts\kurradar tcmb backfill --start 1996-01-01      # full history (~40 min first time)
.venv\Scripts\kurradar tcmb backfill --start 2026-11-01      # incremental; already-known days skip
.venv\Scripts\kurradar tcmb reload                           # re-verify hashes + reload landed files
.venv\Scripts\kurradar tcmb status                           # coverage report
```

### Tests

| File | What it proves |
|---|---|
| `test_tcmb_parser.py` | Both eras and encodings, BOM, blank values and padding, stale vs future dates, malformed-input cases, XML bomb refused, every archive quirk above |
| `test_tcmb_client.py` | URL format, 404 → not published, retry with backoff, give-up, HTML-with-200 refused, rate limit spacing |
| `test_landing.py` | Content-addressed naming, idempotent landing, revisions, tamper detection |
| `test_tcmb_db.py` (Postgres) | Migrations idempotent, reload is a no-op, a revision rewrites only changed currencies, full rollback on bad files, no downgrade of loaded days, planner rules, end-to-end backfill twice, a bad day doesn't stop the run, empty and re-served bulletins become `not_published`, a wrongly loaded day is corrected only with proof |
