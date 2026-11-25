-- Raw layer: values exactly as the source published them. Normalisation
-- (e.g. the 2005 redenomination, per-unit rates) happens later in dbt.
CREATE SCHEMA IF NOT EXISTS raw;

-- Every landed file that was loaded, keyed by the SHA-256 of its bytes.
CREATE TABLE raw.ingest_manifest (
    file_sha256  char(64)    PRIMARY KEY,
    source       text        NOT NULL,
    file_path    text        NOT NULL,
    period_date  date        NOT NULL,
    row_count    integer     NOT NULL,
    loaded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_ingest_manifest_source_period ON raw.ingest_manifest (source, period_date);

-- One row per (source, day) we asked for: was it published, and when did we last look?
-- Lets backfills skip known non-publication days (weekends, holidays) without re-asking.
CREATE TABLE raw.fetch_log (
    source       text        NOT NULL,
    target_date  date        NOT NULL,
    status       text        NOT NULL CHECK (status IN ('loaded', 'not_published')),
    http_status  smallint    NOT NULL,
    file_sha256  char(64)    REFERENCES raw.ingest_manifest (file_sha256),
    checked_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, target_date)
);

-- TCMB "gösterge niteliğindeki" daily bulletin, one row per currency per bulletin day.
CREATE TABLE raw.tcmb_rates (
    rate_date         date          NOT NULL,
    currency_code     char(3)       NOT NULL,
    unit              integer       NOT NULL,
    name_tr           text          NOT NULL,
    name_en           text,
    forex_buying      numeric(20,6),
    forex_selling     numeric(20,6),
    banknote_buying   numeric(20,6),
    banknote_selling  numeric(20,6),
    cross_rate_usd    numeric(20,6),
    cross_rate_other  numeric(20,6),
    bulletin_no       text,
    source_sha256     char(64)      NOT NULL REFERENCES raw.ingest_manifest (file_sha256),
    loaded_at         timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (rate_date, currency_code)
);
