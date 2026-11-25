-- Stage 2: gold. Values exactly as published; product selection, spreads and the
-- historical gram-gold series (ounce × USD/TRY ÷ 31.1034768) are built in dbt.

-- Truncgil's live Turkish market board: one row per instrument per snapshot.
-- Every instrument is kept (gold coins, silver, currencies, ...); dbt picks the ones it needs.
-- A snapshot is identified by the board's own Update_Date (Europe/Istanbul), so polling a
-- board that hasn't moved adds nothing.
CREATE TABLE raw.truncgil_quotes (
    observed_at    timestamptz   NOT NULL,
    code           text          NOT NULL,
    instrument     text          NOT NULL,  -- the board's "Type": Gold, Currency, Platinum, ...
    name           text,
    buying         numeric(20,6),           -- NULL when missing or published as 0
    selling        numeric(20,6),
    change_pct     numeric(10,4),
    source_sha256  char(64)      NOT NULL REFERENCES raw.ingest_manifest (file_sha256),
    loaded_at      timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (observed_at, code)
);
CREATE INDEX ix_truncgil_quotes_code_time ON raw.truncgil_quotes (code, observed_at DESC);

-- Daily ounce-gold history (USD per troy ounce) from a swappable provider.
-- is_final is false for the session still trading when the file was fetched.
CREATE TABLE raw.gold_ounce_daily (
    provider         text          NOT NULL,
    symbol           text          NOT NULL,
    price_date       date          NOT NULL,  -- trading date in the exchange's time zone
    open             numeric(16,6),
    high             numeric(16,6),
    low              numeric(16,6),
    close            numeric(16,6) NOT NULL,
    volume           bigint,
    ohlc_consistent  boolean       NOT NULL,  -- low <= open, close <= high
    is_final         boolean       NOT NULL,
    source_sha256    char(64)      NOT NULL REFERENCES raw.ingest_manifest (file_sha256),
    loaded_at        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, symbol, price_date)
);
