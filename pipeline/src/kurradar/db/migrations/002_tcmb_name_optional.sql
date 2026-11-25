-- 2014–2016 bulletins publish XDR (SDR) with an empty <Isim>; the name is informational.
ALTER TABLE raw.tcmb_rates ALTER COLUMN name_tr DROP NOT NULL;

-- Legacy euro-zone currencies listed without a TRY rate (2002–2008) are skipped by the
-- parser; keep how many were skipped per file for auditability.
ALTER TABLE raw.ingest_manifest ADD COLUMN skipped_count integer NOT NULL DEFAULT 0;
