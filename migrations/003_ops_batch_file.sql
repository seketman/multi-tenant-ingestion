-- Attempt ledger: one row per load attempt of a batch file, loaded or quarantined.
-- The replay guard (partial unique index below) covers only successfully loaded
-- bytes, so a quarantined file can be retried with a new row after a config fix,
-- while the same bytes can never be loaded twice for the same tenant and source.
CREATE TABLE ops.batch_file (
  tenant_id  text NOT NULL REFERENCES ops.tenant (tenant_id),
  id         bigint GENERATED ALWAYS AS IDENTITY,
  source     text NOT NULL CHECK (source IN ('orders', 'email_events', 'ad_spend', 'refunds')),
  batch_no   integer NOT NULL CHECK (batch_no > 0),
  path       text NOT NULL,
  sha256     char(64) NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  status     text NOT NULL CHECK (status IN ('loaded', 'quarantined')),
  row_count  integer NOT NULL DEFAULT 0 CHECK (row_count >= 0),
  detail     jsonb,
  loaded_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id)
);

CREATE UNIQUE INDEX batch_file_loaded_once
  ON ops.batch_file (tenant_id, source, sha256)
  WHERE status = 'loaded';

ALTER TABLE ops.batch_file ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.batch_file FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON ops.batch_file
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

GRANT SELECT, INSERT ON ops.batch_file TO pipeline_app;
