-- Every line of a loaded batch file, verbatim as jsonb.
-- The composite foreign key keeps a record and its batch file in the same tenant.
CREATE TABLE raw.record (
  tenant_id     text NOT NULL,
  batch_file_id bigint NOT NULL,
  line_no       integer NOT NULL CHECK (line_no > 0),
  payload       jsonb NOT NULL,
  loaded_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, batch_file_id, line_no),
  FOREIGN KEY (tenant_id, batch_file_id) REFERENCES ops.batch_file (tenant_id, id)
);

ALTER TABLE raw.record ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw.record FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON raw.record
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

GRANT SELECT, INSERT ON raw.record TO pipeline_app;
