-- Published reports: what each tenant was told, kept apart from the live marts.
--
-- The marts are views and always reflect everything loaded, so a late arrival changes
-- them silently. A report run snapshots them instead: every (mart, day, dimensions) key
-- whose metrics differ from its latest published version gets a new version, and a key
-- that had been published before also gets an ops.restatement row saying what the
-- client was told, what it is now and which batch files caused the change. Unchanged
-- keys are not republished. All three tables are append-only for the application role.

-- One row per report run that published something. `loaded_batch_file_ids` is the set
-- of loaded files the run's snapshot saw; a later run compares against it to tell which
-- files arrived after a version was published, without relying on commit timing.
CREATE TABLE ops.report_run (
  tenant_id             text NOT NULL REFERENCES ops.tenant (tenant_id),
  id                    bigint GENERATED ALWAYS AS IDENTITY,
  loaded_batch_file_ids bigint[] NOT NULL,
  published_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id)
);

-- Every published version of every mart key, generic across marts. `dims` holds the
-- mart's extra dimensions ('{}' when it has none, e.g. {"platform": "google"}) and
-- `metrics` its metric columns as exact numeric strings. NULL metrics is a tombstone:
-- the key was published before and is no longer in the mart.
CREATE TABLE ops.published_metric (
  tenant_id    text NOT NULL,
  mart         text NOT NULL CHECK (mart ~ '^[a-z][a-z0-9_]*$'),
  day          date NOT NULL,
  dims         jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(dims) = 'object'),
  version      integer NOT NULL CHECK (version > 0),
  metrics      jsonb CHECK (jsonb_typeof(metrics) = 'object'),
  run_id       bigint NOT NULL,
  published_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, mart, day, dims, version),
  FOREIGN KEY (tenant_id, run_id) REFERENCES ops.report_run (tenant_id, id)
);

-- A published number that changed: version `from_version` said `before`, `to_version`
-- says `after` (NULL when the key disappeared; `before` is NULL when a tombstoned key
-- came back). `caused_by` lists the ops.batch_file ids behind the change.
CREATE TABLE ops.restatement (
  tenant_id    text NOT NULL,
  run_id       bigint NOT NULL,
  mart         text NOT NULL,
  day          date NOT NULL,
  dims         jsonb NOT NULL,
  from_version integer NOT NULL,
  to_version   integer NOT NULL CHECK (to_version = from_version + 1),
  before       jsonb,
  after        jsonb,
  caused_by    bigint[] NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, mart, day, dims, to_version),
  FOREIGN KEY (tenant_id, run_id) REFERENCES ops.report_run (tenant_id, id),
  FOREIGN KEY (tenant_id, mart, day, dims, from_version)
    REFERENCES ops.published_metric (tenant_id, mart, day, dims, version),
  FOREIGN KEY (tenant_id, mart, day, dims, to_version)
    REFERENCES ops.published_metric (tenant_id, mart, day, dims, version)
);

ALTER TABLE ops.report_run ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.report_run FORCE ROW LEVEL SECURITY;
ALTER TABLE ops.published_metric ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.published_metric FORCE ROW LEVEL SECURITY;
ALTER TABLE ops.restatement ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.restatement FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON ops.report_run
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

CREATE POLICY tenant_isolation ON ops.published_metric
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

CREATE POLICY tenant_isolation ON ops.restatement
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- The numbers the client was told: the latest published version of each key, minus
-- tombstones. security_invoker, like every other view, so RLS applies to the reader.
CREATE VIEW marts.reported_metric WITH (security_invoker = true) AS
SELECT tenant_id, mart, day, dims, version, metrics, run_id, published_at
FROM (
  SELECT DISTINCT ON (tenant_id, mart, day, dims) *
  FROM ops.published_metric
  ORDER BY tenant_id, mart, day, dims, version DESC
) latest
WHERE metrics IS NOT NULL;

-- Append-only: no UPDATE or DELETE, so a published number or restatement cannot be rewritten.
GRANT SELECT, INSERT ON ops.report_run, ops.published_metric, ops.restatement TO pipeline_app;
GRANT SELECT ON marts.reported_metric TO pipeline_app;
