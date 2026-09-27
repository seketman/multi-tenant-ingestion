-- Tenant value maps: raw value -> canonical value for one column of one source,
-- synced from the tenant config by seeding. Mapping is data, so the transformation
-- layers join this table instead of branching on tenant ids.
CREATE TABLE ops.value_map (
  tenant_id       text NOT NULL REFERENCES ops.tenant (tenant_id),
  source          text NOT NULL CHECK (source IN ('orders', 'email_events', 'ad_spend', 'refunds')),
  column_name     text NOT NULL,
  raw_value       text NOT NULL,
  canonical_value text NOT NULL,
  PRIMARY KEY (tenant_id, source, column_name, raw_value)
);

ALTER TABLE ops.value_map ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.value_map FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON ops.value_map
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

GRANT SELECT ON ops.value_map TO pipeline_app;
