CREATE TABLE ops.tenant (
  tenant_id    text PRIMARY KEY CHECK (tenant_id ~ '^[a-z][a-z0-9_]{1,62}$'),
  display_name text NOT NULL,
  currency     char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  created_at   timestamptz NOT NULL DEFAULT now()
);

-- Even the tenant registry is scoped: a session only sees its own tenant row.
-- FORCE applies the policy to the table owner too (pipeline_owner is neither a
-- superuser nor BYPASSRLS), so seeding writes each row inside that tenant's scope.
ALTER TABLE ops.tenant ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.tenant FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON ops.tenant
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

GRANT SELECT ON ops.tenant TO pipeline_app;
