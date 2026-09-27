-- Application login role. It owns nothing and cannot bypass row-level security,
-- so every tenant-scoped read and write it makes is filtered by the policies below.
--
-- Migrations run as pipeline_owner, a CREATEROLE non-superuser. PostgreSQL 16+
-- lets such a role set attributes when it creates a role, but not change
-- SUPERUSER, CREATEDB, REPLICATION or BYPASSRLS on an existing one. So the
-- attributes are fixed at creation, and a pre-existing pipeline_app is checked
-- instead of silently trusted.
DO $$
DECLARE
  existing pg_roles%ROWTYPE;
BEGIN
  SELECT * INTO existing FROM pg_roles WHERE rolname = 'pipeline_app';
  IF NOT FOUND THEN
    CREATE ROLE pipeline_app LOGIN PASSWORD 'pipeline_app'
      NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT;
  ELSIF existing.rolsuper OR existing.rolbypassrls OR existing.rolcreatedb
     OR existing.rolcreaterole OR existing.rolreplication OR existing.rolinherit THEN
    RAISE EXCEPTION 'role pipeline_app already exists with elevated attributes; drop or fix it as a superuser';
  END IF;
END
$$;

-- ops: control plane (tenants, batch file ledger).
-- raw: loaded payloads, verbatim.
-- staging / marts: reserved for the transformation layers.
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- The owner owns the database (docker/initdb), and through pg_database_owner the
-- public schema, which is what allows these revokes without superuser.
REVOKE ALL ON SCHEMA public FROM PUBLIC;
DO $$
BEGIN
  EXECUTE format('REVOKE ALL ON DATABASE %I FROM PUBLIC', current_database());
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO pipeline_app', current_database());
END
$$;
GRANT USAGE ON SCHEMA ops, raw TO pipeline_app;
