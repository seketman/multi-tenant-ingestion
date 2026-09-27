-- Runs once, as the bootstrap superuser, when the data volume is first created.
-- The superuser is used for nothing else: migrations and seeding connect as
-- pipeline_owner, which is not a superuser and cannot bypass row-level security,
-- so FORCE ROW LEVEL SECURITY genuinely applies to it.
-- CREATEROLE lets it create and manage pipeline_app (migration 001).
CREATE ROLE pipeline_owner LOGIN PASSWORD 'pipeline_owner'
  NOSUPERUSER NOCREATEDB CREATEROLE NOREPLICATION NOBYPASSRLS;

-- Owning the database lets it create schemas and revoke/grant database privileges.
ALTER DATABASE pipeline OWNER TO pipeline_owner;
