import pg from "pg";
import { appDatabaseUrl, ownerDatabaseUrl } from "./env.ts";

let ownerPool: pg.Pool | undefined;
let appPool: pg.Pool | undefined;

/**
 * Owner connection: migrations and seeding only. Never used to serve tenant reads.
 * Not a superuser: tenant tables use FORCE ROW LEVEL SECURITY, so owner writes go through withTenant.
 */
export function getOwnerPool(): pg.Pool {
  ownerPool ??= new pg.Pool({ connectionString: ownerDatabaseUrl(), max: 2 });
  return ownerPool;
}

/** Application connection: subject to row-level security on every tenant table. */
export function getAppPool(): pg.Pool {
  appPool ??= new pg.Pool({ connectionString: appDatabaseUrl() });
  return appPool;
}

export async function closePools(): Promise<void> {
  const pools = [ownerPool, appPool];
  ownerPool = undefined;
  appPool = undefined;
  await Promise.all(pools.map((pool) => pool?.end()));
}
