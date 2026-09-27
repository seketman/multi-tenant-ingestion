import type pg from "pg";
import { loadTenants, type TenantConfig } from "../config/tenants.ts";
import { closePools, getOwnerPool } from "./pool.ts";
import { withTenant } from "./tenant-scope.ts";

/**
 * Inserts or updates one ops.tenant row as the owner. The owner is neither a
 * superuser nor BYPASSRLS, so FORCE ROW LEVEL SECURITY applies to it and the
 * write must run inside that tenant's scope.
 * Returns true when a row was inserted or changed.
 */
export async function upsertTenant(tenant: TenantConfig, pool: pg.Pool = getOwnerPool()): Promise<boolean> {
  return withTenant(
    tenant.id,
    async (client) => {
      const { rowCount } = await client.query(
        `INSERT INTO ops.tenant (tenant_id, display_name, currency)
         VALUES ($1, $2, $3)
         ON CONFLICT (tenant_id) DO UPDATE
           SET display_name = excluded.display_name, currency = excluded.currency
           WHERE (ops.tenant.display_name, ops.tenant.currency)
             IS DISTINCT FROM (excluded.display_name, excluded.currency)`,
        [tenant.id, tenant.displayName, tenant.currency],
      );
      return (rowCount ?? 0) > 0;
    },
    pool,
  );
}

/** Upserts every configured tenant. Returns the ids that were inserted or changed. */
export async function seedTenants(): Promise<string[]> {
  const changed: string[] = [];
  for (const tenant of await loadTenants()) {
    if (await upsertTenant(tenant)) changed.push(tenant.id);
  }
  return changed;
}

if (import.meta.main) {
  try {
    const changed = await seedTenants();
    console.log(`tenants: ${changed.length === 0 ? "up to date" : `upserted ${changed.join(", ")}`}`);
  } catch (error) {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  } finally {
    await closePools();
  }
}
