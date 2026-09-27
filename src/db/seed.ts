import type pg from "pg";
import { loadTenants, type TenantConfig } from "../config/tenants.ts";
import { closePools, getOwnerPool } from "./pool.ts";
import { withTenant } from "./tenant-scope.ts";

/** One ops.value_map row per mapped raw value, flattened from the tenant's source configs. */
function valueMapRows(tenant: TenantConfig): { source: string; column: string; raw: string; canonical: string }[] {
  return Object.entries(tenant.sources).flatMap(([source, mapping]) =>
    Object.entries(mapping?.valueMaps ?? {}).flatMap(([column, values]) =>
      Object.entries(values ?? {}).map(([raw, canonical]) => ({ source, column, raw, canonical })),
    ),
  );
}

/**
 * Inserts or updates one ops.tenant row as the owner, and makes the tenant's
 * ops.value_map rows equal to its config's valueMaps, in the same transaction. The
 * owner is neither a superuser nor BYPASSRLS, so FORCE ROW LEVEL SECURITY applies to
 * it and the writes must run inside that tenant's scope.
 * Returns true when the tenant row or any value map row was inserted, changed or removed.
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

      // Replaces the tenant's value maps, touching only the rows that differ from the
      // config: an unchanged config writes nothing, so the return value stays honest.
      const maps = valueMapRows(tenant);
      const desired = [
        maps.map((m) => m.source),
        maps.map((m) => m.column),
        maps.map((m) => m.raw),
        maps.map((m) => m.canonical),
      ];
      const removed = await client.query(
        `DELETE FROM ops.value_map
         WHERE tenant_id = $1
           AND (source, column_name, raw_value, canonical_value) NOT IN (
             SELECT * FROM unnest($2::text[], $3::text[], $4::text[], $5::text[]))`,
        [tenant.id, ...desired],
      );
      const added = await client.query(
        `INSERT INTO ops.value_map (tenant_id, source, column_name, raw_value, canonical_value)
         SELECT $1, * FROM unnest($2::text[], $3::text[], $4::text[], $5::text[])
         ON CONFLICT (tenant_id, source, column_name, raw_value) DO NOTHING`,
        [tenant.id, ...desired],
      );
      return (rowCount ?? 0) + (removed.rowCount ?? 0) + (added.rowCount ?? 0) > 0;
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
