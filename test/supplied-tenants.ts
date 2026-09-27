/**
 * The tenants the supplied fixtures were written for. Suites that assert on those fixtures use
 * only these, so a tenant added to tenants/ and fixtures/manifest.json cannot change their
 * expectations.
 */
export const SUPPLIED_TENANTS: ReadonlySet<string> = new Set(["lumen", "northwind"]);

/** True for a supplied tenant config (`id`) or one of its manifest entries (`tenant`). */
export const isSupplied = (x: { id: string } | { tenant: string }): boolean =>
  SUPPLIED_TENANTS.has("id" in x ? x.id : x.tenant);
