import type pg from "pg";
import { getAppPool } from "./pool.ts";

/**
 * Runs `fn` inside a transaction scoped to one tenant.
 *
 * `app.tenant_id` is set with `is_local = true`, so it lives only until COMMIT or
 * ROLLBACK and never leaks to the next user of a pooled connection. The RLS
 * policies read this setting; without it, tenant tables return no rows and
 * reject every write.
 */
export async function withTenant<T>(
  tenantId: string,
  fn: (client: pg.PoolClient) => Promise<T>,
  pool: pg.Pool = getAppPool(),
): Promise<T> {
  if (tenantId.trim() === "") {
    throw new Error("withTenant requires a non-empty tenant id");
  }

  const client = await pool.connect();
  let broken: Error | undefined;
  try {
    await client.query("BEGIN");
    await client.query("SELECT set_config('app.tenant_id', $1, true)", [tenantId]);
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch (rollbackError) {
      // A connection that cannot roll back must not go back into the pool.
      broken = rollbackError instanceof Error ? rollbackError : new Error(String(rollbackError));
    }
    throw error;
  } finally {
    client.release(broken);
  }
}
