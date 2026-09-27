import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import type pg from "pg";
import { closePools, getOwnerPool } from "./pool.ts";
import { seedTenants } from "./seed.ts";

const DEFAULT_MIGRATIONS_DIR = fileURLToPath(new URL("../../migrations/", import.meta.url));
const DEFAULT_TRACKING_TABLE = "schema_migrations";
const IDENTIFIER = /^[a-z_][a-z0-9_]{0,62}$/;
const MIGRATION_FILE = /^\d{3}_[a-z0-9_]+\.sql$/;
// Arbitrary constant: serializes concurrent migration runs against one database.
const MIGRATION_LOCK_KEY = 482_901;

const sha256 = (text: string): string => createHash("sha256").update(text).digest("hex");
const toError = (value: unknown): Error => (value instanceof Error ? value : new Error(String(value)));

export interface MigrateOptions {
  /** Directory holding the NNN_name.sql files. */
  dir?: string;
  /** Tracking table name in the public schema; tests use their own so they never touch the real records. */
  trackingTable?: string;
  pool?: pg.Pool;
}

/**
 * Applies every migration file not yet recorded, in file-name order. Each file runs in
 * its own transaction together with its tracking row, so a failure leaves nothing behind.
 * Returns the files applied by this run.
 */
export async function migrate({
  dir = DEFAULT_MIGRATIONS_DIR,
  trackingTable = DEFAULT_TRACKING_TABLE,
  pool = getOwnerPool(),
}: MigrateOptions = {}): Promise<string[]> {
  if (!IDENTIFIER.test(trackingTable)) {
    throw new Error(`Invalid tracking table name "${trackingTable}"`);
  }
  const table = `public.${trackingTable}`;
  const files = (await readdir(dir)).filter((f) => MIGRATION_FILE.test(f)).sort();
  const client = await pool.connect();
  const applied: string[] = [];
  let broken: Error | undefined;
  try {
    await client.query("SELECT pg_advisory_lock($1)", [MIGRATION_LOCK_KEY]);
    await client.query(`
      CREATE TABLE IF NOT EXISTS ${table} (
        filename   text PRIMARY KEY,
        sha256     char(64) NOT NULL,
        applied_at timestamptz NOT NULL DEFAULT now()
      )`);
    const { rows } = await client.query<{ filename: string; sha256: string }>(
      `SELECT filename, sha256 FROM ${table}`,
    );
    const done = new Map(rows.map((r) => [r.filename, r.sha256]));

    for (const file of files) {
      const sql = await readFile(join(dir, file), "utf8");
      const checksum = sha256(sql);
      const previous = done.get(file);
      if (previous !== undefined) {
        if (previous !== checksum) {
          throw new Error(`Migration ${file} was edited after it was applied; add a new migration instead`);
        }
        continue;
      }
      try {
        await client.query("BEGIN");
        await client.query(sql);
        await client.query(`INSERT INTO ${table} (filename, sha256) VALUES ($1, $2)`, [file, checksum]);
        await client.query("COMMIT");
      } catch (error) {
        try {
          await client.query("ROLLBACK");
        } catch (rollbackError) {
          // A connection that cannot roll back must not go back into the pool; the
          // migration error below stays the one reported.
          broken = toError(rollbackError);
        }
        throw new Error(`Migration ${file} failed: ${(error as Error).message}`, { cause: error });
      }
      applied.push(file);
    }
  } finally {
    try {
      await client.query("SELECT pg_advisory_unlock($1)", [MIGRATION_LOCK_KEY]);
    } catch (unlockError) {
      // The session may still hold the lock, so it is discarded rather than pooled.
      broken ??= toError(unlockError);
    }
    client.release(broken);
  }
  return applied;
}

if (import.meta.main) {
  try {
    const applied = await migrate();
    console.log(applied.length === 0 ? "migrations: up to date" : `migrations: applied ${applied.join(", ")}`);
    const changed = await seedTenants();
    console.log(`tenants: ${changed.length === 0 ? "up to date" : `upserted ${changed.join(", ")}`}`);
  } catch (error) {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  } finally {
    await closePools();
  }
}
