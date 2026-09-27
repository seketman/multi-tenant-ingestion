import { randomBytes } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { closePools, getOwnerPool, ownerDatabaseUrl } from "../src/db/index.ts";
import { migrate } from "../src/db/migrate.ts";

// A throwaway schema and tracking table per run: the real migrations' records are never touched.
const suffix = randomBytes(4).toString("hex");
const schema = `migtest_${suffix}`;
const trackingTable = `schema_migrations_test_${suffix}`;

const relationExists = async (name: string): Promise<boolean> => {
  const { rows } = await getOwnerPool().query<{ oid: string | null }>("SELECT to_regclass($1) AS oid", [name]);
  return rows[0]?.oid != null;
};

const recorded = async (): Promise<string[]> => {
  const { rows } = await getOwnerPool().query<{ filename: string }>(
    `SELECT filename FROM public.${trackingTable} ORDER BY filename`,
  );
  return rows.map((r) => r.filename);
};

describe("migration runner", () => {
  let dir: string;
  const run = () => migrate({ dir, trackingTable });
  const write = (file: string, sql: string) => writeFile(join(dir, file), sql);

  beforeAll(async () => {
    dir = await mkdtemp(join(tmpdir(), "migrations-"));
    await write("001_schema.sql", `CREATE SCHEMA ${schema}; CREATE TABLE ${schema}.a (id int);`);
  });

  afterAll(async () => {
    const owner = getOwnerPool();
    await owner.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    await owner.query(`DROP TABLE IF EXISTS public.${trackingTable}`);
    await rm(dir, { recursive: true, force: true });
    await closePools();
  });

  it("applies pending files and is a no-op on re-run", async () => {
    expect(await run()).toEqual(["001_schema.sql"]);
    expect(await run()).toEqual([]);
    expect(await recorded()).toEqual(["001_schema.sql"]);
  });

  it("rolls a failing migration back fully and applies it once fixed", async () => {
    // The table would be created before the error; the transaction must discard it.
    await write("002_broken.sql", `CREATE TABLE ${schema}.b (id int); SELECT 1 / 0;`);
    await expect(run()).rejects.toThrow(/Migration 002_broken\.sql failed: division by zero/);
    expect(await relationExists(`${schema}.b`)).toBe(false);
    expect(await recorded()).toEqual(["001_schema.sql"]);

    await write("002_broken.sql", `CREATE TABLE ${schema}.b (id int);`);
    expect(await run()).toEqual(["002_broken.sql"]);
    expect(await relationExists(`${schema}.b`)).toBe(true);
  });

  it("rejects a migration edited after it was applied", async () => {
    await write("001_schema.sql", `CREATE SCHEMA ${schema}; CREATE TABLE ${schema}.a (id bigint);`);
    await expect(run()).rejects.toThrow(/001_schema\.sql was edited after it was applied/);
  });
});

// A terminated backend makes the client emit 'error' outside any query; without a
// listener that event would crash the test run.
const ownerPoolWithErrorListeners = (max: number): pg.Pool => {
  const pool = new pg.Pool({ connectionString: ownerDatabaseUrl(), max });
  pool.on("error", () => undefined);
  pool.on("connect", (client) => client.on("error", () => undefined));
  return pool;
};

describe("migration runner on a connection that dies mid-migration", () => {
  const deadTrackingTable = `schema_migrations_dead_${suffix}`;
  let dir: string;
  let pool: pg.Pool;

  beforeAll(async () => {
    dir = await mkdtemp(join(tmpdir(), "migrations-dead-"));
    await writeFile(join(dir, "001_die.sql"), "SELECT pg_terminate_backend(pg_backend_pid());");
    pool = ownerPoolWithErrorListeners(1);
  });

  afterAll(async () => {
    await pool.query(`DROP TABLE IF EXISTS public.${deadTrackingTable}`);
    await pool.end();
    await rm(dir, { recursive: true, force: true });
  });

  it("reports the migration error and discards the broken connection", async () => {
    await expect(migrate({ dir, trackingTable: deadTrackingTable, pool })).rejects.toThrow(
      /^Migration 001_die\.sql failed: terminating connection due to administrator command/,
    );
    expect(pool.totalCount).toBe(0);

    const client = await pool.connect();
    try {
      const { rows } = await client.query<{ ok: number }>("SELECT 1 AS ok");
      expect(rows).toEqual([{ ok: 1 }]);
    } finally {
      client.release();
    }
  });
});

describe("migration runner under concurrent runs", () => {
  const concurrentSchema = `migtest_concurrent_${suffix}`;
  const concurrentTrackingTable = `schema_migrations_concurrent_${suffix}`;
  const files = ["001_slow.sql", "002_table.sql"] as const;
  let dir: string;
  let pool: pg.Pool;

  beforeAll(async () => {
    dir = await mkdtemp(join(tmpdir(), "migrations-concurrent-"));
    // The sleep keeps the first run inside its migration while the second one starts.
    await writeFile(join(dir, files[0]), `CREATE SCHEMA ${concurrentSchema}; SELECT pg_sleep(0.3);`);
    await writeFile(join(dir, files[1]), `CREATE TABLE ${concurrentSchema}.t (id int);`);
    // One connection per run, so neither waits on the pool instead of on the lock.
    pool = ownerPoolWithErrorListeners(2);
  });

  afterAll(async () => {
    await pool.query(`DROP SCHEMA IF EXISTS ${concurrentSchema} CASCADE`);
    await pool.query(`DROP TABLE IF EXISTS public.${concurrentTrackingTable}`);
    await pool.end();
    await rm(dir, { recursive: true, force: true });
  });

  it("applies every file exactly once across overlapping runs", async () => {
    const run = () => migrate({ dir, trackingTable: concurrentTrackingTable, pool });
    const results = await Promise.all([run(), run()]);

    expect(results.flat().sort()).toEqual(files);
    const { rows } = await pool.query<{ filename: string; n: number }>(
      `SELECT filename, count(*)::int AS n FROM public.${concurrentTrackingTable} GROUP BY filename ORDER BY filename`,
    );
    expect(rows).toEqual(files.map((filename) => ({ filename, n: 1 })));
  });
});
