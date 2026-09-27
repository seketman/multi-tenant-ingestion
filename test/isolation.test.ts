import { randomBytes } from "node:crypto";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { appDatabaseUrl, closePools, getAppPool, getOwnerPool, withTenant } from "../src/db/index.ts";
import { upsertTenant } from "../src/db/seed.ts";

// Unique tenants per run keep the test re-runnable and independent of seeded data.
const suffix = randomBytes(4).toString("hex");
const tenantA = `test_a_${suffix}`;
const tenantB = `test_b_${suffix}`;
// Own tenants for the replay-guard tests, so their rows never affect the counts above.
const tenantC = `test_c_${suffix}`;
const tenantD = `test_d_${suffix}`;
const sha = (seed: string) => seed.repeat(64).slice(0, 64);

const insertBatchFile = (
  client: pg.PoolClient,
  tenantId: string,
  hash: string,
  status: "loaded" | "quarantined" = "loaded",
  source: "orders" | "refunds" = "orders",
) =>
  client.query<{ id: string }>(
    `INSERT INTO ops.batch_file (tenant_id, source, batch_no, path, sha256, status, row_count)
     VALUES ($1, $4, 1, 'test/' || $4 || '/batch_01.csv', $2, $3, 1)
     RETURNING id`,
    [tenantId, hash, status, source],
  );

const countRows = async (client: pg.ClientBase) => {
  const { rows } = await client.query<{ tenants: number; files: number; records: number }>(
    `SELECT (SELECT count(*) FROM ops.tenant)::int      AS tenants,
            (SELECT count(*) FROM ops.batch_file)::int  AS files,
            (SELECT count(*) FROM raw.record)::int      AS records`,
  );
  return rows[0];
};

// "new row violates row-level security policy": code 42501 is shared with plain
// permission errors, so the message is matched too.
const RLS_VIOLATION = { code: "42501", message: expect.stringMatching(/row-level security/) };

describe("tenant isolation enforced by row-level security", () => {
  beforeAll(async () => {
    for (const id of [tenantA, tenantB, tenantC, tenantD]) {
      await upsertTenant({ id, displayName: id, currency: "USD", fixturesDir: "unused", sources: {} });
    }
    await withTenant(tenantA, async (client) => {
      const { rows } = await insertBatchFile(client, tenantA, sha("a"));
      await client.query(
        `INSERT INTO raw.record (tenant_id, batch_file_id, line_no, payload) VALUES ($1, $2, 1, $3)`,
        [tenantA, rows[0]?.id, { order_id: "A-1" }],
      );
    });
  });

  afterAll(async () => {
    const owner = getOwnerPool();
    for (const id of [tenantA, tenantB, tenantC, tenantD]) {
      await withTenant(
        id,
        async (client) => {
          await client.query("DELETE FROM raw.record WHERE tenant_id = $1", [id]);
          await client.query("DELETE FROM ops.batch_file WHERE tenant_id = $1", [id]);
          await client.query("DELETE FROM ops.value_map WHERE tenant_id = $1", [id]);
          await client.query("DELETE FROM ops.tenant WHERE tenant_id = $1", [id]);
        },
        owner,
      );
    }
    await closePools();
  });

  it("runs as a role that is neither superuser nor BYPASSRLS", async () => {
    const { rows } = await getAppPool().query<{ rolsuper: boolean; rolbypassrls: boolean }>(
      "SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user",
    );
    expect(rows[0]).toEqual({ rolsuper: false, rolbypassrls: false });
  });

  it("migrates and seeds as an owner that is neither superuser nor BYPASSRLS", async () => {
    const { rows } = await getOwnerPool().query<{ rolsuper: boolean; rolbypassrls: boolean }>(
      "SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user",
    );
    expect(rows[0]).toEqual({ rolsuper: false, rolbypassrls: false });
  });

  it("applies FORCE ROW LEVEL SECURITY to the owner as well", async () => {
    const owner = getOwnerPool();
    expect(await withTenant(tenantA, countRows, owner)).toEqual({ tenants: 1, files: 1, records: 1 });
    // Owner scoped to tenant B: tenant A's rows are invisible, even when asked for by id.
    expect(await withTenant(tenantB, countRows, owner)).toEqual({ tenants: 1, files: 0, records: 0 });
    const explicit = await withTenant(
      tenantB,
      (client) => client.query("SELECT 1 FROM raw.record WHERE tenant_id = $1", [tenantA]),
      owner,
    );
    expect(explicit.rowCount).toBe(0);

    const unscoped = await owner.connect();
    try {
      expect(await countRows(unscoped)).toEqual({ tenants: 0, files: 0, records: 0 });
    } finally {
      unscoped.release();
    }
  });

  it("shows tenant A its own rows", async () => {
    const counts = await withTenant(tenantA, countRows);
    expect(counts).toEqual({ tenants: 1, files: 1, records: 1 });
  });

  it("hides tenant A rows from tenant B", async () => {
    const counts = await withTenant(tenantB, countRows);
    expect(counts).toEqual({ tenants: 1, files: 0, records: 0 });

    const explicit = await withTenant(tenantB, (client) =>
      client.query("SELECT 1 FROM ops.batch_file WHERE tenant_id = $1", [tenantA]),
    );
    expect(explicit.rowCount).toBe(0);
  });

  it("rejects writing tenant B rows while scoped to tenant A", async () => {
    const attempt = withTenant(tenantA, (client) => insertBatchFile(client, tenantB, sha("b")));
    await expect(attempt).rejects.toMatchObject(RLS_VIOLATION);

    const recordAttempt = withTenant(tenantA, async (client) => {
      const { rows } = await client.query<{ id: string }>("SELECT id FROM ops.batch_file LIMIT 1");
      await client.query(
        `INSERT INTO raw.record (tenant_id, batch_file_id, line_no, payload) VALUES ($1, $2, 2, '{}')`,
        [tenantB, rows[0]?.id],
      );
    });
    await expect(recordAttempt).rejects.toMatchObject(RLS_VIOLATION);
  });

  it("returns zero rows and rejects writes when no tenant is set", async () => {
    const client = new pg.Client({ connectionString: appDatabaseUrl() });
    await client.connect();
    try {
      expect(await countRows(client)).toEqual({ tenants: 0, files: 0, records: 0 });
      await expect(
        client.query(
          `INSERT INTO ops.batch_file (tenant_id, source, batch_no, path, sha256, status)
           VALUES ($1, 'orders', 1, 'x', $2, 'loaded')`,
          [tenantA, sha("c")],
        ),
      ).rejects.toMatchObject(RLS_VIOLATION);
    } finally {
      await client.end();
    }
  });

  it("does not leak the tenant setting into the next use of a pooled connection", async () => {
    const pool = new pg.Pool({ connectionString: appDatabaseUrl(), max: 1 });
    try {
      await withTenant(tenantA, countRows, pool);
      const client = await pool.connect();
      try {
        expect(await countRows(client)).toEqual({ tenants: 0, files: 0, records: 0 });
      } finally {
        client.release();
      }
    } finally {
      await pool.end();
    }
  });

  it("does not let the app role write the tenant registry", async () => {
    const attempt = withTenant(tenantA, (client) =>
      client.query("UPDATE ops.tenant SET display_name = 'x' WHERE tenant_id = $1", [tenantA]),
    );
    await expect(attempt).rejects.toMatchObject({ code: "42501", message: expect.stringMatching(/permission denied/) });
  });

  it("guards replays of loaded bytes only, so quarantined files can be retried", async () => {
    const hash = sha("d");
    await withTenant(tenantC, async (client) => {
      await insertBatchFile(client, tenantC, hash, "quarantined");
      await insertBatchFile(client, tenantC, hash, "quarantined");
      await insertBatchFile(client, tenantC, hash, "loaded");
    });
    const replay = withTenant(tenantC, (client) => insertBatchFile(client, tenantC, hash, "loaded"));
    await expect(replay).rejects.toMatchObject({ code: "23505" });
  });

  it("scopes the replay guard to one tenant and one source", async () => {
    const shared = sha("e");
    await withTenant(tenantC, (client) => insertBatchFile(client, tenantC, shared));
    await withTenant(tenantD, (client) => insertBatchFile(client, tenantD, shared));

    const perSource = sha("f");
    await withTenant(tenantD, async (client) => {
      await insertBatchFile(client, tenantD, perSource, "loaded", "orders");
      await insertBatchFile(client, tenantD, perSource, "loaded", "refunds");
    });

    const loaded = await withTenant(tenantD, (client) =>
      client.query<{ source: string; sha256: string }>(
        "SELECT source, sha256 FROM ops.batch_file WHERE status = 'loaded' ORDER BY sha256, source",
      ),
    );
    expect(loaded.rows).toEqual([
      { source: "orders", sha256: shared },
      { source: "orders", sha256: perSource },
      { source: "refunds", sha256: perSource },
    ]);
  });

  it.each(["", "   "])("rejects the blank tenant id %j", async (tenantId) => {
    await expect(withTenant(tenantId, countRows)).rejects.toThrow("withTenant requires a non-empty tenant id");
  });

  it("discards a connection that cannot roll back and reports the original error", async () => {
    const pool = new pg.Pool({ connectionString: appDatabaseUrl(), max: 1 });
    // A terminated backend makes the client emit 'error' outside any query; without a
    // listener that event would crash the test run.
    pool.on("error", () => undefined);
    pool.on("connect", (client) => client.on("error", () => undefined));
    try {
      const attempt = withTenant(
        tenantA,
        (client) => client.query("SELECT pg_terminate_backend(pg_backend_pid())"),
        pool,
      );
      // 57P01 admin_shutdown: the error fn saw, not the failed ROLLBACK that follows it.
      await expect(attempt).rejects.toMatchObject({ code: "57P01" });
      expect(pool.totalCount).toBe(0);

      const client = await pool.connect();
      try {
        const { rows } = await client.query<{ tenant: string | null }>(
          "SELECT current_setting('app.tenant_id', true) AS tenant",
        );
        expect([null, ""]).toContain(rows[0]?.tenant);
      } finally {
        client.release();
      }
    } finally {
      await pool.end();
    }
  });
});
