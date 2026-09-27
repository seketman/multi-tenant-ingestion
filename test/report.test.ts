import { randomBytes } from "node:crypto";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { loadTenants, type TenantConfig } from "../src/config/tenants.ts";
import { appDatabaseUrl, closePools, getOwnerPool, withTenant } from "../src/db/index.ts";
import { upsertTenant } from "../src/db/seed.ts";
import { InjectedFailure, loadBatches } from "../src/ingest/loader.ts";
import { loadManifest, type Manifest } from "../src/ingest/manifest.ts";
import { MART_NAMES, MARTS, type PublishResult, publishReports } from "../src/report/publish.ts";

// Unique tenants per run keep the test re-runnable and away from the seeded 'northwind' and 'lumen'.
const suffix = randomBytes(4).toString("hex");
// The tenants the supplied fixtures were written for. Suites that assert on those fixtures use only
// these, so a tenant added to tenants/ and fixtures/manifest.json cannot change their expectations.
const SUPPLIED = new Set(["lumen", "northwind"]);
const isSupplied = (x: { id: string } | { tenant: string }) => SUPPLIED.has("id" in x ? x.id : x.tenant);
const northwind = `northwind_${suffix}`;
const lumen = `lumen_${suffix}`;
const createdTenants: string[] = [];

const TABLES = ["ops.report_run", "ops.published_metric", "ops.restatement", "marts.reported_metric"] as const;

const register = async (tenant: TenantConfig): Promise<void> => {
  await upsertTenant(tenant);
  if (!createdTenants.includes(tenant.id)) createdTenants.push(tenant.id);
};

/** Runs one query scoped to `tenantId` as the application role and returns its rows. */
const query = async <T extends pg.QueryResultRow>(tenantId: string, sql: string, params: unknown[] = []): Promise<T[]> =>
  withTenant(tenantId, async (client) => (await client.query<T>(sql, params)).rows);

afterAll(async () => {
  const owner = getOwnerPool();
  for (const id of createdTenants) {
    await withTenant(
      id,
      async (client) => {
        await client.query("DELETE FROM ops.restatement WHERE tenant_id = $1", [id]);
        await client.query("DELETE FROM ops.published_metric WHERE tenant_id = $1", [id]);
        await client.query("DELETE FROM ops.report_run WHERE tenant_id = $1", [id]);
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

/**
 * The live marts as `mart|day|dims` -> metrics, every value as the text Postgres prints,
 * read straight from the mart views: the expectation the published numbers must match.
 */
async function liveMarts(tenantId: string): Promise<Map<string, string>> {
  const live = new Map<string, string>();
  for (const mart of MART_NAMES) {
    const { dims, metrics } = MARTS[mart];
    const rows = await query<{ day: string; dims: string; metrics: string }>(
      tenantId,
      `SELECT to_char(day, 'YYYY-MM-DD') AS day,
              jsonb_build_object(${dims.map((d) => `'${d}', ${d}`).join(", ")})::text AS dims,
              jsonb_build_object(${metrics.map((m) => `'${m}', ${m}::text`).join(", ")})::text AS metrics
       FROM marts.${mart}`,
    );
    for (const r of rows) live.set(`${mart}|${r.day}|${r.dims}`, r.metrics);
  }
  return live;
}

interface RestatementRow {
  key: string;
  mart: string;
  day: string;
  from_version: number;
  to_version: number;
  before: string | null;
  after: string | null;
  caused_by: string[];
}

const restatements = (tenantId: string): Promise<RestatementRow[]> =>
  query<RestatementRow>(
    tenantId,
    `SELECT mart || '|' || to_char(day, 'YYYY-MM-DD') || '|' || dims::text AS key, mart,
            to_char(day, 'YYYY-MM-DD') AS day, from_version, to_version,
            before::text AS before, after::text AS after, caused_by::text[] AS caused_by
     FROM ops.restatement ORDER BY mart, day, dims`,
  );

const summaryOf = (results: PublishResult[], tenant: string) => {
  const result = results.find((r) => r.tenant === tenant);
  if (result?.status !== "published") throw new Error(`${tenant} did not publish: ${JSON.stringify(result)}`);
  return result;
};

describe("publishing reports on the supplied fixtures, with batch 5 arriving late", () => {
  let tenants: TenantConfig[];
  let manifest: Manifest;
  let first: PublishResult[];
  const before = new Map<string, Map<string, string>>();
  const after = new Map<string, Map<string, string>>();
  let second: PublishResult[];

  beforeAll(async () => {
    tenants = (await loadTenants()).filter(isSupplied).map((t) => ({ ...t, id: `${t.id}_${suffix}` }));
    const real = await loadManifest("fixtures/manifest.json");
    manifest = { batches: real.batches.filter(isSupplied).map((b) => ({ ...b, tenant: `${b.tenant}_${suffix}` })) };
    for (const tenant of tenants) await register(tenant);

    await loadBatches({ tenants, manifest: { batches: manifest.batches.filter((b) => b.batch <= 4) } });
    first = await publishReports({ tenants });
    for (const t of tenants) before.set(t.id, await liveMarts(t.id));

    await loadBatches({ tenants, manifest: { batches: manifest.batches.filter((b) => b.batch === 5) } });
    for (const t of tenants) after.set(t.id, await liveMarts(t.id));
    second = await publishReports({ tenants });
  }, 60_000);

  const batch5Files = async (tenantId: string) =>
    new Map(
      (
        await query<{ id: string; source: string }>(
          tenantId,
          "SELECT id::text AS id, source FROM ops.batch_file WHERE status = 'loaded' AND batch_no = 5",
        )
      ).map((r) => [r.source, r.id]),
    );

  it.each(["northwind", "lumen"])("publishes %s's first run as version 1 of every key, with no restatement", async (name) => {
    const tenantId = `${name}_${suffix}`;
    const live = before.get(tenantId);
    expect(summaryOf(first, tenantId)).toEqual({
      tenant: tenantId,
      status: "published",
      runId: expect.any(String),
      published: live?.size,
      restated: 0,
      unchanged: 0,
    });
    const [firstRun] = await query<{ versions: number; max_version: number }>(
      tenantId,
      `SELECT count(*)::int AS versions, max(version) AS max_version FROM ops.published_metric
       WHERE run_id = $1`,
      [summaryOf(first, tenantId).runId],
    );
    expect(firstRun).toEqual({ versions: live?.size, max_version: 1 });
  });

  it.each(["northwind", "lumen"])("restates exactly the %s keys batch 5 changed, from and to the live numbers", async (name) => {
    const tenantId = `${name}_${suffix}`;
    const was = before.get(tenantId) ?? new Map<string, string>();
    const now = after.get(tenantId) ?? new Map<string, string>();
    const changed = [...was].filter(([key, metrics]) => now.get(key) !== metrics).map(([key]) => key);
    const added = [...now.keys()].filter((key) => !was.has(key));
    expect(changed.length).toBeGreaterThan(0);

    expect(summaryOf(second, tenantId)).toEqual({
      tenant: tenantId,
      status: "published",
      runId: expect.any(String),
      published: changed.length + added.length,
      restated: changed.length,
      unchanged: now.size - changed.length - added.length,
    });

    const files = new Set((await batch5Files(tenantId)).values());
    const rows = await restatements(tenantId);
    expect(rows.map((r) => r.key).sort()).toEqual([...changed].sort());
    for (const row of rows) {
      expect({ key: row.key, before: row.before, after: row.after, versions: [row.from_version, row.to_version] }).toEqual({
        key: row.key,
        before: was.get(row.key),
        after: now.get(row.key) ?? null,
        versions: [1, 2],
      });
      expect(row.caused_by.length).toBeGreaterThan(0);
      expect(row.caused_by.every((id) => files.has(id))).toBe(true);
    }
  });

  it("restates northwind's email engagement for the late events of 2026-01-12..17 only, caused by email batch 5", async () => {
    const emailFile = (await batch5Files(northwind)).get("email_events");
    const email = (await restatements(northwind)).filter((r) => r.mart === "daily_email_engagement");
    expect(email.map((r) => [r.day, r.caused_by])).toEqual(
      ["2026-01-12", "2026-01-13", "2026-01-14", "2026-01-15", "2026-01-16", "2026-01-17"].map((day) => [day, [emailFile]]),
    );
    // A day batch 5 did not touch keeps its single published version.
    const [untouched] = await query<{ versions: number }>(
      northwind,
      `SELECT count(*)::int AS versions FROM ops.published_metric
       WHERE mart = 'daily_email_engagement' AND day = '2026-01-06'`,
    );
    expect(untouched?.versions).toBe(1);
  });

  it("shows the latest version in marts.reported_metric and keeps the earlier one in the history", async () => {
    const [restated] = await restatements(northwind);
    if (restated === undefined) throw new Error("expected a restatement");
    const reported = await query<{ version: number; metrics: string }>(
      northwind,
      `SELECT version, metrics::text AS metrics FROM marts.reported_metric
       WHERE mart || '|' || to_char(day, 'YYYY-MM-DD') || '|' || dims::text = $1`,
      [restated.key],
    );
    expect(reported).toEqual([{ version: 2, metrics: restated.after }]);
    const history = await query<{ version: number; metrics: string }>(
      northwind,
      `SELECT version, metrics::text AS metrics FROM ops.published_metric
       WHERE mart || '|' || to_char(day, 'YYYY-MM-DD') || '|' || dims::text = $1 ORDER BY version`,
      [restated.key],
    );
    expect(history).toEqual([
      { version: 1, metrics: restated.before },
      { version: 2, metrics: restated.after },
    ]);
    // One reported row per live key, each equal to the live mart.
    const all = await query<{ key: string; metrics: string }>(
      northwind,
      `SELECT mart || '|' || to_char(day, 'YYYY-MM-DD') || '|' || dims::text AS key, metrics::text AS metrics
       FROM marts.reported_metric`,
    );
    expect(new Map(all.map((r) => [r.key, r.metrics]))).toEqual(after.get(northwind));
  });

  it("publishes nothing on a rerun without new data", async () => {
    const counts = () =>
      Promise.all(
        [northwind, lumen].map((id) =>
          query(id, "SELECT (SELECT count(*) FROM ops.published_metric) AS v, (SELECT count(*) FROM ops.report_run) AS r"),
        ),
      );
    const was = await counts();
    const third = await publishReports({ tenants });
    for (const id of [northwind, lumen]) {
      expect(summaryOf(third, id)).toEqual({
        tenant: id,
        status: "published",
        runId: null,
        published: 0,
        restated: 0,
        unchanged: after.get(id)?.size,
      });
    }
    expect(await counts()).toEqual(was);
  });

  it("gives the application role no way to rewrite a published number", async () => {
    await expect(query(northwind, "UPDATE ops.published_metric SET metrics = '{}'")).rejects.toThrow(/permission denied/);
    await expect(query(northwind, "DELETE FROM ops.restatement")).rejects.toThrow(/permission denied/);
  });

  it("shows a scoped tenant only its own published rows", async () => {
    for (const table of TABLES) {
      const [own] = await query<{ total: number; foreign: number }>(
        lumen,
        `SELECT count(*)::int AS total, count(*) FILTER (WHERE tenant_id <> $1)::int AS foreign FROM ${table}`,
        [lumen],
      );
      expect({ table, foreign: own?.foreign }).toEqual({ table, foreign: 0 });
      expect(own?.total).toBeGreaterThan(0);
      const [leaked] = await query<{ n: number }>(northwind, `SELECT count(*)::int AS n FROM ${table} WHERE tenant_id = $1`, [
        lumen,
      ]);
      expect({ table, leaked: leaked?.n }).toEqual({ table, leaked: 0 });
    }
  });

  it("returns zero published rows to an unscoped application connection", async () => {
    const client = new pg.Client({ connectionString: appDatabaseUrl() });
    await client.connect();
    try {
      for (const table of TABLES) {
        const { rows } = await client.query<{ n: number }>(`SELECT count(*)::int AS n FROM ${table}`);
        expect({ table, n: rows[0]?.n }).toEqual({ table, n: 0 });
      }
    } finally {
      await client.end();
    }
  });
});

describe("publishing reports on synthetic tenants", () => {
  let root: string;

  const tenantFor = (name: string): TenantConfig => ({
    id: `rpt_${name}_${suffix}`,
    displayName: name,
    currency: "USD",
    fixturesDir: `fixtures/rpt_${name}_${suffix}`,
    sources: { orders: { columnAliases: {} } },
  });
  const ordersHeader = "order_id,created_at,channel,gross,currency,customer_email\n";
  /** Writes orders batch files for `tenant`, keyed by batch number, and loads them. */
  const loadOrders = async (tenant: TenantConfig, files: Record<number, string>): Promise<void> => {
    const batches: Manifest["batches"] = [];
    for (const [batch, rows] of Object.entries(files)) {
      const path = `orders/batch_0${batch}.csv`;
      const file = join(root, "fixtures", tenant.id, path);
      await mkdir(dirname(file), { recursive: true });
      await writeFile(file, ordersHeader + rows);
      batches.push({
        tenant: tenant.id,
        source: "orders",
        batch: Number(batch),
        path: `${tenant.id}/${path}`,
        covers_from: "2026-03-01",
        covers_to: "2026-03-02",
      });
    }
    await loadBatches({ tenants: [tenant], manifest: { batches }, rootDir: root });
  };
  const count = async (tenantId: string, table: string): Promise<number> =>
    (await query<{ n: number }>(tenantId, `SELECT count(*)::int AS n FROM ${table}`))[0]?.n ?? -1;

  beforeAll(async () => {
    root = await mkdtemp(join(tmpdir(), "report-"));
  });

  afterAll(async () => {
    await rm(root, { recursive: true, force: true });
  });

  it("leaves no partial publication behind when a run fails before commit, and a rerun publishes normally", async () => {
    const [a, b] = [tenantFor("crash_a"), tenantFor("crash_b")] as const;
    for (const tenant of [a, b]) {
      await register(tenant);
      await loadOrders(tenant, { 1: "o-1,2026-03-01T10:00:00Z,google,10.00,USD,a@example.invalid\n" });
    }

    await expect(publishReports({ tenants: [a, b], failAfterTenants: 1 })).rejects.toThrow(InjectedFailure);
    expect(await count(a.id, "ops.published_metric")).toBeGreaterThan(0);
    for (const table of TABLES) expect({ table, n: await count(b.id, table) }).toEqual({ table, n: 0 });

    const rerun = await publishReports({ tenants: [a, b] });
    expect(summaryOf(rerun, a.id)).toMatchObject({ runId: null, published: 0, restated: 0 });
    expect(summaryOf(rerun, b.id)).toMatchObject({ runId: expect.any(String), restated: 0 });
    expect(summaryOf(rerun, b.id).published).toBe(await count(a.id, "ops.published_metric"));
  });

  it("tombstones a published day whose rows all moved to another day", async () => {
    const tenant = tenantFor("moved");
    await register(tenant);
    await loadOrders(tenant, { 1: "o-1,2026-03-01T10:00:00Z,google,10.00,USD,a@example.invalid\n" });
    await publishReports({ tenants: [tenant], marts: ["daily_revenue"] });

    // Batch 2 restates the only order of 2026-03-01 onto 2026-03-02.
    await loadOrders(tenant, { 2: "o-1,2026-03-02T10:00:00Z,google,10.00,USD,a@example.invalid\n" });
    const result = await publishReports({ tenants: [tenant], marts: ["daily_revenue"] });
    expect(summaryOf(result, tenant.id)).toMatchObject({ published: 2, restated: 1, unchanged: 0 });

    const [batch2] = await query<{ id: string }>(tenant.id, "SELECT id::text AS id FROM ops.batch_file WHERE batch_no = 2");
    const rows = await restatements(tenant.id);
    expect(rows.map(({ day, after, caused_by, to_version }) => ({ day, after, caused_by, to_version }))).toEqual([
      { day: "2026-03-01", after: null, caused_by: [batch2?.id], to_version: 2 },
    ]);
    expect(
      await query(tenant.id, "SELECT mart, to_char(day, 'YYYY-MM-DD') AS day, version FROM marts.reported_metric"),
    ).toEqual([{ mart: "daily_revenue", day: "2026-03-02", version: 1 }]);
  });
});
