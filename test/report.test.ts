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
import { MART_NAMES, type MartName, MARTS, type PublishResult, publishReports } from "../src/report/publish.ts";
import { isSupplied } from "./supplied-tenants.ts";

// Unique tenants per run keep the test re-runnable and away from the seeded 'northwind' and 'lumen'.
const suffix = randomBytes(4).toString("hex");
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

/**
 * The live keys of `live` that depend on `source` and fall inside `entry`'s window: the keys
 * a report must withhold while that manifest batch has not loaded.
 */
const keysInWindow = (live: Map<string, string>, source: string, entry: Manifest["batches"][number]): string[] =>
  [...live.keys()]
    .filter((key) => {
      const [mart, day] = key.split("|") as [MartName, string];
      const sources: readonly { source: string }[] = MARTS[mart].lineage;
      return sources.some((l) => l.source === source) && day >= entry.covers_from && day <= entry.covers_to;
    })
    .sort();

describe("publishing reports on the supplied fixtures, with batch 5 arriving late", () => {
  let tenants: TenantConfig[];
  let manifest: Manifest;
  let first: PublishResult[];
  const before = new Map<string, Map<string, string>>();
  const after = new Map<string, Map<string, string>>();
  let second: PublishResult[];
  /** Lumen's ad_spend batch 3: listed in the manifest, never delivered. */
  let missing: Manifest["batches"][number];
  /** Per tenant, the live keys inside the window of that missing batch, before and after batch 5. */
  const withheldBefore = new Map<string, string[]>();
  const withheldAfter = new Map<string, string[]>();

  beforeAll(async () => {
    tenants = (await loadTenants()).filter(isSupplied).map((t) => ({ ...t, id: `${t.id}_${suffix}` }));
    const real = await loadManifest("fixtures/manifest.json");
    manifest = { batches: real.batches.filter(isSupplied).map((b) => ({ ...b, tenant: `${b.tenant}_${suffix}` })) };
    for (const tenant of tenants) await register(tenant);

    const found = manifest.batches.find((b) => b.tenant === lumen && b.source === "ad_spend" && b.batch === 3);
    if (found === undefined) throw new Error("expected lumen's ad_spend batch 3 in the manifest");
    missing = found;
    const withheldIn = (tenantId: string, live: Map<string, string>) =>
      tenantId === lumen ? keysInWindow(live, "ad_spend", missing) : [];

    // Each run expects what had been delivered by then: batches 1-4, then all five.
    const firstManifest = { batches: manifest.batches.filter((b) => b.batch <= 4) };
    await loadBatches({ tenants, manifest: firstManifest });
    first = await publishReports({ tenants, manifest: firstManifest });
    for (const t of tenants) before.set(t.id, await liveMarts(t.id));

    await loadBatches({ tenants, manifest: { batches: manifest.batches.filter((b) => b.batch === 5) } });
    for (const t of tenants) after.set(t.id, await liveMarts(t.id));
    second = await publishReports({ tenants, manifest });
    for (const t of tenants) {
      withheldBefore.set(t.id, withheldIn(t.id, before.get(t.id) ?? new Map()));
      withheldAfter.set(t.id, withheldIn(t.id, after.get(t.id) ?? new Map()));
    }
  }, 60_000);

  const publishedKeys = async (tenantId: string): Promise<Set<string>> =>
    new Set(
      (
        await query<{ key: string }>(
          tenantId,
          "SELECT DISTINCT mart || '|' || to_char(day, 'YYYY-MM-DD') || '|' || dims::text AS key FROM ops.published_metric",
        )
      ).map((r) => r.key),
    );

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
    const live = before.get(tenantId) ?? new Map<string, string>();
    const withheld = withheldBefore.get(tenantId) ?? [];
    expect(summaryOf(first, tenantId)).toEqual({
      tenant: tenantId,
      status: "published",
      runId: expect.any(String),
      published: live.size - withheld.length,
      restated: 0,
      unchanged: 0,
      withheld: withheld.length,
    });
    const [firstRun] = await query<{ versions: number; max_version: number }>(
      tenantId,
      `SELECT count(*)::int AS versions, max(version) AS max_version FROM ops.published_metric
       WHERE run_id = $1`,
      [summaryOf(first, tenantId).runId],
    );
    expect(firstRun).toEqual({ versions: live.size - withheld.length, max_version: 1 });
  });

  it.each(["northwind", "lumen"])("restates exactly the %s keys batch 5 changed, from and to the live numbers", async (name) => {
    const tenantId = `${name}_${suffix}`;
    const held = new Set(withheldAfter.get(tenantId));
    const was = new Map([...(before.get(tenantId) ?? [])].filter(([key]) => !held.has(key)));
    const now = new Map([...(after.get(tenantId) ?? [])].filter(([key]) => !held.has(key)));
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
      withheld: held.size,
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

  it("withholds lumen's ad spend and channel performance for the days of the missing ad_spend batch", async () => {
    const withheld = withheldAfter.get(lumen) ?? [];
    expect(withheld.length).toBeGreaterThan(0);
    expect(new Set(withheld.map((key) => key.split("|")[0]))).toEqual(new Set(["daily_channel_performance"]));
    const published = await publishedKeys(lumen);
    expect(withheld.filter((key) => published.has(key))).toEqual([]);

    // Revenue and email engagement do not depend on ad_spend: every day of the window is published.
    const inWindow = [...published].filter((key) => {
      const day = key.split("|")[1] ?? "";
      return day >= missing.covers_from && day <= missing.covers_to;
    });
    for (const mart of ["daily_revenue", "daily_email_engagement"]) {
      expect(inWindow.filter((key) => key.startsWith(`${mart}|`))).toHaveLength(6);
    }
    expect(inWindow.filter((key) => key.startsWith("daily_channel_performance|") || key.startsWith("daily_ad_spend|"))).toEqual([]);

    // The run records the window for both marts built from ad_spend, with the keys it held back.
    const [run] = await query<{ withheld: unknown }>(lumen, "SELECT withheld FROM ops.report_run WHERE id = $1", [
      summaryOf(second, lumen).runId,
    ]);
    const { path, covers_from, covers_to } = missing;
    const window = { source: "ad_spend", batch: 3, path, covers_from, covers_to, status: "not_received" };
    const keys = withheld.map((key) => {
      const [, day, dims] = key.split("|");
      return { day, dims: JSON.parse(dims ?? "") as unknown };
    });
    // daily_ad_spend has no row at all for those days, so its window holds back no key.
    expect(run?.withheld).toEqual([
      { mart: "daily_ad_spend", ...window, keys: [] },
      { mart: "daily_channel_performance", ...window, keys: expect.arrayContaining(keys) },
    ]);
    expect((run?.withheld as { keys: unknown[] }[])[1]?.keys).toHaveLength(keys.length);
    expect(summaryOf(second, northwind).withheld).toBe(0);
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
    const third = await publishReports({ tenants, manifest });
    for (const id of [northwind, lumen]) {
      const withheld = withheldAfter.get(id)?.length ?? 0;
      // Withholding alone writes no run: nothing was published.
      expect(summaryOf(third, id)).toEqual({
        tenant: id,
        status: "published",
        runId: null,
        published: 0,
        restated: 0,
        unchanged: (after.get(id)?.size ?? 0) - withheld,
        withheld,
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
  /** A manifest expecting orders batches `batches` of `tenant`, batch n covering 2026-03-(2n-1)..2026-03-(2n). */
  const expecting = (tenant: TenantConfig, batches: number[]): Manifest => ({
    batches: batches.map((batch) => ({
      tenant: tenant.id,
      source: "orders",
      batch,
      path: `${tenant.id}/orders/batch_0${batch}.csv`,
      covers_from: `2026-03-${String(2 * batch - 1).padStart(2, "0")}`,
      covers_to: `2026-03-${String(2 * batch).padStart(2, "0")}`,
    })),
  });
  const reportedDays = async (tenantId: string) =>
    query<{ day: string; version: number }>(
      tenantId,
      "SELECT to_char(day, 'YYYY-MM-DD') AS day, version FROM marts.reported_metric ORDER BY day",
    );
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

  it("withholds the days of a batch that has not arrived, then publishes them as version 1 once it loads", async () => {
    const tenant = tenantFor("gap");
    await register(tenant);
    const manifest = expecting(tenant, [1, 2, 3]);
    // Batch 3 carries a late order for 2026-03-03, a day of the missing batch 2.
    await loadOrders(tenant, {
      1: "o-1,2026-03-01T10:00:00Z,google,10.00,USD,a@example.invalid\n",
      3: "o-3,2026-03-05T10:00:00Z,google,30.00,USD,a@example.invalid\no-9,2026-03-03T10:00:00Z,google,5.00,USD,a@example.invalid\n",
    });
    const gap = await publishReports({ tenants: [tenant], marts: ["daily_revenue"], manifest });
    expect(summaryOf(gap, tenant.id)).toMatchObject({ published: 2, restated: 0, unchanged: 0, withheld: 1 });
    expect(await reportedDays(tenant.id)).toEqual([
      { day: "2026-03-01", version: 1 },
      { day: "2026-03-05", version: 1 },
    ]);
    const [run] = await query<{ withheld: unknown }>(tenant.id, "SELECT withheld FROM ops.report_run WHERE id = $1", [
      summaryOf(gap, tenant.id).runId,
    ]);
    expect(run?.withheld).toEqual([
      {
        mart: "daily_revenue",
        source: "orders",
        batch: 2,
        path: `${tenant.id}/orders/batch_02.csv`,
        covers_from: "2026-03-03",
        covers_to: "2026-03-04",
        status: "not_received",
        keys: [{ day: "2026-03-03", dims: {} }],
      },
    ]);

    // Still missing: nothing new to publish, so no run is written, and the withholding is reported again.
    const again = await publishReports({ tenants: [tenant], marts: ["daily_revenue"], manifest });
    expect(summaryOf(again, tenant.id)).toMatchObject({ runId: null, published: 0, unchanged: 2, withheld: 1 });

    await loadOrders(tenant, { 2: "o-2,2026-03-04T10:00:00Z,google,20.00,USD,a@example.invalid\n" });
    const filled = await publishReports({ tenants: [tenant], marts: ["daily_revenue"], manifest });
    expect(summaryOf(filled, tenant.id)).toMatchObject({ published: 2, restated: 0, unchanged: 2, withheld: 0 });
    expect(await reportedDays(tenant.id)).toEqual(
      ["2026-03-01", "2026-03-03", "2026-03-04", "2026-03-05"].map((day) => ({ day, version: 1 })),
    );
    expect(await count(tenant.id, "ops.restatement")).toBe(0);
  });

  it("withholds the days of a batch that was only quarantined", async () => {
    const tenant = tenantFor("quarantined");
    await register(tenant);
    await loadOrders(tenant, {
      1: "o-1,2026-03-01T10:00:00Z,google,10.00,USD,a@example.invalid\n",
      // Too few columns: the whole file is quarantined.
      2: "o-2,2026-03-03T10:00:00Z,google\n",
      3: "o-3,2026-03-05T10:00:00Z,google,30.00,USD,a@example.invalid\no-9,2026-03-04T10:00:00Z,google,5.00,USD,a@example.invalid\n",
    });
    const result = await publishReports({ tenants: [tenant], marts: ["daily_revenue"], manifest: expecting(tenant, [1, 2, 3]) });
    expect(summaryOf(result, tenant.id)).toMatchObject({ published: 2, withheld: 1 });
    expect((await reportedDays(tenant.id)).map((r) => r.day)).toEqual(["2026-03-01", "2026-03-05"]);
    const [run] = await query<{ status: string }>(
      tenant.id,
      "SELECT withheld -> 0 ->> 'status' AS status FROM ops.report_run WHERE id = $1",
      [summaryOf(result, tenant.id).runId],
    );
    expect(run?.status).toBe("quarantined");
  });

  it("keeps the last published version of a key whose window became incomplete, instead of tombstoning it", async () => {
    const tenant = tenantFor("reopened");
    await register(tenant);
    // Batch 1 also carries an early order for 2026-03-03, published while batch 2 was not yet expected.
    await loadOrders(tenant, {
      1: "o-1,2026-03-01T10:00:00Z,google,10.00,USD,a@example.invalid\no-2,2026-03-03T10:00:00Z,google,20.00,USD,a@example.invalid\n",
    });
    await publishReports({ tenants: [tenant], marts: ["daily_revenue"], manifest: expecting(tenant, [1]) });

    // Batch 3 moves o-2 onto 2026-03-05; batch 2, now expected, has not arrived.
    await loadOrders(tenant, { 3: "o-2,2026-03-05T10:00:00Z,google,20.00,USD,a@example.invalid\n" });
    const result = await publishReports({ tenants: [tenant], marts: ["daily_revenue"], manifest: expecting(tenant, [1, 2, 3]) });
    expect(summaryOf(result, tenant.id)).toMatchObject({ published: 1, restated: 0, unchanged: 1, withheld: 1 });
    expect(await count(tenant.id, "ops.restatement")).toBe(0);
    expect(await reportedDays(tenant.id)).toEqual(
      ["2026-03-01", "2026-03-03", "2026-03-05"].map((day) => ({ day, version: 1 })),
    );
  });
});
