import { randomBytes } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { SourceName } from "../src/config/sources.ts";
import { loadTenants, type TenantConfig } from "../src/config/tenants.ts";
import { appDatabaseUrl, closePools, getOwnerPool, withTenant } from "../src/db/index.ts";
import { upsertTenant } from "../src/db/seed.ts";
import { type FileResult, loadBatches } from "../src/ingest/loader.ts";
import { loadManifest, type Manifest } from "../src/ingest/manifest.ts";

// Unique tenants per run keep the test re-runnable and away from the seeded 'northwind' and 'lumen'.
const suffix = randomBytes(4).toString("hex");
const northwind = `northwind_${suffix}`;
const lumen = `lumen_${suffix}`;
const createdTenants: string[] = [];

const VIEWS = [
  "staging.orders",
  "staging.email_events",
  "staging.ad_spend",
  "staging.refunds",
  "staging.invalid_rows",
  "marts.daily_revenue",
  "marts.daily_ad_spend",
  "marts.daily_email_engagement",
  "marts.daily_channel_performance",
] as const;

const register = async (tenant: TenantConfig): Promise<boolean> => {
  const changed = await upsertTenant(tenant);
  if (!createdTenants.includes(tenant.id)) createdTenants.push(tenant.id);
  return changed;
};

/** Runs one query scoped to `tenantId` as the application role and returns its rows. */
const query = async <T extends pg.QueryResultRow>(tenantId: string, sql: string, params: unknown[] = []): Promise<T[]> =>
  withTenant(tenantId, async (client) => (await client.query<T>(sql, params)).rows);

const statusCounts = (results: FileResult[]) => {
  const counts: Record<string, number> = {};
  for (const { status } of results) counts[status] = (counts[status] ?? 0) + 1;
  return counts;
};

afterAll(async () => {
  const owner = getOwnerPool();
  for (const id of createdTenants) {
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

describe("staging and marts on the supplied fixtures", () => {
  beforeAll(async () => {
    const tenants = (await loadTenants()).map((t) => ({ ...t, id: `${t.id}_${suffix}` }));
    const real = await loadManifest("fixtures/manifest.json");
    const manifest = { batches: real.batches.map((b) => ({ ...b, tenant: `${b.tenant}_${suffix}` })) };
    for (const tenant of tenants) await register(tenant);
    expect(statusCounts(await loadBatches({ tenants, manifest }))).toEqual({ loaded: 39, missing: 1 });
  }, 60_000);

  describe.each(["northwind", "lumen"])("%s", (name) => {
    const tenantId = `${name}_${suffix}`;

    it("reports daily gross equal to finance_summary for every date, in the tenant currency", async () => {
      // date,gross_reported,net_reported,currency; the currency label is not trusted, only values.
      const [, ...lines] = (await readFile(`fixtures/${name}/finance_summary.csv`, "utf8")).trim().split("\n");
      const reported = Object.fromEntries(lines.map((line) => line.split(",").slice(0, 2)));

      const rows = await query<{ day: string; gross: string; orders: number; currency: string }>(
        tenantId,
        `SELECT to_char(day, 'YYYY-MM-DD') AS day, gross::text AS gross, orders, currency
         FROM marts.daily_revenue ORDER BY day`,
      );
      const withOrders = rows.filter((r) => r.orders > 0);
      expect(Object.fromEntries(withOrders.map((r) => [r.day, r.gross]))).toEqual(reported);
      const { currency } = (await loadTenants()).find((t) => t.id === name) ?? {};
      expect(new Set(rows.map((r) => r.currency))).toEqual(new Set([currency]));
    });

    it("flags orphan refunds and keeps them out of refunds and net", async () => {
      const refunds = await query<{ order_id: string; amount: string; is_orphan: boolean }>(
        tenantId,
        "SELECT order_id, amount::text AS amount, is_orphan FROM staging.refunds",
      );
      const orphans = refunds.filter((r) => r.is_orphan);
      expect(orphans).toHaveLength(6);
      expect(orphans.every((r) => /-00000000$/.test(r.order_id))).toBe(true);

      const [totals] = await query<{ refunds: string; orphans: number; orphan_amount: string; net_ok: boolean }>(
        tenantId,
        `SELECT sum(refunds)::text AS refunds, sum(orphan_refund_count)::int AS orphans,
                sum(orphan_refund_amount)::text AS orphan_amount, bool_and(net = gross - refunds) AS net_ok
         FROM marts.daily_revenue`,
      );
      const [expected] = await query<{ matched: string; orphaned: string }>(
        tenantId,
        `SELECT sum(amount) FILTER (WHERE NOT is_orphan)::text AS matched,
                sum(amount) FILTER (WHERE is_orphan)::text AS orphaned
         FROM staging.refunds`,
      );
      expect(totals).toEqual({ refunds: expected?.matched, orphans: 6, orphan_amount: expected?.orphaned, net_ok: true });
    });

    it("has no invalid rows", async () => {
      expect(await query(tenantId, "SELECT * FROM staging.invalid_rows")).toEqual([]);
    });
  });

  it("keeps each northwind order once, from its latest batch", async () => {
    const [counts] = await query<{ raw: number; distinct_ids: number; staged: number; unique_ids: number }>(
      northwind,
      `SELECT (SELECT count(*) FROM staging.loaded_record WHERE source = 'orders')::int AS raw,
              (SELECT count(DISTINCT staging.column_value(payload, columns, 'order_id'))
               FROM staging.loaded_record WHERE source = 'orders')::int AS distinct_ids,
              (SELECT count(*) FROM staging.orders)::int AS staged,
              (SELECT count(DISTINCT order_id) FROM staging.orders)::int AS unique_ids`,
    );
    expect(counts).toEqual({ raw: 694, distinct_ids: 680, staged: 680, unique_ids: 680 });

    // Every overlapping order is kept from the highest batch that delivered it.
    const stale = await query(
      northwind,
      `SELECT o.order_id
       FROM staging.orders o
       JOIN staging.loaded_record r
         ON r.source = 'orders' AND staging.column_value(r.payload, r.columns, 'order_id') = o.order_id
       GROUP BY o.order_id, o.batch_no
       HAVING max(r.batch_no) <> o.batch_no`,
    );
    expect(stale).toEqual([]);
  });

  it("resolves lumen spend through the cost_usd alias", async () => {
    const [aliased] = await query<{ rows: number; missing: number }>(
      lumen,
      `SELECT count(*)::int AS rows, count(*) FILTER (WHERE a.spend IS NULL)::int AS missing
       FROM staging.ad_spend a
       JOIN ops.batch_file f ON f.tenant_id = a.tenant_id AND f.id = a.batch_file_id
       WHERE f.detail -> 'columns' ? 'cost_usd'`,
    );
    expect(aliased).toEqual({ rows: 36, missing: 0 });
  });

  it("maps lumen's channel, platform and event type spellings onto canonical values", async () => {
    const distinct = async (sql: string) => (await query<{ v: string }>(lumen, sql)).map((r) => r.v);
    expect(await distinct("SELECT DISTINCT channel AS v FROM staging.orders ORDER BY 1")).toEqual([
      "direct",
      "email",
      "facebook",
      "google",
    ]);
    expect(await distinct("SELECT DISTINCT platform AS v FROM staging.ad_spend ORDER BY 1")).toEqual([
      "email",
      "facebook",
      "google",
    ]);
    expect(await distinct("SELECT DISTINCT event_type AS v FROM staging.email_events ORDER BY 1")).toEqual([
      "click",
      "delivered",
      "open",
      "unsubscribe",
    ]);
    const [engagement] = await query<{ delivered: number; opens: number; clicks: number; unsubscribes: number }>(
      lumen,
      `SELECT sum(delivered)::int AS delivered, sum(opens)::int AS opens,
              sum(clicks)::int AS clicks, sum(unsubscribes)::int AS unsubscribes
       FROM marts.daily_email_engagement`,
    );
    expect(engagement).toEqual({ delivered: 382, opens: 342, clicks: 385, unsubscribes: 364 });
  });

  it("shows a scoped tenant only its own rows in every view", async () => {
    for (const view of VIEWS) {
      const [own] = await query<{ total: number; foreign: number }>(
        lumen,
        `SELECT count(*)::int AS total, count(*) FILTER (WHERE tenant_id <> $1)::int AS foreign FROM ${view}`,
        [lumen],
      );
      expect({ view, foreign: own?.foreign }).toEqual({ view, foreign: 0 });
      // Seen by northwind, not one of lumen's rows.
      const [leaked] = await query<{ n: number }>(
        northwind,
        `SELECT count(*)::int AS n FROM ${view} WHERE tenant_id = $1`,
        [lumen],
      );
      expect({ view, leaked: leaked?.n }).toEqual({ view, leaked: 0 });
      if (view !== "staging.invalid_rows") expect(own?.total).toBeGreaterThan(0);
    }
  });

  it("returns zero rows from every view to an unscoped application connection", async () => {
    // security_invoker makes each view check the policies as this role, which has no
    // app.tenant_id set, so nothing underneath is visible through any view.
    const client = new pg.Client({ connectionString: appDatabaseUrl() });
    await client.connect();
    try {
      for (const view of VIEWS) {
        const { rows } = await client.query<{ n: number }>(`SELECT count(*)::int AS n FROM ${view}`);
        expect({ view, n: rows[0]?.n }).toEqual({ view, n: 0 });
      }
    } finally {
      await client.end();
    }
  });

  it("creates every view with security_invoker", async () => {
    const { rows } = await getOwnerPool().query<{ view: string; options: string[] | null }>(
      `SELECT n.nspname || '.' || c.relname AS view, c.reloptions AS options
       FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relkind = 'v' AND n.nspname IN ('staging', 'marts')`,
    );
    expect(rows.length).toBeGreaterThanOrEqual(VIEWS.length);
    for (const { view, options } of rows) {
      expect({ view, options }).toEqual({ view, options: expect.arrayContaining(["security_invoker=true"]) });
    }
  });
});

describe("a third tenant added by configuration only", () => {
  let root: string;
  const tenant: TenantConfig = {
    id: `acme_${suffix}`,
    displayName: "Acme",
    currency: "GBP",
    fixturesDir: `fixtures/acme_${suffix}`,
    sources: {
      orders: {
        columnAliases: { gross: ["total_amount"] },
        valueMaps: { channel: { FB: "facebook", ADW: "google" } },
      },
      email_events: { columnAliases: { type: ["event"] }, valueMaps: { type: { Opened: "open", Clicked: "click" } } },
      ad_spend: { columnAliases: { spend: ["cost"] }, valueMaps: { platform: { FB: "facebook" } } },
      refunds: { columnAliases: {} },
    },
  };
  const files: Record<string, string> = {
    "orders/batch_01.csv": [
      "order_id,created_at,channel,total_amount,currency,customer_email",
      "A-1,2026-03-01T10:00:00Z,FB,100.00,GBP,a@example.invalid",
      "A-2,2026-03-01T23:30:00-02:00,ADW,50.00,GBP,b@example.invalid",
      "A-3,2026-03-01T12:00:00Z,FB,not-a-number,GBP,c@example.invalid",
    ].join("\n"),
    // Restates A-1 with a new amount: the later batch wins.
    "orders/batch_02.csv": [
      "order_id,created_at,channel,total_amount,currency,customer_email",
      "A-1,2026-03-01T10:00:00Z,FB,120.00,GBP,a@example.invalid",
    ].join("\n"),
    // Canonical key on one line, the declared alias on the next.
    "email_events/batch_01.ndjson": [
      '{"event_id":"e-1","type":"Opened","email":"a@example.invalid","campaign_id":"c","occurred_at":"2026-03-01T09:00:00Z"}',
      '{"event_id":"e-2","event":"Clicked","email":"a@example.invalid","campaign_id":"c","occurred_at":"2026-03-01T09:05:00Z"}',
    ].join("\n"),
    // The second day has spend on a channel without revenue.
    "ad_spend/batch_01.csv": "date,campaign_id,platform,cost\n2026-03-01,c,FB,40.00\n2026-03-02,c,FB,5.00\n",
    "refunds/batch_01.csv": [
      "refund_id,refunded_at,order_id,amount,currency",
      "r-1,2026-03-02T08:00:00Z,A-2,10.00,GBP",
      "r-2,2026-03-02T09:00:00Z,XX-00000000,7.50,GBP",
    ].join("\n"),
  };

  beforeAll(async () => {
    root = await mkdtemp(join(tmpdir(), "staging-"));
    const batches: Manifest["batches"] = [];
    for (const [path, text] of Object.entries(files)) {
      const file = join(root, tenant.fixturesDir, path);
      await mkdir(dirname(file), { recursive: true });
      await writeFile(file, text);
      const [source, name] = path.split("/") as [SourceName, string];
      batches.push({
        tenant: tenant.id,
        source,
        batch: Number(/batch_(\d+)/.exec(name)?.[1]),
        path: `${tenant.id}/${path}`,
        covers_from: "2026-03-01",
        covers_to: "2026-03-01",
      });
    }
    expect(await register(tenant)).toBe(true);
    const results = await loadBatches({ tenants: [tenant], manifest: { batches }, rootDir: root });
    expect(statusCounts(results)).toEqual({ loaded: 5 });
  }, 60_000);

  afterAll(async () => {
    await rm(root, { recursive: true, force: true });
  });

  it("stages its orders through its own aliases and value maps, latest batch first", async () => {
    const rows = await query(
      tenant.id,
      `SELECT order_id, channel, gross::text AS gross, to_char(order_date, 'YYYY-MM-DD') AS order_date, batch_no
       FROM staging.orders ORDER BY order_id`,
    );
    expect(rows).toEqual([
      { order_id: "A-1", channel: "facebook", gross: "120.00", order_date: "2026-03-01", batch_no: 2 },
      // 23:30 at -02:00 is 01:30 UTC the next day.
      { order_id: "A-2", channel: "google", gross: "50.00", order_date: "2026-03-02", batch_no: 1 },
      { order_id: "A-3", channel: "facebook", gross: null, order_date: "2026-03-01", batch_no: 1 },
    ]);
  });

  it("lists the value that failed its cast instead of failing the view", async () => {
    expect(
      await query(tenant.id, "SELECT source, batch_no, line_no, column_name, raw_value, problem FROM staging.invalid_rows"),
    ).toEqual([
      { source: "orders", batch_no: 1, line_no: 4, column_name: "gross", raw_value: "not-a-number", problem: "invalid numeric" },
    ]);
  });

  it("reads NDJSON lines that use the canonical key and the alias", async () => {
    expect(
      await query(tenant.id, "SELECT event_id, event_type FROM staging.email_events ORDER BY event_id"),
    ).toEqual([
      { event_id: "e-1", event_type: "open" },
      { event_id: "e-2", event_type: "click" },
    ]);
  });

  it("builds the marts in its configured currency", async () => {
    expect(
      await query(
        tenant.id,
        `SELECT to_char(day, 'YYYY-MM-DD') AS day, currency, orders, gross::text AS gross, refunds::text AS refunds,
                net::text AS net, orphan_refund_count, orphan_refund_amount::text AS orphan_refund_amount
         FROM marts.daily_revenue ORDER BY day`,
      ),
    ).toEqual([
      {
        day: "2026-03-01",
        currency: "GBP",
        orders: 2,
        gross: "120.00",
        refunds: "0",
        net: "120.00",
        orphan_refund_count: 0,
        orphan_refund_amount: "0",
      },
      {
        day: "2026-03-02",
        currency: "GBP",
        orders: 1,
        gross: "50.00",
        refunds: "10.00",
        net: "40.00",
        orphan_refund_count: 1,
        orphan_refund_amount: "7.50",
      },
    ]);
    expect(
      await query(
        tenant.id,
        `SELECT to_char(day, 'YYYY-MM-DD') AS day, channel, revenue::text AS revenue, spend::text AS spend,
                roas::text AS roas
         FROM marts.daily_channel_performance ORDER BY day, channel`,
      ),
    ).toEqual([
      { day: "2026-03-01", channel: "facebook", revenue: "120.00", spend: "40.00", roas: "3.0000" },
      { day: "2026-03-02", channel: "facebook", revenue: "0", spend: "5.00", roas: "0.0000" },
      { day: "2026-03-02", channel: "google", revenue: "50.00", spend: "0", roas: null },
    ]);
  });

  it("re-seeds only on a value map change, and the views follow the new map", async () => {
    expect(await register(tenant)).toBe(false);
    const changed = {
      ...tenant,
      sources: { ...tenant.sources, orders: { columnAliases: { gross: ["total_amount"] }, valueMaps: { channel: { FB: "meta" } } } },
    };
    expect(await register(changed)).toBe(true);
    expect(await query(tenant.id, "SELECT DISTINCT channel FROM staging.orders ORDER BY 1")).toEqual([
      // ADW lost its entry, so it passes through unmapped.
      { channel: "ADW" },
      { channel: "meta" },
    ]);
    expect(await register(changed)).toBe(false);
  });
});
