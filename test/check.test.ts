import { randomBytes } from "node:crypto";
import { access, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { SourceName } from "../src/config/sources.ts";
import { loadTenants, type TenantConfig } from "../src/config/tenants.ts";
import { closePools, getOwnerPool, withTenant } from "../src/db/index.ts";
import { upsertTenant } from "../src/db/seed.ts";
import { checkSources, exitStatus, formatReport, type TenantHealth } from "../src/ingest/check.ts";
import { loadBatches } from "../src/ingest/loader.ts";
import { loadManifest, type Manifest } from "../src/ingest/manifest.ts";
import { isSupplied } from "./supplied-tenants.ts";

// Unique tenants per run keep the test re-runnable and away from the seeded 'northwind' and 'lumen'.
const suffix = randomBytes(4).toString("hex");
const createdTenants: string[] = [];

const register = async (tenant: TenantConfig) => {
  await upsertTenant(tenant);
  createdTenants.push(tenant.id);
};

afterAll(async () => {
  const owner = getOwnerPool();
  for (const id of createdTenants) {
    await withTenant(
      id,
      async (client) => {
        await client.query("DELETE FROM raw.record WHERE tenant_id = $1", [id]);
        await client.query("DELETE FROM ops.batch_file WHERE tenant_id = $1", [id]);
        // ops.value_map arrives with a later migration; clean it only where it exists.
        const { rows } = await client.query<{ exists: boolean }>(
          "SELECT to_regclass('ops.value_map') IS NOT NULL AS exists",
        );
        if (rows[0]?.exists) await client.query("DELETE FROM ops.value_map WHERE tenant_id = $1", [id]);
        await client.query("DELETE FROM ops.tenant WHERE tenant_id = $1", [id]);
      },
      owner,
    );
  }
  await closePools();
});

const exists = (path: string) =>
  access(path).then(
    () => true,
    () => false,
  );

const reportOf = (reports: TenantHealth[], tenant: string): TenantHealth => {
  const report = reports.find((r) => r.tenant === tenant);
  if (report === undefined) throw new Error(`no report for ${tenant}`);
  return report;
};

const reportTenant = (tenants: TenantConfig[], id: string): TenantConfig => {
  const tenant = tenants.find((t) => t.id === id);
  if (tenant === undefined) throw new Error(`no tenant ${id}`);
  return tenant;
};

describe("source health check on the supplied fixtures", () => {
  let tenants: TenantConfig[];
  let manifest: Manifest;
  let reports: TenantHealth[];
  const lumen = `lumen_${suffix}`;
  const northwind = `northwind_${suffix}`;

  beforeAll(async () => {
    tenants = (await loadTenants()).filter(isSupplied).map((t) => ({ ...t, id: `${t.id}_${suffix}` }));
    const real = await loadManifest("fixtures/manifest.json");
    manifest = { batches: real.batches.filter(isSupplied).map((b) => ({ ...b, tenant: `${b.tenant}_${suffix}` })) };
    for (const tenant of tenants) await register(tenant);
    await loadBatches({ tenants, manifest });
    reports = await checkSources({ tenants, manifest });
  }, 60_000);

  it("reports exactly the batch that never arrived for lumen, and northwind as healthy", async () => {
    // Derived from the files on disk rather than hard-coded: a listed file that is absent never arrived.
    const absent = [];
    for (const b of manifest.batches) {
      if (!(await exists(join("fixtures", b.path)))) absent.push(b);
    }
    expect(absent.map((b) => [b.tenant, b.source, b.batch])).toEqual([[lumen, "ad_spend", 3]]);
    const [gap] = absent;
    if (gap === undefined) throw new Error("unreachable");

    const lumenReport = reportOf(reports, lumen);
    const asOf = manifest.batches
      .filter((b) => b.tenant === lumen)
      .map((b) => b.covers_to)
      .sort()
      .at(-1);
    expect(lumenReport.asOf).toBe(asOf);
    // Later ad_spend batches did load, so freshness alone would not reveal the hole.
    const adFresh = manifest.batches
      .filter((b) => b.tenant === lumen && b.source === "ad_spend" && b.batch !== gap.batch)
      .map((b) => b.covers_to)
      .sort()
      .at(-1);
    expect(lumenReport.freshness.ad_spend).toBe(adFresh);
    const stale = adFresh === undefined || asOf === undefined || adFresh < asOf;
    expect(lumenReport.findings).toEqual([
      { kind: "not_received", source: "ad_spend", batch: gap.batch, path: gap.path, coversTo: gap.covers_to },
      ...(stale ? [{ kind: "stale", source: "ad_spend", freshThrough: adFresh ?? null, asOf }] : []),
    ]);

    expect(reportOf(reports, northwind).findings).toEqual([]);
    expect(reportOf(reports, northwind).batches.every((b) => b.status === "loaded")).toBe(true);
    // Both tenants' later ad_spend files say cost_usd, a declared alias: a note, never a finding.
    const alias = { kind: "declared_alias", source: "ad_spend", header: "cost_usd", column: "spend" };
    expect(reportOf(reports, lumen).notes).toEqual([{ ...alias, batches: [4, 5] }]);
    expect(reportOf(reports, northwind).notes).toEqual([{ ...alias, batches: [4, 5] }]);
    expect(exitStatus(reports)).toBe(2);
    expect(exitStatus([reportOf(reports, northwind)])).toBe(0);

    expect(formatReport(reports)).toEqual([
      `lumen_${suffix}: 1 finding (19/20 batches loaded, as of 2026-02-04)`,
      `  not_received: ad_spend/batch 3 (lumen/ad_spend/batch_03.csv, covers through 2026-01-23)`,
      `  note: ad_spend batches 4-5 read header "cost_usd" as spend (declared alias)`,
      `northwind_${suffix}: healthy (20/20 batches loaded, as of 2026-02-04)`,
      `  note: ad_spend batches 4-5 read header "cost_usd" as spend (declared alias)`,
    ]);
  });

  it("flags every configured source as stale against a later asOf", async () => {
    const later = await checkSources({ tenants, manifest, asOf: "2026-03-01" });
    expect(reportOf(later, northwind).findings.map((f) => [f.kind, f.source])).toEqual([
      ["stale", "orders"],
      ["stale", "email_events"],
      ["stale", "ad_spend"],
      ["stale", "refunds"],
    ]);
    await expect(checkSources({ tenants, manifest, asOf: "01/03/2026" })).rejects.toThrow("YYYY-MM-DD");
  });

  it("never reports another tenant's ledger rows", async () => {
    // Same sources and batch numbers as the loaded northwind clone, but nothing loaded of its own.
    const empty: TenantConfig = { ...reportTenant(tenants, northwind), id: `chk_empty_${suffix}` };
    await register(empty);
    const batches = manifest.batches.filter((b) => b.tenant === northwind).map((b) => ({ ...b, tenant: empty.id }));
    const both = { batches: [...manifest.batches, ...batches] };

    const [report] = await checkSources({ tenants: [empty], manifest: both });
    expect(report?.tenant).toBe(empty.id);
    expect(report?.batches).toHaveLength(batches.length);
    expect(report?.batches.every((b) => b.status === "not_received")).toBe(true);
    expect(Object.values(report?.freshness ?? {}).every((d) => d === null)).toBe(true);
    expect(report?.notes).toEqual([]);

    // And the loaded tenant's report only covers its own manifest entries.
    const [own] = await checkSources({ tenants: [reportTenant(tenants, northwind)], manifest: both });
    expect(own?.batches).toHaveLength(batches.length);
    expect(own?.findings).toEqual([]);
  });
});

describe("source health check on synthetic tenants", () => {
  let root: string;
  let northwindSources: TenantConfig["sources"];
  let lumenSources: TenantConfig["sources"];

  const tenantFor = (name: string, sources: SourceName[]): TenantConfig => ({
    id: `chk_${name}_${suffix}`,
    displayName: name,
    currency: "USD",
    fixturesDir: `fixtures/chk_${name}_${suffix}`,
    sources: Object.fromEntries(sources.map((s) => [s, northwindSources[s]])),
  });
  const put = async (tenant: TenantConfig, path: string, text: string) => {
    const file = join(root, "fixtures", tenant.id, path);
    await mkdir(dirname(file), { recursive: true });
    await writeFile(file, text);
  };
  const entry = (tenant: TenantConfig, source: SourceName, batch: number, path: string, coversTo: string) => ({
    tenant: tenant.id,
    source,
    batch,
    path: `${tenant.id}/${path}`,
    covers_from: "2026-01-06",
    covers_to: coversTo,
  });
  const loadAndCheck = async (tenant: TenantConfig, batches: Manifest["batches"]) => {
    await register(tenant);
    await loadBatches({ tenants: [tenant], manifest: { batches }, rootDir: root });
    const reports = await checkSources({ tenants: [tenant], manifest: { batches } });
    return { report: reportOf(reports, tenant.id), status: exitStatus(reports) };
  };

  beforeAll(async () => {
    root = await mkdtemp(join(tmpdir(), "check-"));
    const supplied = await loadTenants();
    northwindSources = supplied.find((t) => t.id === "northwind")?.sources ?? {};
    lumenSources = supplied.find((t) => t.id === "lumen")?.sources ?? {};
  });

  afterAll(async () => {
    await rm(root, { recursive: true, force: true });
  });

  it("reports a batch that was only ever quarantined, with its reason codes", async () => {
    const tenant = tenantFor("quar", ["ad_spend"]);
    await put(tenant, "ad_spend/batch_01.csv", "date,campaign_id,platform,cost_usd\n2026-01-06,c-1,facebook,1.00\n");
    await put(tenant, "ad_spend/batch_02.csv", "date,campaign_id,platform,spend_eur\n2026-01-12,c-1,facebook,1.00\n");

    const { report, status } = await loadAndCheck(tenant, [
      entry(tenant, "ad_spend", 1, "ad_spend/batch_01.csv", "2026-01-11"),
      entry(tenant, "ad_spend", 2, "ad_spend/batch_02.csv", "2026-01-17"),
    ]);
    expect(report.batches.map((b) => [b.batch, b.status])).toEqual([
      [1, "loaded"],
      [2, "quarantined"],
    ]);
    expect(report.findings).toEqual([
      {
        kind: "quarantined",
        source: "ad_spend",
        batch: 2,
        path: `${tenant.id}/ad_spend/batch_02.csv`,
        coversTo: "2026-01-17",
        reasonCodes: ["unknown_header", "missing_column"],
      },
      { kind: "stale", source: "ad_spend", freshThrough: "2026-01-11", asOf: "2026-01-17" },
    ]);
    expect(status).toBe(2);
  });

  it("notes headers read through a declared alias without counting them as findings", async () => {
    const tenant = tenantFor("alias", ["ad_spend"]);
    const row = (day: string) => `${day},c-1,facebook,1.00\n`;
    await put(tenant, "ad_spend/batch_01.csv", `date,campaign_id,platform,cost_usd\n${row("2026-01-06")}`);
    await put(tenant, "ad_spend/batch_02.csv", `date,campaign_id,platform,spend\n${row("2026-01-07")}`);
    await put(tenant, "ad_spend/batch_03.csv", `date,campaign_id,platform,cost_usd\n${row("2026-01-08")}`);
    await put(tenant, "ad_spend/batch_04.csv", `date,campaign_id,platform,cost_usd\n${row("2026-01-09")}`);

    const { report, status } = await loadAndCheck(tenant, [
      entry(tenant, "ad_spend", 1, "ad_spend/batch_01.csv", "2026-01-06"),
      entry(tenant, "ad_spend", 2, "ad_spend/batch_02.csv", "2026-01-07"),
      entry(tenant, "ad_spend", 3, "ad_spend/batch_03.csv", "2026-01-08"),
      entry(tenant, "ad_spend", 4, "ad_spend/batch_04.csv", "2026-01-09"),
    ]);
    expect(report.findings).toEqual([]);
    expect(report.notes).toEqual([
      { kind: "declared_alias", source: "ad_spend", header: "cost_usd", column: "spend", batches: [1, 3, 4] },
    ]);
    expect(formatReport([report])).toEqual([
      `${tenant.id}: healthy (4/4 batches loaded, as of 2026-01-09)`,
      '  note: ad_spend batches 1, 3-4 read header "cost_usd" as spend (declared alias)',
    ]);
    expect(status).toBe(0);
  });

  it("names a single aliased batch in the singular", async () => {
    const tenant = tenantFor("alias1", ["ad_spend"]);
    const row = (day: string) => `${day},c-1,facebook,1.00\n`;
    await put(tenant, "ad_spend/batch_01.csv", `date,campaign_id,platform,spend\n${row("2026-01-06")}`);
    await put(tenant, "ad_spend/batch_02.csv", `date,campaign_id,platform,cost_usd\n${row("2026-01-07")}`);

    const { report, status } = await loadAndCheck(tenant, [
      entry(tenant, "ad_spend", 1, "ad_spend/batch_01.csv", "2026-01-06"),
      entry(tenant, "ad_spend", 2, "ad_spend/batch_02.csv", "2026-01-07"),
    ]);
    expect(report.notes).toEqual([
      { kind: "declared_alias", source: "ad_spend", header: "cost_usd", column: "spend", batches: [2] },
    ]);
    expect(formatReport([report])[1]).toBe(
      '  note: ad_spend batch 2 read header "cost_usd" as spend (declared alias)',
    );
    expect(status).toBe(0);
  });

  it("lists two aliased headers of one source in the order they were first loaded", async () => {
    // cost_usd is first loaded (batch 1) but sorts after channel and follows it in the header row,
    // so neither alphabetical nor column order would put it first.
    const tenant: TenantConfig = {
      ...tenantFor("alias2", []),
      sources: { ad_spend: { columnAliases: { spend: ["cost_usd"], platform: ["channel"] } } },
    };
    const row = (day: string) => `${day},c-1,facebook,1.00\n`;
    await put(tenant, "ad_spend/batch_01.csv", `date,campaign_id,platform,cost_usd\n${row("2026-01-06")}`);
    await put(tenant, "ad_spend/batch_02.csv", `date,campaign_id,channel,spend\n${row("2026-01-07")}`);
    await put(tenant, "ad_spend/batch_03.csv", `date,campaign_id,channel,cost_usd\n${row("2026-01-08")}`);

    const { report, status } = await loadAndCheck(tenant, [
      entry(tenant, "ad_spend", 1, "ad_spend/batch_01.csv", "2026-01-06"),
      entry(tenant, "ad_spend", 2, "ad_spend/batch_02.csv", "2026-01-07"),
      entry(tenant, "ad_spend", 3, "ad_spend/batch_03.csv", "2026-01-08"),
    ]);
    expect(report.findings).toEqual([]);
    expect(report.notes).toEqual([
      { kind: "declared_alias", source: "ad_spend", header: "cost_usd", column: "spend", batches: [1, 3] },
      { kind: "declared_alias", source: "ad_spend", header: "channel", column: "platform", batches: [2, 3] },
    ]);
    expect(formatReport([report])).toEqual([
      `${tenant.id}: healthy (3/3 batches loaded, as of 2026-01-08)`,
      '  note: ad_spend batches 1, 3 read header "cost_usd" as spend (declared alias)',
      '  note: ad_spend batches 2-3 read header "channel" as platform (declared alias)',
    ]);
    expect(status).toBe(0);
  });

  it("reports a latest batch that was never delivered as not received and the source as stale", async () => {
    const tenant = tenantFor("late", ["refunds"]);
    await put(tenant, "refunds/batch_01.csv", "refund_id,refunded_at,order_id,amount,currency\nrf-1,2026-01-06T00:00:00Z,o-1,1.00,USD\n");

    const { report, status } = await loadAndCheck(tenant, [
      entry(tenant, "refunds", 1, "refunds/batch_01.csv", "2026-01-11"),
      entry(tenant, "refunds", 2, "refunds/batch_02.csv", "2026-01-17"),
    ]);
    expect(report.findings).toEqual([
      {
        kind: "not_received",
        source: "refunds",
        batch: 2,
        path: `${tenant.id}/refunds/batch_02.csv`,
        coversTo: "2026-01-17",
      },
      { kind: "stale", source: "refunds", freshThrough: "2026-01-11", asOf: "2026-01-17" },
    ]);
    expect(status).toBe(2);
  });

  it("reports a loaded batch whose later redelivery with other content was quarantined", async () => {
    const tenant = tenantFor("redeliver", ["ad_spend"]);
    const header = "date,campaign_id,platform,cost_usd\n";
    await put(tenant, "ad_spend/batch_01.csv", `${header}2026-01-06,c-1,facebook,1.00\n`);
    const batches = [entry(tenant, "ad_spend", 1, "ad_spend/batch_01.csv", "2026-01-11")];
    const first = await loadAndCheck(tenant, batches);
    expect(first.report.findings).toEqual([]);
    expect(first.status).toBe(0);

    // The source re-sends batch 1 with a corrected amount: the loader holds it back.
    await put(tenant, "ad_spend/batch_01.csv", `${header}2026-01-06,c-1,facebook,2.00\n`);
    const { report, status } = await loadAndCheck(tenant, batches);
    expect(report.batches.map((b) => [b.batch, b.status])).toEqual([[1, "loaded"]]);
    expect(report.findings).toEqual([
      {
        kind: "conflicting_redelivery",
        source: "ad_spend",
        batch: 1,
        path: `${tenant.id}/ad_spend/batch_01.csv`,
        coversTo: "2026-01-11",
        reasonCodes: ["batch_conflict"],
      },
    ]);
    expect(formatReport([report])[1]).toBe(
      "  conflicting_redelivery: ad_spend/batch 1 is loaded, but a later delivery with different content was quarantined (batch_conflict)",
    );
    // The loaded batch still read cost_usd as spend: a note and a finding on the same batch.
    expect(report.notes).toEqual([
      { kind: "declared_alias", source: "ad_spend", header: "cost_usd", column: "spend", batches: [1] },
    ]);
    expect(status).toBe(2);
  });

  it("reports loaded rows whose required value failed its cast", async () => {
    const tenant = tenantFor("badgross", ["orders"]);
    await put(
      tenant,
      "orders/batch_01.csv",
      "order_id,created_at,channel,gross,currency,customer_email\n" +
        "o-1,2026-01-06T00:00:00Z,direct,10.00,USD,a@example.com\n" +
        "o-2,2026-01-06T00:00:00Z,direct,12.5O,USD,b@example.com\n",
    );

    const { report, status } = await loadAndCheck(tenant, [
      entry(tenant, "orders", 1, "orders/batch_01.csv", "2026-01-11"),
    ]);
    expect(report.batches.map((b) => [b.batch, b.status])).toEqual([[1, "loaded"]]);
    expect(report.findings).toEqual([
      { kind: "invalid_rows", source: "orders", column: "gross", rows: 1, firstBatch: 1, firstLine: 3 },
    ]);
    expect(formatReport([report])[1]).toBe("  invalid_rows: orders.gross 1 row (first: batch 1 line 3)");
    expect(status).toBe(2);
  });

  it("reports email event types with no value map entry, which the marts never count", async () => {
    // Lumen's map covers CLICK, OPEN, DELIVERED and UNSUBSCRIBE; BOUNCE has no entry and stays raw.
    const tenant: TenantConfig = { ...tenantFor("unmapped", []), sources: { email_events: lumenSources.email_events } };
    const event = (id: string, type: string) =>
      `${JSON.stringify({ event_id: id, type, email: "a@example.com", campaign_id: "c-1", occurred_at: "2026-01-06T00:00:00Z" })}\n`;
    await put(tenant, "email_events/batch_01.ndjson", event("e-1", "OPEN") + event("e-2", "BOUNCE") + event("e-3", "BOUNCE"));

    const { report, status } = await loadAndCheck(tenant, [
      entry(tenant, "email_events", 1, "email_events/batch_01.ndjson", "2026-01-11"),
    ]);
    expect(report.findings).toEqual([
      {
        kind: "uncounted_values",
        source: "email_events",
        column: "type",
        canonical: ["delivered", "open", "click", "unsubscribe"],
        values: [{ value: "BOUNCE", rows: 2 }],
      },
    ]);
    expect(formatReport([report])[1]).toBe(
      '  uncounted_values: email_events.type "BOUNCE" 2 rows, not one of delivered, open, click, unsubscribe',
    );
    expect(status).toBe(2);
  });

  it("flags a configured source with no manifest entries and a listed source that is not configured", async () => {
    const tenant = tenantFor("mismatch", ["refunds", "orders"]);
    await put(tenant, "refunds/batch_01.csv", "refund_id,refunded_at,order_id,amount,currency\nrf-1,2026-01-06T00:00:00Z,o-1,1.00,USD\n");
    await put(tenant, "ad_spend/batch_01.csv", "date,campaign_id,platform,spend\n2026-01-06,c-1,facebook,1.00\n");

    const { report } = await loadAndCheck(tenant, [
      entry(tenant, "refunds", 1, "refunds/batch_01.csv", "2026-01-11"),
      entry(tenant, "ad_spend", 1, "ad_spend/batch_01.csv", "2026-01-11"),
    ]);
    expect(report.findings).toEqual([
      expect.objectContaining({ kind: "quarantined", source: "ad_spend", reasonCodes: ["source_not_configured"] }),
      { kind: "no_manifest_entries", source: "orders" },
      { kind: "source_not_configured", source: "ad_spend" },
    ]);
  });
});
