import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { exitStatus, formatValidation, type ScopeReport, validateTenants } from "../src/onboarding/validate.ts";
import { isSupplied } from "./supplied-tenants.ts";

// No database: every case builds its own tenants/ and fixtures/ under a temp root.
const roots: string[] = [];
afterAll(async () => {
  for (const root of roots) await rm(root, { recursive: true, force: true });
});

const acmeConfig = () => ({
  id: "acme",
  displayName: "Acme",
  currency: "USD",
  fixturesDir: "fixtures/acme",
  sources: {
    orders: { columnAliases: {}, valueMaps: { channel: { FB: "facebook", Google: "google" } } },
    email_events: { columnAliases: {}, valueMaps: { type: { OPEN: "open" } } },
    ad_spend: { columnAliases: { spend: ["cost_usd"] } },
  },
});

const acmeFiles = (): Record<string, string> => ({
  "acme/orders/batch_01.csv":
    "order_id,created_at,channel,gross,currency,customer_email\n" +
    "o1,2026-01-01T10:00:00Z,FB,10.00,USD,a@example.com\n" +
    "o2,2026-01-01T11:00:00Z,Google,20.00,USD,b@example.com\n",
  "acme/email_events/batch_01.ndjson":
    '{"event_id":"e1","type":"OPEN","email":"a@example.com","campaign_id":"c1","occurred_at":"2026-01-01T10:00:00Z"}\n' +
    '{"event_id":"e2","type":"click","email":"b@example.com","campaign_id":"c1","occurred_at":"2026-01-01T11:00:00Z"}\n',
  "acme/ad_spend/batch_01.csv": "date,campaign_id,platform,cost_usd\n2026-01-01,c1,facebook,5\n2026-01-01,c2,google,7\n",
});

const entry = (source: string, batch: number, path: string, tenant = "acme") => ({
  tenant,
  source,
  batch,
  path,
  covers_from: "2026-01-01",
  covers_to: "2026-01-01",
});

const acmeManifest = () => ({
  batches: [
    entry("orders", 1, "acme/orders/batch_01.csv"),
    entry("email_events", 1, "acme/email_events/batch_01.ndjson"),
    entry("ad_spend", 1, "acme/ad_spend/batch_01.csv"),
  ],
});

interface Workspace {
  /** File name in tenants/ -> content; an object is written as JSON. */
  tenants?: Record<string, unknown>;
  /** Path under fixtures/ -> content. */
  files?: Record<string, string>;
  manifest?: unknown;
}

async function validate({
  tenants = { "acme.json": acmeConfig() },
  files = acmeFiles(),
  manifest = acmeManifest(),
}: Workspace = {}): Promise<ScopeReport[]> {
  const root = await mkdtemp(join(tmpdir(), "validate-"));
  roots.push(root);
  const write = async (path: string, content: string) => {
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, content);
  };
  await mkdir(join(root, "tenants"), { recursive: true });
  for (const [name, content] of Object.entries(tenants)) {
    await write(join(root, "tenants", name), typeof content === "string" ? content : JSON.stringify(content));
  }
  for (const [path, content] of Object.entries(files)) await write(join(root, "fixtures", path), content);
  await write(join(root, "fixtures", "manifest.json"), JSON.stringify(manifest));
  return validateTenants({ rootDir: root });
}

/** `severity code: message` for every issue of a scope, info excluded. */
const problems = (reports: ScopeReport[], scope = "acme"): string[] =>
  (reports.find((r) => r.scope === scope)?.issues ?? [])
    .filter((i) => i.severity !== "info")
    .map((i) => `${i.severity} ${i.code}: ${i.message}`);

describe("validateTenants", () => {
  it("passes a clean tenant with no errors or warnings", async () => {
    const reports = await validate();
    expect(problems(reports)).toEqual([]);
    expect(exitStatus(reports)).toBe(0);
    expect(formatValidation(reports)).toEqual([
      "acme: 0 errors, 0 warnings",
      "  info values: ad_spend.platform facebook 1, google 1",
      "  info values: orders.channel facebook 1, google 1",
      "tenant:validate: 0 errors, 0 warnings",
    ]);
  });

  it("names the tenant file on a JSON syntax error", async () => {
    const reports = await validate({ tenants: { "acme.json": '{ "id": "acme", }' } });
    const [issue] = problems(reports);
    expect(issue).toMatch(/^error invalid_json: tenants\/acme\.json: /);
    expect(exitStatus(reports)).toBe(2);
  });

  it("lists every schema issue and an id that does not match the file name", async () => {
    const reports = await validate({
      tenants: {
        "acme.json": { ...acmeConfig(), currency: "usd", fixturesDir: "../acme" },
        "other.json": { ...acmeConfig(), fixturesDir: "fixtures/other" },
      },
    });
    expect(problems(reports)).toEqual([
      "error invalid_config: tenants/acme.json currency: ISO 4217 code",
      'error invalid_config: tenants/acme.json fixturesDir: must not contain ".." segments',
    ]);
    expect(problems(reports, "other")).toEqual([
      'error id_mismatch: tenants/other.json declares id "acme"; the file name must match the id',
    ]);
  });

  it("reports a header with no declared alias as a would-be quarantine", async () => {
    const config = acmeConfig();
    config.sources.ad_spend.columnAliases = {} as never;
    const reports = await validate({ tenants: { "acme.json": config } });
    expect(problems(reports)).toEqual([
      'error would_be_quarantined: ad_spend/batch 1 "acme/ad_spend/batch_01.csv" would be quarantined: ' +
        'unknown_header header "cost_usd" is neither a column of ad_spend nor a declared alias; ' +
        'missing_column column "spend" is missing',
    ]);
  });

  it("warns on a manifest path typo, suggesting the unlisted file next to it", async () => {
    const manifest = acmeManifest();
    manifest.batches[0] = entry("orders", 1, "acme/orders/batch_1.csv");
    const reports = await validate({ manifest });
    expect(problems(reports)).toEqual([
      'warning not_on_disk: orders/batch 1 "acme/orders/batch_1.csv" not on disk: not delivered yet, ' +
        "or a path typo? (similar: batch_01.csv)",
      "warning not_in_manifest: fixtures/acme/orders/batch_01.csv on disk but not in the manifest: it will not load",
    ]);
    expect(exitStatus(reports)).toBe(0);
  });

  it("rejects a manifest tenant that matches no tenant file, suggesting the closest id", async () => {
    const manifest = acmeManifest();
    manifest.batches.push(entry("refunds", 1, "acme/refunds/batch_01.csv", "Acme"));
    const reports = await validate({ manifest });
    expect(problems(reports, "fixtures/manifest.json")).toEqual([
      'error unknown_tenant: 1 entry for tenant "Acme", which has no tenants/Acme.json (did you mean "acme"?)',
    ]);
    expect(exitStatus(reports)).toBe(2);
  });

  it("rejects a date that matches the format but is not on the calendar", async () => {
    const manifest = acmeManifest();
    manifest.batches[0] = { ...entry("orders", 1, "acme/orders/batch_01.csv"), covers_to: "2026-02-30" };
    const reports = await validate({ manifest });
    expect(problems(reports)).toEqual(['error invalid_date: orders/batch 1 covers_to "2026-02-30" is not a calendar date']);
  });

  it("warns on an unmapped open-column value and on channels that do not join", async () => {
    const files = acmeFiles();
    files["acme/orders/batch_01.csv"] += "o3,2026-01-01T12:00:00Z,Fb,30.00,USD,c@example.com\n";
    files["acme/ad_spend/batch_01.csv"] += "2026-01-01,c3,tiktok,9\n";
    const reports = await validate({ files });
    expect(problems(reports)).toEqual([
      'warning unmapped_values: orders.channel "Fb" 1 row not in the value map: passed through unchanged',
      'warning channel_mismatch: orders.channel "Fb" not in ad_spend.platform: revenue with no spend in channel performance',
      'warning channel_mismatch: ad_spend.platform "tiktok" not in orders.channel: spend with no revenue in channel performance',
    ]);
    expect(exitStatus(reports)).toBe(0);
  });

  it("rejects an email type the marts would not count", async () => {
    const files = acmeFiles();
    files["acme/email_events/batch_01.ndjson"] +=
      '{"event_id":"e3","type":"CLICK","email":"c@example.com","campaign_id":"c1","occurred_at":"2026-01-01T12:00:00Z"}\n';
    const reports = await validate({ files });
    expect(problems(reports)).toEqual([
      'error uncounted_values: email_events.type "CLICK" 1 row would not be counted: not one of delivered, open, click, unsubscribe',
    ]);
    expect(exitStatus(reports)).toBe(2);
  });

  it("mirrors check on configured sources without entries and entries for unconfigured sources", async () => {
    const manifest = acmeManifest();
    manifest.batches = manifest.batches.filter((e) => e.source !== "email_events");
    manifest.batches.push(entry("refunds", 2, "acme/refunds/batch_02.csv"));
    const reports = await validate({ manifest });
    expect(problems(reports)).toEqual([
      "warning no_manifest_entries: email_events is configured but the manifest lists no batch for it",
      "error source_not_configured: the manifest lists refunds, which this tenant has not configured: its files would be quarantined",
      "warning batch_gap: refunds manifest skips batch 1",
      'warning not_on_disk: refunds/batch 2 "acme/refunds/batch_02.csv" not on disk: not delivered yet, or a path typo?',
      "warning not_in_manifest: fixtures/acme/email_events/batch_01.ndjson on disk but not in the manifest: it will not load",
    ]);
  });

  it("rejects a manifest path outside the tenant's fixturesDir", async () => {
    const manifest = acmeManifest();
    manifest.batches[0] = entry("orders", 1, "other/orders/batch_01.csv");
    const reports = await validate({ manifest });
    expect(problems(reports)[0]).toBe(
      'error path_outside: orders/batch 1 "other/orders/batch_01.csv" is outside fixtures/acme: the load would stop before any file',
    );
  });
});

describe("the supplied tenants", () => {
  it("validate with no errors against the real tenants/ and fixtures/", async () => {
    const reports = (await validateTenants()).filter((r) => isSupplied({ id: r.scope }));
    expect(exitStatus(reports)).toBe(0);
    // lumen's ad_spend batch 3 is listed but was never delivered (fixtures/README.md), and
    // neither tenant spends on "direct", so that revenue has no spend row to join.
    expect(reports.map((r) => [r.scope, problems(reports, r.scope)])).toEqual([
      [
        "lumen",
        [
          'warning not_on_disk: ad_spend/batch 3 "lumen/ad_spend/batch_03.csv" not on disk: not delivered yet, or a path typo?',
          'warning channel_mismatch: orders.channel "direct" not in ad_spend.platform: revenue with no spend in channel performance',
        ],
      ],
      [
        "northwind",
        [
          'warning channel_mismatch: orders.channel "direct" not in ad_spend.platform: revenue with no spend in channel performance',
        ],
      ],
    ]);
  });
});
