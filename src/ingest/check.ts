import { fileURLToPath } from "node:url";
import type pg from "pg";
import { SOURCE_NAMES, type SourceName } from "../config/sources.ts";
import { loadTenants, type TenantConfig } from "../config/tenants.ts";
import { closePools, getAppPool } from "../db/pool.ts";
import { withTenant } from "../db/tenant-scope.ts";
import { describeError } from "./loader.ts";
import { loadManifest, type Manifest, type ManifestEntry, manifestSchema } from "./manifest.ts";

const DEFAULT_MANIFEST = fileURLToPath(new URL("../../fixtures/manifest.json", import.meta.url));
const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;

/** What the ledger says about one manifest batch. */
export type BatchStatus = "loaded" | "quarantined" | "not_received";

export interface BatchHealth {
  source: SourceName;
  batch: number;
  path: string;
  coversTo: string;
  status: BatchStatus;
  /** For `quarantined`: the reason codes of the latest attempt. */
  reasonCodes?: string[];
}

/**
 * Something a scheduler should alert on:
 * - not_received: a manifest batch with no ledger row at all (never arrived, or never loaded).
 * - quarantined: attempted, never loaded.
 * - stale: a configured source whose loaded data ends before `asOf` (`freshThrough` null: nothing loaded).
 * - no_manifest_entries: a configured source the manifest lists no batch for.
 * - source_not_configured: the manifest lists batches for a source the tenant has not configured.
 */
export type Finding =
  | ({ kind: "not_received" } & Omit<BatchHealth, "status" | "reasonCodes">)
  | ({ kind: "quarantined"; reasonCodes: string[] } & Omit<BatchHealth, "status" | "reasonCodes">)
  | { kind: "stale"; source: SourceName; freshThrough: string | null; asOf: string }
  | { kind: "no_manifest_entries"; source: SourceName }
  | { kind: "source_not_configured"; source: SourceName };

export interface TenantHealth {
  tenant: string;
  asOf: string;
  batches: BatchHealth[];
  /** Latest covers_to among loaded batches, per configured source; null when none is loaded. */
  freshness: Partial<Record<SourceName, string | null>>;
  findings: Finding[];
}

export interface CheckOptions {
  /** Tenants to check; manifest entries for any other tenant are ignored. */
  tenants: TenantConfig[];
  /** A parsed manifest or the path of one. Defaults to fixtures/manifest.json. */
  manifest?: Manifest | string;
  /** Reads the ledger as this pool's role, one tenant scope at a time. Defaults to the app pool. */
  pool?: pg.Pool;
  /**
   * YYYY-MM-DD date every configured source should be loaded through. Defaults, per tenant,
   * to the latest covers_to among that tenant's manifest entries, which only catches a
   * source lagging behind the others; a scheduler should pass the date it expects data for.
   */
  asOf?: string;
}

interface LedgerRow {
  source: SourceName;
  batch_no: number;
  status: "loaded" | "quarantined";
  detail: { reasons?: { code: string }[] } | null;
}

/**
 * Compares each tenant's manifest with its ledger (ops.batch_file), read inside that
 * tenant's scope, so row-level security keeps one tenant's rows out of another's report.
 * Read-only. Without `pool`, the shared application pool is used and the caller must
 * call `closePools()` when done, as the CLI below does.
 */
export async function checkSources({
  tenants,
  manifest,
  pool = getAppPool(),
  asOf,
}: CheckOptions): Promise<TenantHealth[]> {
  if (asOf !== undefined && !ISO_DATE.test(asOf)) throw new Error(`asOf must be YYYY-MM-DD, got "${asOf}"`);
  const parsedManifest =
    typeof manifest === "object" ? manifestSchema.parse(manifest) : await loadManifest(manifest ?? DEFAULT_MANIFEST);

  const reports: TenantHealth[] = [];
  for (const tenant of tenants) {
    const entries = parsedManifest.batches
      .filter((e) => e.tenant === tenant.id)
      .sort((a, b) => SOURCE_NAMES.indexOf(a.source) - SOURCE_NAMES.indexOf(b.source) || a.batch - b.batch);
    const ledger = await withTenant(
      tenant.id,
      async (client) =>
        (
          await client.query<LedgerRow>(
            "SELECT source, batch_no, status, detail FROM ops.batch_file ORDER BY source, batch_no, id",
          )
        ).rows,
      pool,
    );
    reports.push(assess(tenant, entries, ledger, asOf));
  }
  return reports;
}

const latest = (dates: string[]): string | null => dates.reduce<string | null>((a, d) => (a === null || d > a ? d : a), null);

function assess(tenant: TenantConfig, entries: ManifestEntry[], ledger: LedgerRow[], asOfOption?: string): TenantHealth {
  const asOf = asOfOption ?? latest(entries.map((e) => e.covers_to)) ?? "";
  const findings: Finding[] = [];

  const batches = entries.map((entry): BatchHealth => {
    const attempts = ledger.filter((r) => r.source === entry.source && r.batch_no === entry.batch);
    const base = { source: entry.source, batch: entry.batch, path: entry.path, coversTo: entry.covers_to };
    if (attempts.some((r) => r.status === "loaded")) return { ...base, status: "loaded" };
    const last = attempts.at(-1);
    if (last === undefined) {
      findings.push({ kind: "not_received", ...base });
      return { ...base, status: "not_received" };
    }
    const reasonCodes = [...new Set((last.detail?.reasons ?? []).map((r) => r.code))];
    findings.push({ kind: "quarantined", ...base, reasonCodes });
    return { ...base, status: "quarantined", reasonCodes };
  });

  const configured = SOURCE_NAMES.filter((s) => tenant.sources[s] !== undefined);
  const freshness: TenantHealth["freshness"] = {};
  for (const source of configured) {
    const listed = batches.filter((b) => b.source === source);
    if (listed.length === 0) {
      findings.push({ kind: "no_manifest_entries", source });
      continue;
    }
    const freshThrough = latest(listed.filter((b) => b.status === "loaded").map((b) => b.coversTo));
    freshness[source] = freshThrough;
    if (freshThrough === null || freshThrough < asOf) findings.push({ kind: "stale", source, freshThrough, asOf });
  }
  for (const source of new Set(entries.map((e) => e.source))) {
    if (!configured.includes(source)) findings.push({ kind: "source_not_configured", source });
  }

  return { tenant: tenant.id, asOf, batches, freshness, findings };
}

/**
 * The process exit code for a run, which is what a scheduler alerts on:
 * - 0: every manifest batch is loaded and every configured source is current.
 * - 2: findings (a batch not received or quarantined, a stale or unlisted source).
 * - 1 (set by the CLI, never returned here): the check itself failed, e.g. a bad
 *   manifest or an unreachable database, so nothing is known about the data.
 */
export const exitStatus = (reports: TenantHealth[]): 0 | 2 => (reports.some((r) => r.findings.length > 0) ? 2 : 0);

function formatFinding(f: Finding): string {
  switch (f.kind) {
    case "not_received":
      return `not_received: ${f.source}/batch ${f.batch} (${f.path}, covers through ${f.coversTo})`;
    case "quarantined":
      return `quarantined: ${f.source}/batch ${f.batch} (${f.reasonCodes.join(", ") || "no reasons recorded"})`;
    case "stale":
      return `stale: ${f.source} loaded through ${f.freshThrough ?? "nothing"}, expected ${f.asOf}`;
    case "no_manifest_entries":
      return `no_manifest_entries: ${f.source} is configured but the manifest lists no batch for it`;
    case "source_not_configured":
      return `source_not_configured: the manifest lists ${f.source}, which this tenant has not configured`;
  }
}

/** A summary line per tenant, followed by one indented line per finding. */
export function formatReport(reports: TenantHealth[]): string[] {
  return reports.flatMap((r) => {
    const loaded = r.batches.filter((b) => b.status === "loaded").length;
    const state = r.findings.length === 0 ? "healthy" : `${r.findings.length} finding${r.findings.length === 1 ? "" : "s"}`;
    return [
      `${r.tenant}: ${state} (${loaded}/${r.batches.length} batches loaded, as of ${r.asOf || "n/a"})`,
      ...r.findings.map((f) => `  ${formatFinding(f)}`),
    ];
  });
}

// CLI: `pnpm check`, optionally with CHECK_AS_OF=YYYY-MM-DD and MANIFEST=<path>; exit codes as in exitStatus.
if (import.meta.main) {
  try {
    const asOf = process.env.CHECK_AS_OF || undefined;
    const reports = await checkSources({
      tenants: await loadTenants(),
      manifest: process.env.MANIFEST || DEFAULT_MANIFEST,
      ...(asOf === undefined ? {} : { asOf }),
    });
    for (const line of formatReport(reports)) console.log(line);
    process.exitCode = exitStatus(reports);
  } catch (error) {
    console.error(describeError(error));
    process.exitCode = 1;
  } finally {
    await closePools();
  }
}
