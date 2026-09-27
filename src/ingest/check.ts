import { fileURLToPath } from "node:url";
import type pg from "pg";
import { type ClosedColumn, canonicalValues, SOURCE_COLUMNS, SOURCE_NAMES, type SourceName } from "../config/sources.ts";
import { loadTenants, type TenantConfig } from "../config/tenants.ts";
import { closePools, getAppPool } from "../db/pool.ts";
import { withTenant } from "../db/tenant-scope.ts";
import { describeError, headerAdaptations } from "./loader.ts";
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
 * - conflicting_redelivery: loaded, but a later attempt delivered different bytes for the
 *   same batch and was quarantined (batch_conflict). The loaded numbers may be superseded
 *   by data nobody has loaded yet, so the replaced batch must not look healthy.
 * - stale: a configured source whose loaded data ends before `asOf` (`freshThrough` null: nothing loaded).
 * - no_manifest_entries: a configured source the manifest lists no batch for.
 * - source_not_configured: the manifest lists batches for a source the tenant has not configured.
 * - invalid_rows: loaded lines whose required column is missing or fails its cast
 *   (staging.invalid_rows), per source and column. Staging nulls the value or drops the
 *   row, so the marts under-count without failing. Superseded lines count too, as in the view.
 * - uncounted_values: values of a closed column (CANONICAL_VALUES) outside its canonical
 *   set, e.g. an email event type with no value map entry. The marts count only canonical
 *   values, so these rows are loaded but never reported.
 */
export type Finding =
  | ({ kind: "not_received" } & Omit<BatchHealth, "status" | "reasonCodes">)
  | ({ kind: "quarantined"; reasonCodes: string[] } & Omit<BatchHealth, "status" | "reasonCodes">)
  | ({ kind: "conflicting_redelivery"; reasonCodes: string[] } & Omit<BatchHealth, "status" | "reasonCodes">)
  | { kind: "stale"; source: SourceName; freshThrough: string | null; asOf: string }
  | { kind: "no_manifest_entries"; source: SourceName }
  | { kind: "source_not_configured"; source: SourceName }
  | { kind: "invalid_rows"; source: SourceName; column: string; rows: number; firstBatch: number; firstLine: number }
  | {
      kind: "uncounted_values";
      source: SourceName;
      column: string;
      canonical: readonly string[];
      /** Staged rows per value outside `canonical`, most frequent first. */
      values: { value: string; rows: number }[];
    };

/**
 * Something worth knowing that is not a problem, so it never changes the exit status:
 * - declared_alias: loaded batches that read a header through an alias the tenant declared
 *   (ops.batch_file.detail.columns), e.g. `cost_usd` read as `spend`. The drift is expected,
 *   but it stays visible. `batches` are ascending batch numbers of that source.
 */
export type Note = { kind: "declared_alias"; source: SourceName; header: string; column: string; batches: number[] };

export interface TenantHealth {
  tenant: string;
  asOf: string;
  batches: BatchHealth[];
  /** Latest covers_to among loaded batches, per configured source; null when none is loaded. */
  freshness: Partial<Record<SourceName, string | null>>;
  findings: Finding[];
  notes: Note[];
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
  detail: { reasons?: { code: string }[]; columns?: Record<string, string> } | null;
}

interface InvalidRowsRow {
  source: SourceName;
  column_name: string;
  rows: number;
  first_batch: number;
  first_line: number;
}

/**
 * Where each closed column's mapped value lands in staging, which is what the marts read.
 * Keyed by ClosedColumn, so declaring a new closed column without saying where to look
 * for it fails to compile. Identifiers come only from this constant, never from input.
 */
const STAGED_COLUMN: Record<ClosedColumn, { view: string; column: string }> = {
  "email_events.type": { view: "staging.email_events", column: "event_type" },
};

/** Data-quality findings from staging, read in the caller's tenant scope. */
async function stagingFindings(client: pg.PoolClient): Promise<Finding[]> {
  const findings: Finding[] = [];
  const invalid = (
    await client.query<InvalidRowsRow>(
      `SELECT source, column_name, count(*)::integer AS rows,
              (array_agg(batch_no ORDER BY batch_no, line_no))[1] AS first_batch,
              (array_agg(line_no ORDER BY batch_no, line_no))[1] AS first_line
       FROM staging.invalid_rows GROUP BY source, column_name`,
    )
  ).rows;
  const columnOrder = (r: InvalidRowsRow) => (SOURCE_COLUMNS[r.source] as readonly string[]).indexOf(r.column_name);
  invalid.sort((a, b) => SOURCE_NAMES.indexOf(a.source) - SOURCE_NAMES.indexOf(b.source) || columnOrder(a) - columnOrder(b));
  for (const r of invalid) {
    findings.push({
      kind: "invalid_rows",
      source: r.source,
      column: r.column_name,
      rows: r.rows,
      firstBatch: r.first_batch,
      firstLine: r.first_line,
    });
  }

  for (const source of SOURCE_NAMES) {
    for (const [column, canonical] of Object.entries(canonicalValues(source))) {
      if (canonical === undefined) continue;
      const staged = STAGED_COLUMN[`${source}.${column}` as ClosedColumn];
      // A missing value is already an invalid_rows finding; only present, non-canonical ones count here.
      const values = (
        await client.query<{ value: string; rows: number }>(
          `SELECT ${staged.column} AS value, count(*)::integer AS rows FROM ${staged.view}
           WHERE ${staged.column} IS NOT NULL AND NOT (${staged.column} = ANY($1::text[]))
           GROUP BY 1 ORDER BY 2 DESC, 1`,
          [canonical],
        )
      ).rows;
      if (values.length > 0) findings.push({ kind: "uncounted_values", source, column, canonical, values });
    }
  }
  return findings;
}

/**
 * Compares each tenant's manifest with its ledger (ops.batch_file), read inside that
 * tenant's scope, so row-level security keeps one tenant's rows out of another's report.
 * In the same scope it reads staging for data-quality findings (invalid_rows, uncounted_values).
 * The ledger's loaded rows also yield notes on headers read through declared aliases.
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
    const { ledger, quality } = await withTenant(
      tenant.id,
      async (client) => ({
        ledger: (
          await client.query<LedgerRow>(
            "SELECT source, batch_no, status, detail FROM ops.batch_file ORDER BY source, batch_no, id",
          )
        ).rows,
        quality: await stagingFindings(client),
      }),
      pool,
    );
    const report = assess(tenant, entries, ledger, asOf);
    report.findings.push(...quality);
    reports.push(report);
  }
  return reports;
}

const latest = (dates: string[]): string | null => dates.reduce<string | null>((a, d) => (a === null || d > a ? d : a), null);

const codesOf = (row: LedgerRow): string[] => [...new Set((row.detail?.reasons ?? []).map((r) => r.code))];

function assess(tenant: TenantConfig, entries: ManifestEntry[], ledger: LedgerRow[], asOfOption?: string): TenantHealth {
  const asOf = asOfOption ?? latest(entries.map((e) => e.covers_to)) ?? "";
  const findings: Finding[] = [];

  const batches = entries.map((entry): BatchHealth => {
    const attempts = ledger.filter((r) => r.source === entry.source && r.batch_no === entry.batch);
    const base = { source: entry.source, batch: entry.batch, path: entry.path, coversTo: entry.covers_to };
    const loadedAt = attempts.findIndex((r) => r.status === "loaded");
    if (loadedAt !== -1) {
      // Ledger rows are in attempt order (by id), so these came after the load.
      const conflict = attempts
        .slice(loadedAt + 1)
        .filter((r) => r.status === "quarantined" && codesOf(r).includes("batch_conflict"))
        .at(-1);
      if (conflict !== undefined) findings.push({ kind: "conflicting_redelivery", ...base, reasonCodes: codesOf(conflict) });
      return { ...base, status: "loaded" };
    }
    const last = attempts.at(-1);
    if (last === undefined) {
      findings.push({ kind: "not_received", ...base });
      return { ...base, status: "not_received" };
    }
    const reasonCodes = codesOf(last);
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

  return { tenant: tenant.id, asOf, batches, freshness, findings, notes: aliasNotes(ledger) };
}

/**
 * One declared_alias note per source, header and column, from the column maps of loaded
 * ledger rows. Read from the ledger rather than the manifest, so a batch that never loaded
 * is left out of the batch list. Sources in SOURCE_NAMES order, headers as first seen.
 */
function aliasNotes(ledger: LedgerRow[]): Note[] {
  const notes = new Map<string, Note>();
  for (const row of ledger) {
    if (row.status !== "loaded") continue;
    for (const { header, column } of headerAdaptations(row.detail?.columns ?? {})) {
      const key = JSON.stringify([row.source, header, column]);
      const note = notes.get(key) ?? { kind: "declared_alias", source: row.source, header, column, batches: [] };
      if (!note.batches.includes(row.batch_no)) note.batches.push(row.batch_no);
      notes.set(key, note);
    }
  }
  for (const note of notes.values()) note.batches.sort((a, b) => a - b);
  return [...notes.values()].sort((a, b) => SOURCE_NAMES.indexOf(a.source) - SOURCE_NAMES.indexOf(b.source));
}

/** Ascending batch numbers as runs, e.g. [1, 3, 4, 5] -> "batches 1, 3-5", [4] -> "batch 4". */
function batchRanges(batches: number[]): string {
  const runs: [number, number][] = [];
  for (const batch of batches) {
    const last = runs.at(-1);
    if (last !== undefined && batch === last[1] + 1) last[1] = batch;
    else runs.push([batch, batch]);
  }
  const text = runs.map(([first, end]) => (first === end ? `${first}` : `${first}-${end}`)).join(", ");
  return `${batches.length === 1 ? "batch" : "batches"} ${text}`;
}

/**
 * The process exit code for a run, which is what a scheduler alerts on:
 * - 0: every manifest batch is loaded, every configured source is current and staging
 *   has no invalid rows or uncounted values.
 * - 2: findings (a batch not received, quarantined or redelivered with other content, a
 *   stale or unlisted source, invalid rows or values the marts do not count).
 * - 1 (set by the CLI, never returned here): the check itself failed, e.g. a bad
 *   manifest or an unreachable database, so nothing is known about the data.
 * Notes (e.g. a declared alias in use) are expected drift and never affect it.
 */
export const exitStatus = (reports: TenantHealth[]): 0 | 2 => (reports.some((r) => r.findings.length > 0) ? 2 : 0);

function formatNote(n: Note): string {
  return `note: ${n.source} ${batchRanges(n.batches)} read header "${n.header}" as ${n.column} (declared alias)`;
}

function formatFinding(f: Finding): string {
  switch (f.kind) {
    case "not_received":
      return `not_received: ${f.source}/batch ${f.batch} (${f.path}, covers through ${f.coversTo})`;
    case "quarantined":
      return `quarantined: ${f.source}/batch ${f.batch} (${f.reasonCodes.join(", ") || "no reasons recorded"})`;
    case "conflicting_redelivery":
      return `conflicting_redelivery: ${f.source}/batch ${f.batch} is loaded, but a later delivery with different content was quarantined (${f.reasonCodes.join(", ")})`;
    case "stale":
      return `stale: ${f.source} loaded through ${f.freshThrough ?? "nothing"}, expected ${f.asOf}`;
    case "no_manifest_entries":
      return `no_manifest_entries: ${f.source} is configured but the manifest lists no batch for it`;
    case "source_not_configured":
      return `source_not_configured: the manifest lists ${f.source}, which this tenant has not configured`;
    case "invalid_rows":
      return `invalid_rows: ${f.source}.${f.column} ${f.rows} row${f.rows === 1 ? "" : "s"} (first: batch ${f.firstBatch} line ${f.firstLine})`;
    case "uncounted_values": {
      const values = f.values.map((v) => `"${v.value}" ${v.rows} row${v.rows === 1 ? "" : "s"}`).join(", ");
      return `uncounted_values: ${f.source}.${f.column} ${values}, not one of ${f.canonical.join(", ")}`;
    }
  }
}

/**
 * A summary line per tenant, followed by one indented line per finding and then one per
 * note. Notes are not findings, so the summary's count and "healthy" leave them out.
 */
export function formatReport(reports: TenantHealth[]): string[] {
  return reports.flatMap((r) => {
    const loaded = r.batches.filter((b) => b.status === "loaded").length;
    const state = r.findings.length === 0 ? "healthy" : `${r.findings.length} finding${r.findings.length === 1 ? "" : "s"}`;
    return [
      `${r.tenant}: ${state} (${loaded}/${r.batches.length} batches loaded, as of ${r.asOf || "n/a"})`,
      ...r.findings.map((f) => `  ${formatFinding(f)}`),
      ...r.notes.map((n) => `  ${formatNote(n)}`),
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
