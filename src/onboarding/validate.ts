import { readdir, readFile, realpath } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { z } from "zod";
import { canonicalValues, SOURCE_NAMES, type SourceColumn, type SourceName } from "../config/sources.ts";
import { type TenantConfig, tenantConfigSchema } from "../config/tenants.ts";
import { describeError, isInside } from "../ingest/loader.ts";
import { type Manifest, type ManifestEntry, manifestSchema } from "../ingest/manifest.ts";
import { parseBatchFile, type RawRecord } from "../ingest/parse.ts";

const DEFAULT_ROOT_DIR = fileURLToPath(new URL("../../", import.meta.url));

/**
 * - error: the tenant would fail to migrate or load, or would load numbers that are silently
 *   wrong (a quarantined file, a value the marts never count).
 * - warning: worth a look before loading (a file not delivered yet, an unmapped spelling).
 * - info: context for the reviewer, such as the distinct channel values; never a problem.
 */
export type Severity = "error" | "warning" | "info";

export interface Issue {
  severity: Severity;
  /** Stable and greppable, e.g. "not_on_disk". */
  code: string;
  message: string;
}

/** The issues of one tenant, or of the manifest itself (`scope` is then its path). */
export interface ScopeReport {
  scope: string;
  issues: Issue[];
}

export interface ValidateOptions {
  /** Tenant ids to validate. Empty or absent: every `*.json` in the tenants directory. */
  ids?: readonly string[];
  /** Directory that tenant `fixturesDir` values are relative to: the repository root. */
  rootDir?: string;
  /** Directory of the tenant configs. Defaults to `<rootDir>/tenants`. */
  tenantsDir?: string;
  /** Directory that manifest paths are relative to. Defaults to `<rootDir>/fixtures`. */
  fixturesRoot?: string;
  /** A manifest object or the path of one. Defaults to `<fixturesRoot>/manifest.json`. */
  manifest?: unknown;
}

/**
 * The columns staging passes through `staging.map_value` (migrations/006_staging.sql). A
 * value map on any other column is accepted by the config schema but never applied.
 */
const MAPPED_COLUMNS: { [S in SourceName]?: readonly SourceColumn<S>[] } = {
  orders: ["channel"],
  email_events: ["type"],
  ad_spend: ["platform"],
};

/** The two open columns marts.daily_channel_performance joins on (migrations/007_marts.sql). */
const CHANNEL_JOIN = { revenue: "orders.channel", spend: "ad_spend.platform" } as const;

// Code-point order, as the loader sorts, so the report reads the same on every machine.
const compareText = (a: string, b: string): number => (a < b ? -1 : a > b ? 1 : 0);
const plural = (n: number, word: string): string => `${n} ${word}${n === 1 ? "" : "s"}`;
const quoteList = (values: Iterable<string>): string => [...values].map((v) => `"${v}"`).join(", ");

/** Levenshtein distance, for "did you mean" suggestions on short names. */
function editDistance(a: string, b: string): number {
  let previous = Array.from({ length: b.length + 1 }, (_, j) => j);
  for (let i = 1; i <= a.length; i++) {
    const current = [i];
    for (let j = 1; j <= b.length; j++) {
      const substitution = (previous[j - 1] ?? 0) + (a[i - 1] === b[j - 1] ? 0 : 1);
      current.push(Math.min((previous[j] ?? 0) + 1, (current[j - 1] ?? 0) + 1, substitution));
    }
    previous = current;
  }
  return previous[b.length] ?? 0;
}

/** Candidates that look like a typo of `name`: equal ignoring case, or at most two edits away. */
function nearMisses(name: string, candidates: Iterable<string>): string[] {
  return [...candidates]
    .filter((c) => c !== name && (c.toLowerCase() === name.toLowerCase() || editDistance(c, name) <= 2))
    .sort((a, b) => editDistance(a, name) - editDistance(b, name) || compareText(a, b));
}

const didYouMean = (name: string, candidates: Iterable<string>): string => {
  const [best] = nearMisses(name, candidates);
  return best === undefined ? "" : ` (did you mean "${best}"?)`;
};

/** True for a YYYY-MM-DD string naming a real day; the manifest schema checks only the format. */
const isCalendarDate = (value: string): boolean => {
  const date = new Date(`${value}T00:00:00Z`);
  return !Number.isNaN(date.getTime()) && date.toISOString().slice(0, 10) === value;
};

/** A zod issue as `path: message`, one line each, so every problem of a file is listed. */
const formatZodIssue = (issue: z.core.$ZodIssue): string =>
  `${issue.path.length === 0 ? "(root)" : issue.path.map(String).join(".")}: ${issue.message}`;

/**
 * The text staging reads for a canonical column: among the raw keys mapped to it, in key
 * order, the first value that is not blank after trimming spaces (staging.column_value
 * uses `payload ->> key` and `btrim`). Undefined when every candidate is blank.
 */
function columnValue(record: RawRecord, columns: Record<string, string>, canonical: string): string | undefined {
  const keys = Object.keys(columns)
    .filter((key) => columns[key] === canonical)
    .sort(compareText);
  for (const key of keys) {
    const value = record.payload[key];
    if (value === null || value === undefined) continue;
    // An object or array is tallied under its JSON.stringify text, which need not be the text
    // staging reads: `->>` on jsonb prints `{"a": 1}` for `{"a":1}` and orders keys shortest
    // first. So such a value is not value-compared with staging. The mapped columns (channel,
    // type, platform) only ever hold scalars today, so no current output depends on this.
    const text = (typeof value === "string" ? value : typeof value === "object" ? JSON.stringify(value) : String(value))
      // btrim with no characters argument removes spaces only, not tabs or newlines.
      .replace(/^ +| +$/g, "");
    if (text !== "") return text;
  }
  return undefined;
}

/** A raw value through the tenant's value map: exact match, or unchanged when unmapped (staging.map_value). */
const mapValue = (map: Readonly<Record<string, string>> | undefined, raw: string): string =>
  map !== undefined && Object.hasOwn(map, raw) ? (map[raw] ?? raw) : raw;

/** Raw value -> rows, per mapped column of one tenant, across every file that would load. */
type ValueTally = Map<`${SourceName}.${string}`, Map<string, number>>;

interface Context {
  root: string;
  fixtures: string;
  /** Shows a path relative to the repository root when it is inside it. */
  display: (path: string) => string;
}

/**
 * Validates tenant configs, their manifest entries and their batch files without a
 * database, so an onboarding mistake shows up before `pnpm migrate` or `pnpm load`.
 * Reads files only; it never opens a connection.
 *
 * Returns the manifest's own issues first (scope: the manifest path), when it has any, and
 * then one report per selected tenant, in id order. Issues within a scope keep check order:
 * config, manifest entries, files on disk, file contents.
 */
export async function validateTenants({
  ids = [],
  rootDir = DEFAULT_ROOT_DIR,
  tenantsDir,
  fixturesRoot,
  manifest,
}: ValidateOptions = {}): Promise<ScopeReport[]> {
  const root = resolve(rootDir);
  const configDir = resolve(root, tenantsDir ?? "tenants");
  const fixtures = resolve(root, fixturesRoot ?? "fixtures");
  const display = (path: string): string => {
    const rel = relative(root, path);
    return rel !== "" && !isAbsolute(rel) && !rel.startsWith("..") ? rel : path;
  };
  const ctx: Context = { root, fixtures, display };

  const knownIds = (await readdir(configDir))
    .filter((f) => f.endsWith(".json"))
    .map((f) => basename(f, ".json"))
    .sort(compareText);
  const selected = ids.length === 0 ? knownIds : [...new Set(ids)].sort(compareText);

  const manifestPath = typeof manifest === "string" ? resolve(manifest) : join(fixtures, "manifest.json");
  const manifestScope: ScopeReport = {
    scope: manifest === undefined || typeof manifest === "string" ? display(manifestPath) : "manifest",
    issues: [],
  };
  const parsedManifest = await readManifest(manifest, manifestPath, manifestScope);
  if (parsedManifest !== undefined) {
    const unknown = new Map<string, number>();
    for (const entry of parsedManifest.batches) {
      if (!knownIds.includes(entry.tenant)) unknown.set(entry.tenant, (unknown.get(entry.tenant) ?? 0) + 1);
    }
    for (const [tenant, count] of [...unknown].sort(([a], [b]) => compareText(a, b))) {
      manifestScope.issues.push({
        severity: "error",
        code: "unknown_tenant",
        message:
          `${count} ${count === 1 ? "entry" : "entries"} for tenant "${tenant}", which has no ` +
          `${display(join(configDir, `${tenant}.json`))}${didYouMean(tenant, knownIds)}`,
      });
    }
  }

  const reports: ScopeReport[] = manifestScope.issues.length > 0 ? [manifestScope] : [];
  for (const id of selected) {
    const report: ScopeReport = { scope: id, issues: [] };
    reports.push(report);
    const tenant = await readTenant(id, configDir, knownIds, report, ctx);
    if (tenant === undefined || parsedManifest === undefined) continue;
    const entries = parsedManifest.batches
      .filter((e) => e.tenant === id)
      .sort((a, b) => compareText(a.source, b.source) || a.batch - b.batch);
    checkEntries(tenant, entries, report);
    await checkFiles(tenant, entries, report, ctx);
  }
  return reports;
}

async function readManifest(manifest: unknown, path: string, report: ScopeReport): Promise<Manifest | undefined> {
  let raw: unknown = manifest;
  if (manifest === undefined || typeof manifest === "string") {
    try {
      raw = JSON.parse(await readFile(path, "utf8"));
    } catch (error) {
      const code = error instanceof SyntaxError ? "invalid_json" : "unreadable";
      report.issues.push({
        severity: "error",
        code,
        message: `${report.scope}: ${describeError(error)}; manifest entries and files were not checked`,
      });
      return undefined;
    }
  }
  const parsed = manifestSchema.safeParse(raw);
  if (parsed.success) return parsed.data;
  for (const issue of parsed.error.issues) {
    report.issues.push({ severity: "error", code: "invalid_manifest", message: formatZodIssue(issue) });
  }
  report.issues.push({
    severity: "info",
    code: "skipped",
    message: "manifest entries and files were not checked until the manifest is valid",
  });
  return undefined;
}

/** Parses and validates one tenant file, listing every problem; undefined when it cannot be trusted. */
async function readTenant(
  id: string,
  configDir: string,
  knownIds: readonly string[],
  report: ScopeReport,
  { display }: Context,
): Promise<TenantConfig | undefined> {
  const path = join(configDir, `${id}.json`);
  const name = display(path);
  if (!knownIds.includes(id)) {
    report.issues.push({ severity: "error", code: "no_tenant_file", message: `${name} does not exist${didYouMean(id, knownIds)}` });
    return undefined;
  }
  let raw: unknown;
  try {
    raw = JSON.parse(await readFile(path, "utf8"));
  } catch (error) {
    report.issues.push({ severity: "error", code: "invalid_json", message: `${name}: ${describeError(error)}` });
    return undefined;
  }
  const parsed = tenantConfigSchema.safeParse(raw);
  if (!parsed.success) {
    for (const issue of parsed.error.issues) {
      report.issues.push({ severity: "error", code: "invalid_config", message: `${name} ${formatZodIssue(issue)}` });
    }
    return undefined;
  }
  if (parsed.data.id !== id) {
    report.issues.push({
      severity: "error",
      code: "id_mismatch",
      message: `${name} declares id "${parsed.data.id}"; the file name must match the id`,
    });
    return undefined;
  }
  return parsed.data;
}

/** Checks on the manifest entries alone: dates, batch numbering, and sources against the config. */
function checkEntries(tenant: TenantConfig, entries: readonly ManifestEntry[], report: ScopeReport): void {
  const configured = SOURCE_NAMES.filter((s) => tenant.sources[s] !== undefined);
  for (const entry of entries) {
    for (const field of ["covers_from", "covers_to"] as const) {
      if (!isCalendarDate(entry[field])) {
        report.issues.push({
          severity: "error",
          code: "invalid_date",
          message: `${entry.source}/batch ${entry.batch} ${field} "${entry[field]}" is not a calendar date`,
        });
      }
    }
  }
  for (const source of SOURCE_NAMES) {
    const batches = entries.filter((e) => e.source === source).map((e) => e.batch);
    if (batches.length === 0) {
      if (configured.includes(source)) {
        report.issues.push({
          severity: "warning",
          code: "no_manifest_entries",
          message: `${source} is configured but the manifest lists no batch for it`,
        });
      }
      continue;
    }
    if (!configured.includes(source)) {
      report.issues.push({
        severity: "error",
        code: "source_not_configured",
        message: `the manifest lists ${source}, which this tenant has not configured: its files would be quarantined`,
      });
    }
    // Batches are numbered from 1; a number the manifest skips is usually a forgotten entry.
    const missing: number[] = [];
    for (let n = 1; n < Math.max(...batches); n++) if (!batches.includes(n)) missing.push(n);
    if (missing.length > 0) {
      report.issues.push({
        severity: "warning",
        code: "batch_gap",
        message: `${source} manifest skips ${missing.length === 1 ? "batch" : "batches"} ${missing.join(", ")}`,
      });
    }
  }
}

/** Checks each listed file on disk, the files nobody listed, and the contents of what would load. */
async function checkFiles(
  tenant: TenantConfig,
  entries: readonly ManifestEntry[],
  report: ScopeReport,
  { root, fixtures, display }: Context,
): Promise<void> {
  const tenantDir = resolve(root, tenant.fixturesDir);
  let realTenantDir: string | undefined;
  try {
    realTenantDir = await realpath(tenantDir);
  } catch {
    report.issues.push({
      severity: "warning",
      code: "no_fixtures_dir",
      message: `fixturesDir ${tenant.fixturesDir} does not exist`,
    });
  }

  const listed = new Set(entries.map((e) => resolve(fixtures, e.path)));
  const tally: ValueTally = new Map();
  for (const entry of entries) {
    const name = `${entry.source}/batch ${entry.batch} "${entry.path}"`;
    const file = resolve(fixtures, entry.path);
    // The loader's rule: a manifest path must resolve strictly below the tenant's fixturesDir.
    if (!isInside(tenantDir, file)) {
      report.issues.push({
        severity: "error",
        code: "path_outside",
        message: `${name} is outside ${tenant.fixturesDir}: the load would stop before any file`,
      });
      continue;
    }
    let real: string;
    try {
      real = await realpath(file);
    } catch {
      // Files another entry already lists are accounted for; a typo's real file is not.
      const siblings = (await readdir(dirname(file)).catch(() => [])).filter((f) => !listed.has(join(dirname(file), f)));
      const similar = nearMisses(basename(file), siblings);
      report.issues.push({
        severity: "warning",
        code: "not_on_disk",
        message:
          `${name} not on disk: not delivered yet, or a path typo?` +
          (similar.length === 0 ? "" : ` (similar: ${similar.join(", ")})`),
      });
      continue;
    }
    if (realTenantDir === undefined || !isInside(realTenantDir, real)) {
      report.issues.push({
        severity: "error",
        code: "path_outside",
        message: `${name} resolves to ${real}, outside ${tenant.fixturesDir}: the load would fail on it`,
      });
      continue;
    }
    const mapping = tenant.sources[entry.source];
    if (mapping !== undefined) checkContent(entry, name, await readFile(real), mapping, tally, report);
  }

  for (const source of SOURCE_NAMES) {
    const dir = join(tenantDir, source);
    const names = await readdir(dir, { withFileTypes: true }).catch(() => []);
    for (const dirent of names.sort((a, b) => compareText(a.name, b.name))) {
      if (dirent.name.startsWith(".") || dirent.isDirectory()) continue;
      if (!listed.has(join(dir, dirent.name))) {
        report.issues.push({
          severity: "warning",
          code: "not_in_manifest",
          message: `${display(join(dir, dirent.name))} on disk but not in the manifest: it will not load`,
        });
      }
    }
  }

  checkValues(tenant, tally, report);
}

/** Parses a file as the loader would, then tallies its mapped-column values for checkValues. */
function checkContent(
  entry: ManifestEntry,
  name: string,
  bytes: Uint8Array,
  mapping: NonNullable<TenantConfig["sources"][SourceName]>,
  tally: ValueTally,
  report: ScopeReport,
): void {
  const parsed = parseBatchFile(bytes, entry.source, mapping.columnAliases);
  if (!parsed.ok) {
    const reasons = parsed.reasons.map((r) => `${r.code}${r.line === undefined ? "" : ` line ${r.line}`} ${r.message}`);
    report.issues.push({
      severity: "error",
      code: "would_be_quarantined",
      message: `${name} would be quarantined: ${reasons.join("; ")}`,
    });
    return;
  }
  for (const column of MAPPED_COLUMNS[entry.source] ?? []) {
    const key = `${entry.source}.${column}` as const;
    const counts = tally.get(key) ?? new Map<string, number>();
    tally.set(key, counts);
    for (const record of parsed.records) {
      const raw = columnValue(record, parsed.columns, column);
      if (raw !== undefined) counts.set(raw, (counts.get(raw) ?? 0) + 1);
    }
  }
}

/** Value checks over every file of the tenant, mapped exactly as staging maps them. */
function checkValues(tenant: TenantConfig, tally: ValueTally, report: ScopeReport): void {
  for (const source of SOURCE_NAMES) {
    const valueMaps: Partial<Record<string, Record<string, string>>> = tenant.sources[source]?.valueMaps ?? {};
    const mapped: readonly string[] = MAPPED_COLUMNS[source] ?? [];
    for (const column of Object.keys(valueMaps).filter((c) => !mapped.includes(c))) {
      report.issues.push({
        severity: "warning",
        code: "unused_value_map",
        message: `${source}.${column} has a value map, but staging maps only ${mapped.map((c) => `${source}.${c}`).join(", ") || "nothing"} of ${source}`,
      });
    }
  }

  const mappedSets = new Map<string, Set<string>>();
  for (const [key, counts] of tally) {
    const [source, column] = key.split(".") as [SourceName, string];
    const map = tenant.sources[source]?.valueMaps?.[column as never] as Record<string, string> | undefined;
    const allowed = canonicalValues(source)[column];
    const byMapped = new Map<string, number>();
    for (const [raw, rows] of counts) byMapped.set(mapValue(map, raw), (byMapped.get(mapValue(map, raw)) ?? 0) + rows);
    mappedSets.set(key, new Set(byMapped.keys()));
    const rawOrder = [...counts].sort(([a, x], [b, y]) => y - x || compareText(a, b));
    const rowsOf = ([raw, rows]: [string, number]): string => {
      const target = mapValue(map, raw);
      return `"${raw}"${target === raw ? "" : ` (mapped to "${target}")`} ${plural(rows, "row")}`;
    };

    if (allowed !== undefined) {
      const uncounted = rawOrder.filter(([raw]) => !allowed.includes(mapValue(map, raw)));
      if (uncounted.length > 0) {
        report.issues.push({
          severity: "error",
          code: "uncounted_values",
          message: `${key} ${uncounted.map(rowsOf).join(", ")} would not be counted: not one of ${allowed.join(", ")}`,
        });
      }
      continue;
    }

    const values = [...byMapped].sort(([a, x], [b, y]) => y - x || compareText(a, b));
    report.issues.push({
      severity: "info",
      code: "values",
      message: `${key} ${values.map(([v, rows]) => `${v} ${rows}`).join(", ")}`,
    });
    // Without a map every value is taken as already canonical; with one, a spelling that is
    // neither a key nor a target is most likely a variant the map missed.
    if (map !== undefined && Object.keys(map).length > 0) {
      const targets = new Set(Object.values(map));
      const unmapped = rawOrder.filter(([raw]) => !Object.hasOwn(map, raw) && !targets.has(raw));
      if (unmapped.length > 0) {
        report.issues.push({
          severity: "warning",
          code: "unmapped_values",
          message: `${key} ${unmapped.map(rowsOf).join(", ")} not in the value map: passed through unchanged`,
        });
      }
    }
  }

  const revenue = mappedSets.get(CHANNEL_JOIN.revenue);
  const spend = mappedSets.get(CHANNEL_JOIN.spend);
  if (revenue === undefined || spend === undefined) return;
  const revenueOnly = [...revenue].filter((v) => !spend.has(v)).sort(compareText);
  const spendOnly = [...spend].filter((v) => !revenue.has(v)).sort(compareText);
  if (revenueOnly.length > 0) {
    report.issues.push({
      severity: "warning",
      code: "channel_mismatch",
      message: `${CHANNEL_JOIN.revenue} ${quoteList(revenueOnly)} not in ${CHANNEL_JOIN.spend}: revenue with no spend in channel performance`,
    });
  }
  if (spendOnly.length > 0) {
    report.issues.push({
      severity: "warning",
      code: "channel_mismatch",
      message: `${CHANNEL_JOIN.spend} ${quoteList(spendOnly)} not in ${CHANNEL_JOIN.revenue}: spend with no revenue in channel performance`,
    });
  }
}

const count = (report: ScopeReport, severity: Severity): number =>
  report.issues.filter((i) => i.severity === severity).length;

/**
 * The process exit code for a validation:
 * - 0: no errors; warnings and info are allowed.
 * - 2: at least one error, i.e. a mistake that would fail migrate or load or silently corrupt a number.
 * - 1 (set by the CLI, never returned here): the validator itself failed, e.g. an unreadable
 *   tenants directory, so nothing is known about the tenants.
 */
export const exitStatus = (reports: readonly ScopeReport[]): 0 | 2 =>
  reports.some((r) => count(r, "error") > 0) ? 2 : 0;

/**
 * A summary line per scope, then one indented `<severity> <code>: <message>` line per issue,
 * and a final totals line. Lines are stable, so `rg '^  error '` lists every error.
 */
export function formatValidation(reports: readonly ScopeReport[]): string[] {
  const errors = reports.reduce((n, r) => n + count(r, "error"), 0);
  const warnings = reports.reduce((n, r) => n + count(r, "warning"), 0);
  return [
    ...reports.flatMap((r) => [
      `${r.scope}: ${plural(count(r, "error"), "error")}, ${plural(count(r, "warning"), "warning")}`,
      ...r.issues.map((i) => `  ${i.severity} ${i.code}: ${i.message}`),
    ]),
    `tenant:validate: ${plural(errors, "error")}, ${plural(warnings, "warning")}`,
  ];
}

// CLI: `pnpm tenant:validate [id ...]`, optionally with TENANTS_DIR=<dir> and MANIFEST=<path>;
// exit codes as in exitStatus.
if (import.meta.main) {
  try {
    const reports = await validateTenants({
      ids: process.argv.slice(2),
      ...(process.env.TENANTS_DIR ? { tenantsDir: resolve(process.env.TENANTS_DIR) } : {}),
      ...(process.env.MANIFEST ? { manifest: process.env.MANIFEST } : {}),
    });
    for (const line of formatValidation(reports)) console.log(line);
    process.exitCode = exitStatus(reports);
  } catch (error) {
    console.error(describeError(error));
    process.exitCode = 1;
  }
}
