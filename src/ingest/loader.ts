import { createHash } from "node:crypto";
import { readFile, realpath } from "node:fs/promises";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import type pg from "pg";
import type { SourceName } from "../config/sources.ts";
import { loadTenants, type TenantConfig } from "../config/tenants.ts";
import { closePools, getAppPool } from "../db/pool.ts";
import { withTenant } from "../db/tenant-scope.ts";
import { loadManifest, type Manifest, type ManifestEntry, manifestSchema } from "./manifest.ts";
import { type ParseResult, parseBatchFile } from "./parse.ts";
import type { QuarantineReason } from "./reasons.ts";

const DEFAULT_ROOT_DIR = fileURLToPath(new URL("../../", import.meta.url));
// Rows per INSERT: keeps each statement's parameters bounded on large files.
export const INSERT_CHUNK_SIZE = 1_000;

/**
 * - loaded, skipped (same bytes already loaded for this batch), quarantined, missing: the
 *   file was dealt with; quarantined and missing are findings about the data.
 * - failed: an unexpected error while loading it; nothing was written for the file.
 * - blocked: not attempted, because an earlier file of the same tenant failed.
 */
export type FileStatus = "loaded" | "skipped" | "quarantined" | "missing" | "failed" | "blocked";

export interface FileResult {
  tenant: string;
  source: SourceName;
  batch: number;
  path: string;
  status: FileStatus;
  /** Data rows in the loaded file (for `skipped`, the rows loaded by the earlier run). */
  rowCount?: number;
  reasons?: QuarantineReason[];
  /** For `failed`: the error message, with its code (and Postgres detail) when it has one. */
  error?: string;
}

export interface LoadOptions {
  /** Tenants to load; manifest entries for any other tenant are ignored. */
  tenants: TenantConfig[];
  /** A parsed manifest or the path of one. Defaults to `<fixturesRoot>/manifest.json`. */
  manifest?: Manifest | string;
  /** Directory that tenant `fixturesDir` values are relative to: the repository root. */
  rootDir?: string;
  /** Directory that manifest paths are relative to. Defaults to `<rootDir>/fixtures`. */
  fixturesRoot?: string;
  pool?: pg.Pool;
  /**
   * Fault injection for crash tests: once this many files have been loaded, the next file
   * throws inside its transaction after its raw rows are inserted, before COMMIT.
   */
  failAfterFiles?: number;
  /** Called as each file finishes, in processing order, with the result also returned at the end. */
  onResult?: (result: FileResult) => void;
}

/** Thrown by the `failAfterFiles` fault injection. */
export class InjectedFailure extends Error {
  override name = "InjectedFailure";
}

interface PlannedFile {
  entry: ManifestEntry;
  tenant: TenantConfig;
  tenantDir: string;
  file: string;
}

// Code-point order, not locale order, so the processing order is the same on every machine.
const compareText = (a: string, b: string): number => (a < b ? -1 : a > b ? 1 : 0);
const compareEntries = (a: ManifestEntry, b: ManifestEntry): number =>
  compareText(a.tenant, b.tenant) || compareText(a.source, b.source) || a.batch - b.batch;

/** True when `path` is strictly below `dir`. Both must be absolute. */
const isInside = (dir: string, path: string): boolean => {
  const rel = relative(dir, path);
  return rel !== "" && !isAbsolute(rel) && rel.split(sep)[0] !== "..";
};

const quarantine = (reason: QuarantineReason): ParseResult => ({ ok: false, reasons: [reason] });

const entryName = ({ tenant, source, batch }: Pick<ManifestEntry, "tenant" | "source" | "batch">): string =>
  `${tenant}/${source}/batch ${batch}`;

const baseResult = ({ tenant, source, batch, path }: ManifestEntry) => ({ tenant, source, batch, path });

/** An error's message plus its `code` (a Postgres SQLSTATE or a Node errno) and Postgres `detail`, when present. */
export function describeError(error: unknown): string {
  if (!(error instanceof Error)) return String(error);
  const { code, detail } = error as { code?: unknown; detail?: unknown };
  const pgCode = typeof code === "string" ? ` [${code}]` : "";
  const pgDetail = typeof detail === "string" ? ` (${detail})` : "";
  return `${error.message}${pgCode}${pgDetail}`;
}

/**
 * Loads every manifest batch file of the given tenants into raw.record, in
 * (tenant, source, batch) order, as the application role.
 *
 * Idempotent per file: a batch whose bytes are already loaded is skipped, and each file's
 * ledger row and raw rows commit in one transaction, so a crash leaves every file either
 * fully loaded or absent and a rerun picks up where the last one stopped. Files that
 * cannot be loaded as-is are quarantined with their reasons in the ledger, including
 * new bytes for a loaded batch and a loaded batch's bytes under another batch number.
 *
 * An unexpected error on one file marks it `failed` and the tenant's remaining files
 * `blocked` (their order matters), while other tenants carry on. A manifest path outside
 * its tenant's directory is rejected before anything loads, and `InjectedFailure`
 * propagates, since both stand for the whole run failing.
 * Returns one result per manifest entry processed.
 *
 * Without `pool`, the shared application pool is used; the caller then owns its
 * lifecycle and must call `closePools()` when done, as the CLI below does.
 */
export async function loadBatches({
  tenants,
  manifest,
  rootDir = DEFAULT_ROOT_DIR,
  fixturesRoot,
  pool = getAppPool(),
  failAfterFiles,
  onResult,
}: LoadOptions): Promise<FileResult[]> {
  const root = resolve(rootDir);
  const fixtures = resolve(root, fixturesRoot ?? "fixtures");
  const parsedManifest =
    typeof manifest === "object"
      ? manifestSchema.parse(manifest)
      : await loadManifest(manifest ?? join(fixtures, "manifest.json"));

  // Every path is checked before the first load, so a manifest that points one tenant
  // at another tenant's files (or anywhere else) loads nothing at all.
  const byId = new Map(tenants.map((t) => [t.id, t]));
  const planned: PlannedFile[] = [];
  for (const entry of [...parsedManifest.batches].sort(compareEntries)) {
    const tenant = byId.get(entry.tenant);
    if (tenant === undefined) continue;
    const tenantDir = resolve(root, tenant.fixturesDir);
    const file = resolve(fixtures, entry.path);
    if (!isInside(tenantDir, file)) {
      throw new Error(`Manifest entry ${entryName(entry)} path "${entry.path}" is outside ${tenant.fixturesDir}`);
    }
    planned.push({ entry, tenant, tenantDir, file });
  }

  const results: FileResult[] = [];
  const failedTenants = new Set<string>();
  let loaded = 0;
  for (const plan of planned) {
    let result: FileResult;
    if (failedTenants.has(plan.tenant.id)) {
      result = { ...baseResult(plan.entry), status: "blocked" };
    } else {
      const failBeforeCommit = failAfterFiles !== undefined && loaded >= failAfterFiles;
      try {
        result = await loadFile(plan, pool, failBeforeCommit);
      } catch (error) {
        if (error instanceof InjectedFailure) throw error;
        // Later files of this tenant may depend on this one (a restatement, a late
        // arrival), so they wait for the next run instead of loading out of order.
        failedTenants.add(plan.tenant.id);
        result = { ...baseResult(plan.entry), status: "failed", error: describeError(error) };
      }
    }
    if (result.status === "loaded") loaded++;
    results.push(result);
    try {
      onResult?.(result);
    } catch {
      // Reporting is best effort: an observer that throws never changes what loads.
    }
  }
  return results;
}

/** Reads the file through its real path, refusing a symlink that leads out of the tenant's directory. */
async function readInside(tenantDir: string, file: string): Promise<Buffer | undefined> {
  let real: string;
  try {
    real = await realpath(file);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
  if (!isInside(await realpath(tenantDir), real)) {
    throw new Error(`${file} resolves to ${real}, outside ${tenantDir}`);
  }
  return readFile(real);
}

async function loadFile(plan: PlannedFile, pool: pg.Pool, failBeforeCommit: boolean): Promise<FileResult> {
  const bytes = await readInside(plan.tenantDir, plan.file);
  if (bytes === undefined) return { ...baseResult(plan.entry), status: "missing" };
  const sha256 = createHash("sha256").update(bytes).digest("hex");

  try {
    return await loadBytes(plan, pool, bytes, sha256, failBeforeCommit);
  } catch (error) {
    // Not expected from another loader, since every loader takes the lock in loadBytes;
    // only a writer that bypasses it can commit the same bytes in between. One retry
    // classifies the file against that committed row (skipped or duplicate_content)
    // through the normal path.
    const { code, constraint } = error as { code?: unknown; constraint?: unknown };
    if (code === "23505" && constraint === "batch_file_loaded_once") {
      try {
        return await loadBytes(plan, pool, bytes, sha256, failBeforeCommit);
      } catch (retryError) {
        // Reported as the retry's error, keeping the unique violation that caused the retry.
        if (retryError instanceof Error && retryError.cause === undefined) retryError.cause = error;
        throw retryError;
      }
    }
    throw error;
  }
}

async function loadBytes(
  { entry, tenant }: PlannedFile,
  pool: pg.Pool,
  bytes: Buffer,
  sha256: string,
  failBeforeCommit: boolean,
): Promise<FileResult> {
  const base = baseResult(entry);
  return withTenant(
    tenant.id,
    async (client): Promise<FileResult> => {
      // Serializes every attempt at this tenant's source, whatever the batch number, so
      // the ledger check and the insert below cannot interleave with another run's.
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [`${tenant.id}/${entry.source}`]);
      const { rows: previous } = await client.query<{ sha256: string; batch_no: number; row_count: number }>(
        `SELECT sha256, batch_no, row_count FROM ops.batch_file
         WHERE status = 'loaded' AND source = $1 AND (sha256 = $2 OR batch_no = $3)`,
        [entry.source, sha256, entry.batch],
      );
      const alreadyLoaded = previous.find((row) => row.sha256 === sha256 && row.batch_no === entry.batch);
      if (alreadyLoaded !== undefined) return { ...base, status: "skipped", rowCount: alreadyLoaded.row_count };

      const mapping = tenant.sources[entry.source];
      const sameBatch = previous.find((row) => row.batch_no === entry.batch);
      const sameBytes = previous.find((row) => row.sha256 === sha256);
      const parsed: ParseResult =
        sameBatch !== undefined
          ? // Restating a loaded batch needs its own handling; until then it is held back.
            quarantine({
              code: "batch_conflict",
              message: `batch ${entry.batch} was already loaded with different content (sha256 ${sameBatch.sha256})`,
            })
          : sameBytes !== undefined
            ? // Loading it would count every row twice under two batch numbers.
              quarantine({
                code: "duplicate_content",
                message: `file has the same content as batch ${sameBytes.batch_no}, which is already loaded`,
              })
            : mapping === undefined
              ? quarantine({ code: "source_not_configured", message: `${tenant.id} has no ${entry.source} source` })
              : parseBatchFile(bytes, entry.source, mapping.columnAliases);

      if (!parsed.ok) {
        const { reasons } = parsed;
        await client.query(
          `INSERT INTO ops.batch_file (tenant_id, source, batch_no, path, sha256, status, detail)
           VALUES ($1, $2, $3, $4, $5, 'quarantined', $6)`,
          [tenant.id, entry.source, entry.batch, entry.path, sha256, { reasons }],
        );
        return { ...base, status: "quarantined", reasons };
      }

      const { rows } = await client.query<{ id: string }>(
        `INSERT INTO ops.batch_file (tenant_id, source, batch_no, path, sha256, status, row_count, detail)
         VALUES ($1, $2, $3, $4, $5, 'loaded', $6, $7)
         RETURNING id`,
        [tenant.id, entry.source, entry.batch, entry.path, sha256, parsed.records.length, { columns: parsed.columns }],
      );
      const batchFileId = rows[0]?.id;
      for (let i = 0; i < parsed.records.length; i += INSERT_CHUNK_SIZE) {
        const chunk = parsed.records.slice(i, i + INSERT_CHUNK_SIZE);
        await client.query(
          `INSERT INTO raw.record (tenant_id, batch_file_id, line_no, payload)
           SELECT $1, $2, line_no, payload FROM unnest($3::int[], $4::jsonb[]) AS t (line_no, payload)`,
          [tenant.id, batchFileId, chunk.map((r) => r.lineNo), chunk.map((r) => JSON.stringify(r.payload))],
        );
      }
      if (failBeforeCommit) {
        throw new InjectedFailure(`injected failure before committing ${entryName(entry)}`);
      }
      return { ...base, status: "loaded", rowCount: parsed.records.length };
    },
    pool,
  );
}

/** Parses LOADER_FAIL_AFTER_FILES: unset or empty means no fault injection. */
function failAfterFilesFromEnv(value: string | undefined): number | undefined {
  if (value === undefined || value === "") return undefined;
  if (!/^\d+$/.test(value)) {
    throw new Error(`LOADER_FAIL_AFTER_FILES must be a non-negative integer, got "${value}"`);
  }
  return Number(value);
}

/** One line per file for the CLI: status, which batch, and rows or what went wrong. */
function formatResult(result: FileResult): string {
  const outcome =
    result.status === "failed"
      ? result.error
      : result.status === "quarantined"
        ? result.reasons?.map((r) => `${r.code}: ${r.message}`).join("; ")
        : result.status === "blocked"
          ? "not attempted after an earlier failure of this tenant"
          : result.status === "missing"
            ? result.path
            : `${result.rowCount} rows`;
  return `${result.status}: ${entryName(result)}${outcome === undefined ? "" : ` (${outcome})`}`;
}

if (import.meta.main) {
  try {
    const failAfterFiles = failAfterFilesFromEnv(process.env.LOADER_FAIL_AFTER_FILES);
    const results = await loadBatches({
      tenants: await loadTenants(),
      ...(failAfterFiles === undefined ? {} : { failAfterFiles }),
      onResult: (result) => console.log(formatResult(result)),
    });
    const counts: Record<FileStatus, number> = { loaded: 0, skipped: 0, quarantined: 0, missing: 0, failed: 0, blocked: 0 };
    for (const result of results) counts[result.status]++;
    console.log(
      `load: ${counts.loaded} loaded, ${counts.skipped} skipped, ${counts.quarantined} quarantined, ` +
        `${counts.missing} missing, ${counts.failed} failed, ${counts.blocked} blocked`,
    );
    // Quarantined and missing files are findings about the data, not failures of the run.
    if (counts.failed > 0) process.exitCode = 1;
  } catch (error) {
    console.error(describeError(error));
    process.exitCode = 1;
  } finally {
    await closePools();
  }
}
