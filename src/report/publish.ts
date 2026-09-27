import { fileURLToPath } from "node:url";
import type pg from "pg";
import type { SourceName } from "../config/sources.ts";
import { loadTenants, type TenantConfig } from "../config/tenants.ts";
import { closePools, getAppPool } from "../db/pool.ts";
import { withTenant } from "../db/tenant-scope.ts";
import { covers, type IncompleteWindow, incompleteWindows, type LedgerAttempt } from "../ingest/coverage.ts";
import { describeError, InjectedFailure } from "../ingest/loader.ts";
import { loadManifest, type Manifest, type ManifestEntry, manifestSchema } from "../ingest/manifest.ts";

const DEFAULT_MANIFEST = fileURLToPath(new URL("../../fixtures/manifest.json", import.meta.url));

/** Where a mart's rows come from: a staging view, its day column and its dimension columns. */
interface LineageSpec {
  source: SourceName;
  view: string;
  day: string;
  /** Mart dimension name -> column of the staging view. */
  dims: Record<string, string>;
}

interface MartSpec {
  /** Columns that, with the day, identify one row of the mart. */
  dims: readonly string[];
  /** Columns published as the row's numbers. */
  metrics: readonly string[];
  /** Staging views whose rows feed the mart, used to name the files behind a restatement. */
  lineage: readonly LineageSpec[];
}

/**
 * Every publishable mart, declared once. Names here are trusted constants spliced into
 * SQL; nothing tenant- or user-supplied is. Adding a mart is one entry, no new table.
 */
export const MARTS = {
  daily_revenue: {
    dims: [],
    metrics: ["orders", "gross", "refunds", "net", "orphan_refund_count", "orphan_refund_amount"],
    lineage: [
      { source: "orders", view: "staging.orders", day: "order_date", dims: {} },
      { source: "refunds", view: "staging.refunds", day: "refund_date", dims: {} },
    ],
  },
  daily_ad_spend: {
    dims: ["platform"],
    metrics: ["spend"],
    lineage: [{ source: "ad_spend", view: "staging.ad_spend", day: "spend_date", dims: { platform: "platform" } }],
  },
  daily_email_engagement: {
    dims: [],
    metrics: ["delivered", "opens", "clicks", "unsubscribes"],
    lineage: [{ source: "email_events", view: "staging.email_events", day: "event_date", dims: {} }],
  },
  daily_channel_performance: {
    dims: ["channel"],
    metrics: ["revenue", "spend", "roas"],
    lineage: [
      { source: "orders", view: "staging.orders", day: "order_date", dims: { channel: "channel" } },
      { source: "ad_spend", view: "staging.ad_spend", day: "spend_date", dims: { channel: "platform" } },
    ],
  },
} as const satisfies Record<string, MartSpec>;

export type MartName = keyof typeof MARTS;

export const MART_NAMES = Object.keys(MARTS) as MartName[];

/** The marts whose lineage includes `source`, in MARTS order. */
export const martsBuiltFrom = (source: SourceName): MartName[] =>
  MART_NAMES.filter((mart) => MARTS[mart].lineage.some((l: LineageSpec) => l.source === source));

export interface PublishOptions {
  /** Tenants to publish, each in its own transaction. */
  tenants: TenantConfig[];
  /** Marts to publish, at least one. Defaults to every mart in MARTS. */
  marts?: MartName[];
  pool?: pg.Pool;
  /**
   * The batches each tenant is expected to have delivered, a parsed manifest or the path of
   * one; defaults to fixtures/manifest.json. A key inside the window of a listed batch that
   * never loaded is withheld (see publishReports).
   */
  manifest?: Manifest | string;
  /**
   * Fault injection for crash tests: once this many tenants have been published, the next
   * tenant throws `InjectedFailure` inside its transaction after its rows are inserted, before COMMIT.
   */
  failAfterTenants?: number;
}

export type PublishResult =
  | {
      tenant: string;
      status: "published";
      /** The ops.report_run id, or null when nothing changed and no run was recorded. */
      runId: string | null;
      /** Versions written: new keys, restated keys and tombstones. */
      published: number;
      /** Versions that replaced a number already published (an ops.restatement row each). */
      restated: number;
      unchanged: number;
      /** Keys held back because a batch covering their day, for a source they depend on, never loaded. */
      withheld: number;
    }
  | { tenant: string; status: "failed"; error: string };

/** A mart key and its metrics, all as text: day YYYY-MM-DD, dims and metrics as canonical jsonb text. */
interface KeyedRow {
  mart: MartName;
  day: string;
  dims: string;
}
interface CurrentRow extends KeyedRow {
  metrics: string;
}
interface PublishedRow extends KeyedRow {
  version: number;
  metrics: string | null;
  run_id: string;
}
interface LineageRow extends KeyedRow {
  batch_file_id: string;
}
interface Snapshot {
  current: CurrentRow[];
  lineage: LineageRow[];
  files: { id: string; source: SourceName }[];
  ledger: LedgerAttempt[];
}

/** One element of ops.report_run.withheld: an incomplete window of a mart and the keys it held back. */
interface WithheldWindow {
  mart: MartName;
  source: SourceName;
  batch: number;
  path: string;
  covers_from: string;
  covers_to: string;
  status: IncompleteWindow["status"];
  keys: { day: string; dims: unknown }[];
}

const keyOf = (row: KeyedRow): string => `${row.mart}|${row.day}|${row.dims}`;
const byNumber = (a: string, b: string): number => (BigInt(a) < BigInt(b) ? -1 : BigInt(a) > BigInt(b) ? 1 : 0);

/** jsonb_build_object over `name -> expression` pairs; '{}' when there are none. */
const jsonObject = (pairs: [string, string][]): string =>
  `jsonb_build_object(${pairs.map(([name, expr]) => `'${name}', ${expr}`).join(", ")})`;

/**
 * One statement reading the marts, their lineage, the loaded files and the ledger, so all
 * four come from the same snapshot even under READ COMMITTED: a file committed mid-run is
 * either in all of them or in none, and a key is never judged complete by a ledger that
 * is newer or older than its numbers.
 */
function snapshotSql(marts: MartName[]): string {
  const current = marts.map((name) => {
    const spec: MartSpec = MARTS[name];
    return `SELECT '${name}' AS mart, to_char(day, 'YYYY-MM-DD') AS day,
              ${jsonObject(spec.dims.map((d) => [d, d]))}::text AS dims,
              ${jsonObject(spec.metrics.map((m) => [m, `${m}::text`]))}::text AS metrics
            FROM marts.${name}`;
  });
  const lineage = marts.flatMap((name) =>
    MARTS[name].lineage.map(
      (l: LineageSpec) =>
        `SELECT DISTINCT '${name}' AS mart, to_char(${l.day}, 'YYYY-MM-DD') AS day,
                ${jsonObject(Object.entries(l.dims))}::text AS dims, batch_file_id::text AS batch_file_id
         FROM ${l.view} WHERE ${l.day} IS NOT NULL`,
    ),
  );
  return `SELECT
    (SELECT coalesce(json_agg(c), '[]') FROM (${current.join(" UNION ALL ")}) c) AS current,
    (SELECT coalesce(json_agg(l), '[]') FROM (${lineage.join(" UNION ")}) l) AS lineage,
    (SELECT coalesce(json_agg(json_build_object('id', id::text, 'source', source)), '[]')
     FROM ops.batch_file WHERE status = 'loaded') AS files,
    (SELECT coalesce(json_agg(json_build_object('source', source, 'batch_no', batch_no, 'status', status)), '[]')
     FROM ops.batch_file) AS ledger`;
}

/**
 * Publishes each tenant's marts: every (mart, day, dimensions) key whose metrics differ
 * from its latest published version gets a new version, so ops.published_metric keeps
 * what the client was told and marts.reported_metric shows the latest of it.
 *
 * - A key never published: version 1.
 * - Changed metrics: the next version, plus an ops.restatement row with the numbers
 *   before and after and the batch files that caused the change.
 * - Unchanged: nothing is written.
 * - Published before but gone from the mart (e.g. all its rows restated onto another
 *   day): a tombstone version with NULL metrics and a restatement whose `after` is NULL.
 *
 * `caused_by` is the loaded files that feed the key's rows now and that the previous
 * version's run had not seen. When the change came only from rows leaving the key (none
 * of its current rows is new), it falls back to every file of the mart's sources loaded
 * since then; that is approximate, since not all of them need touch this key.
 * Metrics are compared as exact numeric strings, so a value stored with another scale
 * (10.0 vs 10.00) counts as a change.
 *
 * Withheld: a key whose day falls inside the window (covers_from..covers_to) of a manifest
 * batch with no loaded ledger row (not received, or only quarantined), for a source in the
 * mart's lineage, is not compared at all: no new version, no restatement, no tombstone.
 * Its numbers are missing a whole batch, and publishing them would tell the client, say,
 * that no money was spent. If a version was published before the window became
 * incomplete, it stays the latest, i.e. the last thing the client was told. Once the batch
 * loads, the next run treats the key as usual: version 1 if it was never published, a
 * restatement if it was and its numbers moved. The run records each incomplete window and
 * the keys it held back in ops.report_run.withheld. A batch that loaded and was later
 * redelivered with other bytes counts as loaded here; `pnpm check` reports it.
 *
 * Each tenant publishes in one transaction under a per-tenant advisory lock, so a crash
 * leaves that tenant's publication either complete or absent. An unexpected error fails
 * that tenant and the others carry on; `InjectedFailure` propagates.
 * Without `pool`, the shared application pool is used; the caller must then call
 * `closePools()` when done, as the CLI below does.
 */
export async function publishReports({
  tenants,
  marts = MART_NAMES,
  pool = getAppPool(),
  manifest,
  failAfterTenants,
}: PublishOptions): Promise<PublishResult[]> {
  // An empty list would build an empty UNION ALL, and every tenant would fail on it.
  if (marts.length === 0) throw new Error("marts must list at least one mart");
  const unknown = marts.filter((m) => !Object.hasOwn(MARTS, m));
  if (unknown.length > 0) throw new Error(`Unknown marts: ${unknown.join(", ")}`);
  const parsedManifest =
    typeof manifest === "object" ? manifestSchema.parse(manifest) : await loadManifest(manifest ?? DEFAULT_MANIFEST);

  const results: PublishResult[] = [];
  let done = 0;
  for (const tenant of tenants) {
    const failBeforeCommit = failAfterTenants !== undefined && done >= failAfterTenants;
    try {
      const expected = parsedManifest.batches.filter((e) => e.tenant === tenant.id);
      results.push(await publishTenant(tenant.id, marts, expected, pool, failBeforeCommit));
      done++;
    } catch (error) {
      if (error instanceof InjectedFailure) throw error;
      results.push({ tenant: tenant.id, status: "failed", error: describeError(error) });
    }
  }
  return results;
}

interface NewVersion extends KeyedRow {
  version: number;
  metrics: string | null;
}
interface NewRestatement extends NewVersion {
  before: string | null;
  causedBy: string[];
}

async function publishTenant(
  tenantId: string,
  marts: MartName[],
  expected: ManifestEntry[],
  pool: pg.Pool,
  failBeforeCommit: boolean,
): Promise<PublishResult> {
  return withTenant(
    tenantId,
    async (client) => {
      // Serializes report runs of this tenant: two runs must not both publish version n+1.
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [`report/${tenantId}`]);

      const { rows: published } = await client.query<PublishedRow>(
        `SELECT DISTINCT ON (mart, day, dims) mart, to_char(day, 'YYYY-MM-DD') AS day, dims::text AS dims,
                version, metrics::text AS metrics, run_id::text AS run_id
         FROM ops.published_metric
         WHERE mart = ANY($1)
         ORDER BY mart, day, dims, version DESC`,
        [marts],
      );
      const { rows: runs } = await client.query<{ id: string; files: string[] }>(
        "SELECT id::text AS id, loaded_batch_file_ids::text[] AS files FROM ops.report_run",
      );
      const snapshot = (await client.query<Snapshot>(snapshotSql(marts))).rows[0];
      if (snapshot === undefined) throw new Error("snapshot query returned no row");

      const seenByRun = new Map(runs.map((r) => [r.id, new Set(r.files)]));
      const latest = new Map(published.map((p) => [keyOf(p), p]));
      const current = new Map(snapshot.current.map((c) => [keyOf(c), c]));
      const feeding = new Map<string, string[]>();
      for (const l of snapshot.lineage) {
        const key = keyOf(l);
        feeding.set(key, [...(feeding.get(key) ?? []), l.batch_file_id]);
      }

      /** Files behind a change to `key` since `previous` was published; see the JSDoc above. */
      const causedBy = (key: string, previous: PublishedRow): string[] => {
        const seen = seenByRun.get(previous.run_id) ?? new Set<string>();
        const direct = (feeding.get(key) ?? []).filter((id) => !seen.has(id));
        if (direct.length > 0) return direct.sort(byNumber);
        const sources = new Set<string>(MARTS[previous.mart].lineage.map((l: LineageSpec) => l.source));
        return snapshot.files
          .filter((f) => sources.has(f.source) && !seen.has(f.id))
          .map((f) => f.id)
          .sort(byNumber);
      };

      // Per mart, the incomplete windows of the sources it is built from, each with the keys
      // it holds back. A window with no key is kept too: the mart is incomplete there even
      // when it has no row for those days.
      const incomplete = incompleteWindows(expected, snapshot.ledger);
      const windowsOf = new Map<MartName, { window: IncompleteWindow; record: WithheldWindow }[]>(
        marts.map((mart) => {
          const sources = new Set<string>(MARTS[mart].lineage.map((l: LineageSpec) => l.source));
          const windows = incomplete
            .filter((w) => sources.has(w.source))
            .map((w) => ({
              window: w,
              record: {
                mart,
                source: w.source,
                batch: w.batch,
                path: w.path,
                covers_from: w.coversFrom,
                covers_to: w.coversTo,
                status: w.status,
                keys: [],
              },
            }));
          return [mart, windows];
        }),
      );
      const withheldKeys = new Set<string>();
      /** True, and recorded, when `row`'s day is inside an incomplete window of its mart. */
      const withhold = (key: string, row: KeyedRow): boolean => {
        const hits = (windowsOf.get(row.mart) ?? []).filter(({ window }) => covers(window, row.day));
        for (const { record } of hits) record.keys.push({ day: row.day, dims: JSON.parse(row.dims) as unknown });
        if (hits.length > 0) withheldKeys.add(key);
        return hits.length > 0;
      };

      const versions: NewVersion[] = [];
      const restatements: NewRestatement[] = [];
      let unchanged = 0;
      for (const [key, row] of current) {
        if (withhold(key, row)) continue;
        const previous = latest.get(key);
        if (previous === undefined) {
          versions.push({ ...row, version: 1 });
        } else if (previous.metrics === row.metrics) {
          unchanged++;
        } else {
          const next = { ...row, version: previous.version + 1 };
          versions.push(next);
          restatements.push({ ...next, before: previous.metrics, causedBy: causedBy(key, previous) });
        }
      }
      for (const [key, previous] of latest) {
        if (current.has(key) || previous.metrics === null || withhold(key, previous)) continue;
        const tombstone = { mart: previous.mart, day: previous.day, dims: previous.dims, version: previous.version + 1, metrics: null };
        versions.push(tombstone);
        restatements.push({ ...tombstone, before: previous.metrics, causedBy: causedBy(key, previous) });
      }

      const withheld = withheldKeys.size;
      // A run row records a publication, so a run that publishes nothing writes nothing even
      // when it withheld keys: the withholding follows from the manifest and the ledger and
      // is recomputed, and reported, on every run until the batch loads.
      if (versions.length === 0) {
        return { tenant: tenantId, status: "published", runId: null, published: 0, restated: 0, unchanged, withheld };
      }

      const { rows: runRows } = await client.query<{ id: string }>(
        `INSERT INTO ops.report_run (tenant_id, loaded_batch_file_ids, withheld)
         VALUES ($1, $2::bigint[], $3::jsonb) RETURNING id::text AS id`,
        [tenantId, snapshot.files.map((f) => f.id), JSON.stringify([...windowsOf.values()].flat().map(({ record }) => record))],
      );
      const runId = runRows[0]?.id;
      if (runId === undefined) throw new Error("report run insert returned no id");

      await client.query(
        `INSERT INTO ops.published_metric (tenant_id, mart, day, dims, version, metrics, run_id)
         SELECT $1, mart, day, dims::jsonb, version, metrics::jsonb, $2
         FROM unnest($3::text[], $4::date[], $5::text[], $6::int[], $7::text[]) AS t (mart, day, dims, version, metrics)`,
        [
          tenantId,
          runId,
          versions.map((v) => v.mart),
          versions.map((v) => v.day),
          versions.map((v) => v.dims),
          versions.map((v) => v.version),
          versions.map((v) => v.metrics),
        ],
      );
      if (restatements.length > 0) {
        await client.query(
          `INSERT INTO ops.restatement
             (tenant_id, run_id, mart, day, dims, from_version, to_version, before, after, caused_by)
           SELECT $1, $2, mart, day, dims::jsonb, version - 1, version, before::jsonb, after::jsonb, caused_by::bigint[]
           FROM unnest($3::text[], $4::date[], $5::text[], $6::int[], $7::text[], $8::text[], $9::text[])
             AS t (mart, day, dims, version, before, after, caused_by)`,
          [
            tenantId,
            runId,
            restatements.map((r) => r.mart),
            restatements.map((r) => r.day),
            restatements.map((r) => r.dims),
            restatements.map((r) => r.version),
            restatements.map((r) => r.before),
            restatements.map((r) => r.metrics),
            // One array literal per row: unnest cannot take a ragged two-dimensional array.
            restatements.map((r) => `{${r.causedBy.join(",")}}`),
          ],
        );
      }
      if (failBeforeCommit) {
        throw new InjectedFailure(`injected failure before committing the report run of ${tenantId}`);
      }
      return {
        tenant: tenantId,
        status: "published",
        runId,
        published: versions.length,
        restated: restatements.length,
        unchanged,
        withheld,
      };
    },
    pool,
  );
}

/** One line per tenant for the CLI. */
export function formatResult(result: PublishResult): string {
  if (result.status === "failed") return `failed: ${result.tenant} (${result.error})`;
  const run = result.runId === null ? "nothing to publish" : `run ${result.runId}`;
  return (
    `published: ${result.tenant} (${run}; ${result.published} versions, ` +
    `${result.restated} restated, ${result.unchanged} unchanged, ${result.withheld} withheld)`
  );
}

// CLI: `pnpm report`, optionally with MANIFEST=<path>. Restatements and withheld keys are
// information, not errors: exit 0 unless a tenant failed.
if (import.meta.main) {
  try {
    const results = await publishReports({ tenants: await loadTenants(), manifest: process.env.MANIFEST || DEFAULT_MANIFEST });
    for (const result of results) console.log(formatResult(result));
    if (results.some((r) => r.status === "failed")) process.exitCode = 1;
  } catch (error) {
    console.error(describeError(error));
    process.exitCode = 1;
  } finally {
    await closePools();
  }
}
