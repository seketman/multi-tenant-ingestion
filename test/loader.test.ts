import { randomBytes } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { SourceName } from "../src/config/sources.ts";
import { loadTenants, type TenantConfig } from "../src/config/tenants.ts";
import { closePools, getOwnerPool, withTenant } from "../src/db/index.ts";
import { upsertTenant } from "../src/db/seed.ts";
import {
  type FileResult,
  INSERT_CHUNK_SIZE,
  InjectedFailure,
  type LoadOptions,
  loadBatches,
} from "../src/ingest/loader.ts";
import { loadManifest, type Manifest } from "../src/ingest/manifest.ts";
import { parseBatchFile } from "../src/ingest/parse.ts";

// Unique tenants per run keep the test re-runnable and away from the seeded 'northwind' and 'lumen'.
const suffix = randomBytes(4).toString("hex");
const createdTenants: string[] = [];

const register = async (tenant: TenantConfig) => {
  await upsertTenant(tenant);
  createdTenants.push(tenant.id);
};

interface LedgerRow {
  source: SourceName;
  batch_no: number;
  status: "loaded" | "quarantined";
  row_count: number;
  detail: { columns?: Record<string, string>; reasons?: { code: string; message: string }[] } | null;
  records: number;
}

const ledger = async (tenantId: string): Promise<LedgerRow[]> =>
  withTenant(tenantId, async (client) => {
    const { rows } = await client.query<LedgerRow>(
      `SELECT f.source, f.batch_no, f.status, f.row_count, f.detail,
              (SELECT count(*)::int FROM raw.record r WHERE r.batch_file_id = f.id) AS records
       FROM ops.batch_file f
       ORDER BY f.source, f.batch_no, f.id`,
    );
    return rows;
  });

const rawCount = async (tenantId: string): Promise<number> =>
  withTenant(
    tenantId,
    async (client) => (await client.query<{ n: number }>("SELECT count(*)::int AS n FROM raw.record")).rows[0]?.n ?? 0,
  );

/** The supplied tenants and manifest under ids ending in `_<tag>`, registered in the database. */
const fixturesAs = async (tag: string): Promise<{ tenants: TenantConfig[]; manifest: Manifest }> => {
  const tenants = (await loadTenants()).map((t) => ({ ...t, id: `${t.id}_${tag}` }));
  const real = await loadManifest("fixtures/manifest.json");
  const manifest = { batches: real.batches.map((b) => ({ ...b, tenant: `${b.tenant}_${tag}` })) };
  for (const tenant of tenants) await register(tenant);
  return { tenants, manifest };
};

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
        await client.query("DELETE FROM ops.tenant WHERE tenant_id = $1", [id]);
      },
      owner,
    );
  }
  await closePools();
});

describe("raw loader on the supplied fixtures", () => {
  let tenants: TenantConfig[];
  let manifest: Manifest;
  // tenant -> "source/batch" -> records the parser finds in the file on disk
  const expectedRows = new Map<string, Map<string, number>>();

  beforeAll(async () => {
    ({ tenants, manifest } = await fixturesAs(suffix));

    for (const entry of manifest.batches) {
      let bytes: Buffer;
      try {
        bytes = await readFile(join("fixtures", entry.path));
      } catch {
        continue;
      }
      const aliases = tenants.find((t) => t.id === entry.tenant)?.sources[entry.source]?.columnAliases;
      const parsed = parseBatchFile(bytes, entry.source, aliases);
      if (!parsed.ok) throw new Error(`fixture ${entry.path} does not parse: ${JSON.stringify(parsed.reasons)}`);
      const perTenant = expectedRows.get(entry.tenant) ?? new Map<string, number>();
      perTenant.set(`${entry.source}/${entry.batch}`, parsed.records.length);
      expectedRows.set(entry.tenant, perTenant);
    }
  });

  const loadedRows = async () => {
    const rows = await Promise.all(tenants.map(async (t) => (await ledger(t.id)).map((r) => ({ ...r, tenant: t.id }))));
    return rows.flat();
  };

  it(
    "rolls back the file in flight on a crash and completes on replay without duplicates",
    async () => {
      const run = (failAfterFiles?: number) =>
        loadBatches({ tenants, manifest, ...(failAfterFiles === undefined ? {} : { failAfterFiles }) });

      await expect(run(7)).rejects.toBeInstanceOf(InjectedFailure);
      const afterCrash = await loadedRows();
      expect(afterCrash.map((r) => r.status)).toEqual(Array(7).fill("loaded"));
      // Processing order is lumen first; its 8th present file is email_events batch 4.
      expect(afterCrash.some((r) => r.source === "email_events" && r.batch_no === 4)).toBe(false);
      for (const row of afterCrash) {
        expect(row.records).toBe(expectedRows.get(row.tenant)?.get(`${row.source}/${row.batch_no}`));
      }
      const rawTotal = async () => (await Promise.all(tenants.map((t) => rawCount(t.id)))).reduce((sum, n) => sum + n, 0);
      // No orphan raw rows from the rolled-back file either.
      expect(await rawTotal()).toBe(afterCrash.reduce((sum, r) => sum + r.records, 0));

      const replay = await run();
      expect(statusCounts(replay)).toEqual({ skipped: 7, loaded: 32, missing: 1 });
      expect(replay.filter((r) => r.status === "missing").map((r) => [r.tenant, r.source, r.batch])).toEqual([
        [`lumen_${suffix}`, "ad_spend", 3],
      ]);

      const afterReplay = await loadedRows();
      expect(afterReplay.every((r) => r.status === "loaded")).toBe(true);
      const loaded = afterReplay.map((r) => ({ tenant: r.tenant, key: `${r.source}/${r.batch_no}`, records: r.records }));
      const expected = [...expectedRows].flatMap(([tenant, files]) =>
        [...files].map(([key, records]) => ({ tenant, key, records })),
      );
      expect(loaded).toHaveLength(39);
      expect(loaded).toEqual(expect.arrayContaining(expected));
      expect(afterReplay.every((r) => r.row_count === r.records)).toBe(true);

      const ad = afterReplay.find((r) => r.tenant === `lumen_${suffix}` && r.source === "ad_spend" && r.batch_no === 4);
      expect(ad?.detail?.columns).toEqual({ date: "date", campaign_id: "campaign_id", platform: "platform", cost_usd: "spend" });

      const third = await run();
      expect(statusCounts(third)).toEqual({ skipped: 39, missing: 1 });
      expect(await loadedRows()).toEqual(afterReplay);
      expect(await rawTotal()).toBe(afterReplay.reduce((sum, r) => sum + r.records, 0));
    },
    60_000,
  );
});

describe("daily gross from the raw layer", () => {
  // Tenants of their own, so these run (and pass) without the crash test above.
  const tag = `${suffix}_gross`;

  beforeAll(async () => {
    const { tenants, manifest } = await fixturesAs(tag);
    expect(statusCounts(await loadBatches({ tenants, manifest }))).toEqual({ loaded: 39, missing: 1 });
  }, 60_000);

  describe.each(["northwind", "lumen"])("%s daily gross", (name) => {
    // Resolves each column through the ledger's raw -> canonical map rather than
    // assuming the export uses canonical names.
    const dailyGross = (dedupe: boolean) =>
      withTenant(`${name}_${tag}`, async (client) => {
        const { rows } = await client.query<{ day: string; gross: string }>(
          `WITH orders AS (
             SELECT ${dedupe ? "DISTINCT ON (r.payload ->> m.order_id)" : ""}
                    r.payload ->> m.gross AS gross, r.payload ->> m.created_at AS created_at
             FROM raw.record r
             JOIN ops.batch_file f ON f.tenant_id = r.tenant_id AND f.id = r.batch_file_id
             CROSS JOIN LATERAL (
               SELECT max(raw) FILTER (WHERE canonical = 'order_id')   AS order_id,
                      max(raw) FILTER (WHERE canonical = 'gross')      AS gross,
                      max(raw) FILTER (WHERE canonical = 'created_at') AS created_at
               FROM jsonb_each_text(f.detail -> 'columns') AS c (raw, canonical)
             ) m
             WHERE f.source = 'orders' AND f.status = 'loaded'
             ORDER BY r.payload ->> m.order_id, f.batch_no, r.line_no
           )
           SELECT to_char((created_at::timestamptz AT TIME ZONE 'UTC')::date, 'YYYY-MM-DD') AS day,
                  sum(gross::numeric)::text AS gross
           FROM orders
           GROUP BY 1
           ORDER BY 1`,
        );
        return Object.fromEntries(rows.map((r) => [r.day, r.gross]));
      });

    const reported = async () => {
      // date,gross_reported,net_reported,currency; the currency label is not trusted, only values.
      const [, ...lines] = (await readFile(`fixtures/${name}/finance_summary.csv`, "utf8")).trim().split("\n");
      return Object.fromEntries(lines.map((line) => line.split(",").slice(0, 2)));
    };

    it("matches finance_summary to the cent once orders are de-duplicated by order_id", async () => {
      expect(await dailyGross(true)).toEqual(await reported());
    });

    if (name === "northwind") {
      it("does not match without de-duplication, because batches overlap", async () => {
        const naive = await dailyGross(false);
        const expected = await reported();
        expect(Object.entries(naive).some(([day, gross]) => expected[day] !== gross)).toBe(true);
      });
    }
  });
});

describe("raw loader on problem files", () => {
  let root: string;
  let northwindSources: TenantConfig["sources"];
  const tenantFor = (name: string): TenantConfig => ({
    id: `${name}_${suffix}`,
    displayName: name,
    currency: "USD",
    fixturesDir: `fixtures/${name}_${suffix}`,
    sources: northwindSources,
  });
  const put = async (tenant: TenantConfig, path: string, text: string) => {
    const file = join(root, "fixtures", tenant.id, path);
    await mkdir(dirname(file), { recursive: true });
    await writeFile(file, text);
  };
  const entry = (tenant: TenantConfig, source: SourceName, batch: number, path: string) => ({
    tenant: tenant.id,
    source,
    batch,
    path: `${tenant.id}/${path}`,
    covers_from: "2026-01-06",
    covers_to: "2026-01-11",
  });
  const load = (tenant: TenantConfig, batches: Manifest["batches"], options: Pick<LoadOptions, "failAfterFiles"> = {}) =>
    loadBatches({ tenants: [tenant], manifest: { batches }, rootDir: root, ...options });

  const refunds = (rows: number) =>
    ["refund_id,refunded_at,order_id,amount,currency"]
      .concat(Array.from({ length: rows }, (_, i) => `rf-${i},2026-01-06T00:00:00Z,o-${i},1.00,USD`))
      .join("\n");

  beforeAll(async () => {
    root = await mkdtemp(join(tmpdir(), "loader-"));
    northwindSources = (await loadTenants()).find((t) => t.id === "northwind")?.sources ?? {};
  });

  afterAll(async () => {
    await rm(root, { recursive: true, force: true });
  });

  it("quarantines a file with an undeclared header and still loads the others", async () => {
    const tenant = tenantFor("drift");
    await register(tenant);
    await put(tenant, "refunds/batch_01.csv", refunds(3));
    await put(tenant, "ad_spend/batch_01.csv", "date,campaign_id,platform,cost_usd\n2026-01-06,c-1,facebook,1.00\n");
    await put(tenant, "ad_spend/batch_02.csv", "date,campaign_id,platform,spend_eur\n2026-01-07,c-1,facebook,1.00\n");

    const results = await load(tenant, [
      entry(tenant, "refunds", 1, "refunds/batch_01.csv"),
      entry(tenant, "ad_spend", 1, "ad_spend/batch_01.csv"),
      entry(tenant, "ad_spend", 2, "ad_spend/batch_02.csv"),
    ]);
    expect(results.map((r) => [r.source, r.batch, r.status])).toEqual([
      ["ad_spend", 1, "loaded"],
      ["ad_spend", 2, "quarantined"],
      ["refunds", 1, "loaded"],
    ]);
    const reasons = [
      { code: "unknown_header", message: expect.stringContaining('"spend_eur"') },
      { code: "missing_column", message: 'column "spend" is missing' },
    ];
    expect(results[1]?.reasons).toEqual(reasons);

    expect(await ledger(tenant.id)).toEqual([
      expect.objectContaining({ source: "ad_spend", batch_no: 1, status: "loaded", records: 1 }),
      expect.objectContaining({ source: "ad_spend", batch_no: 2, status: "quarantined", records: 0, detail: { reasons } }),
      expect.objectContaining({ source: "refunds", batch_no: 1, status: "loaded", records: 3 }),
    ]);
  });

  it("quarantines new bytes for a batch that is already loaded", async () => {
    const tenant = tenantFor("conflict");
    await register(tenant);
    const batches = [entry(tenant, "refunds", 1, "refunds/batch_01.csv")];
    await put(tenant, "refunds/batch_01.csv", refunds(2));
    expect((await load(tenant, batches))[0]?.status).toBe("loaded");

    await put(tenant, "refunds/batch_01.csv", refunds(3));
    const [result] = await load(tenant, batches);
    expect(result).toMatchObject({ status: "quarantined", reasons: [{ code: "batch_conflict" }] });
    expect(await ledger(tenant.id)).toEqual([
      expect.objectContaining({ batch_no: 1, status: "loaded", records: 2 }),
      expect.objectContaining({ batch_no: 1, status: "quarantined", records: 0 }),
    ]);
  });

  it("loads a file once when two runs race for it", async () => {
    const tenant = tenantFor("race");
    await register(tenant);
    const batches = [entry(tenant, "refunds", 1, "refunds/batch_01.csv")];
    await put(tenant, "refunds/batch_01.csv", refunds(5));

    const results = (await Promise.all([load(tenant, batches), load(tenant, batches)])).flat();
    expect(results.map((r) => r.status).sort()).toEqual(["loaded", "skipped"]);
    expect(results.find((r) => r.status === "skipped")?.rowCount).toBe(5);
    expect(await ledger(tenant.id)).toEqual([expect.objectContaining({ status: "loaded", records: 5 })]);
  });

  it("quarantines a loaded batch's bytes delivered again under another batch number", async () => {
    const tenant = tenantFor("dupe");
    await register(tenant);
    await put(tenant, "refunds/batch_01.csv", refunds(2));
    await put(tenant, "refunds/batch_02.csv", refunds(2));
    const first = entry(tenant, "refunds", 1, "refunds/batch_01.csv");
    const second = entry(tenant, "refunds", 2, "refunds/batch_02.csv");
    expect((await load(tenant, [first]))[0]?.status).toBe("loaded");

    const reasons = [{ code: "duplicate_content", message: expect.stringContaining("same content as batch 1") }];
    expect(await load(tenant, [first, second])).toEqual([
      expect.objectContaining({ batch: 1, status: "skipped", rowCount: 2 }),
      expect.objectContaining({ batch: 2, status: "quarantined", reasons }),
    ]);
    expect(await ledger(tenant.id)).toEqual([
      expect.objectContaining({ batch_no: 1, status: "loaded", records: 2 }),
      expect.objectContaining({ batch_no: 2, status: "quarantined", records: 0, detail: { reasons } }),
    ]);
  });

  it("loads identical bytes once when two runs race for them under different batch numbers", async () => {
    const tenant = tenantFor("dupe_race");
    await register(tenant);
    await put(tenant, "refunds/batch_01.csv", refunds(4));
    await put(tenant, "refunds/batch_02.csv", refunds(4));

    const results = (
      await Promise.all([
        load(tenant, [entry(tenant, "refunds", 1, "refunds/batch_01.csv")]),
        load(tenant, [entry(tenant, "refunds", 2, "refunds/batch_02.csv")]),
      ])
    ).flat();
    expect(results.map((r) => r.status).sort()).toEqual(["loaded", "quarantined"]);
    const [winner, loser] = results[0]?.status === "loaded" ? [1, 2] : [2, 1];
    expect(results.find((r) => r.status === "quarantined")).toMatchObject({
      batch: loser,
      reasons: [{ code: "duplicate_content", message: expect.stringContaining(`same content as batch ${winner}`) }],
    });
    const rows = (await ledger(tenant.id)).map((r) => [r.batch_no, r.status, r.records]);
    expect(rows).toHaveLength(2);
    expect(rows).toEqual(expect.arrayContaining([[winner, "loaded", 4], [loser, "quarantined", 0]]));
  });

  it("rolls back every insert chunk of a large file on a crash and loads it whole on rerun", async () => {
    const tenant = tenantFor("bulk");
    await register(tenant);
    // Two and a half chunks' worth of rows take three INSERT chunks, whatever the chunk size.
    const rows = Math.round(INSERT_CHUNK_SIZE * 2.5);
    await put(tenant, "refunds/batch_01.csv", refunds(rows));
    const batches = [entry(tenant, "refunds", 1, "refunds/batch_01.csv")];

    await expect(load(tenant, batches, { failAfterFiles: 0 })).rejects.toBeInstanceOf(InjectedFailure);
    expect(await ledger(tenant.id)).toEqual([]);
    expect(await rawCount(tenant.id)).toBe(0);

    expect(await load(tenant, batches)).toEqual([expect.objectContaining({ status: "loaded", rowCount: rows })]);
    expect(await ledger(tenant.id)).toEqual([expect.objectContaining({ status: "loaded", row_count: rows, records: rows })]);
    expect(await rawCount(tenant.id)).toBe(rows);
  });

  it("fails a file whose symlink leads out of the tenant's directory, blocks that tenant and loads the others", async () => {
    const tenant = tenantFor("symlink");
    const bystander = tenantFor("bystander");
    await register(tenant);
    await register(bystander);
    const elsewhere = await mkdtemp(join(tmpdir(), "loader-elsewhere-"));
    try {
      await writeFile(join(elsewhere, "refunds.csv"), refunds(2));
      await put(tenant, "refunds/batch_01.csv", refunds(1));
      await symlink(join(elsewhere, "refunds.csv"), join(root, "fixtures", tenant.id, "refunds/batch_02.csv"));
      await put(tenant, "refunds/batch_03.csv", refunds(3));
      await put(bystander, "refunds/batch_01.csv", refunds(4));

      const reported: FileResult[] = [];
      const results = await loadBatches({
        tenants: [tenant, bystander],
        manifest: {
          batches: [
            entry(tenant, "refunds", 1, "refunds/batch_01.csv"),
            entry(tenant, "refunds", 2, "refunds/batch_02.csv"),
            entry(tenant, "refunds", 3, "refunds/batch_03.csv"),
            entry(bystander, "refunds", 1, "refunds/batch_01.csv"),
          ],
        },
        rootDir: root,
        onResult: (result) => reported.push(result),
      });
      expect(results.map((r) => [r.tenant, r.batch, r.status])).toEqual([
        [bystander.id, 1, "loaded"],
        [tenant.id, 1, "loaded"],
        [tenant.id, 2, "failed"],
        [tenant.id, 3, "blocked"],
      ]);
      expect(results[2]?.error).toMatch(/batch_02\.csv resolves to .*, outside /);
      expect(reported).toEqual(results);
      expect(await ledger(tenant.id)).toEqual([expect.objectContaining({ batch_no: 1, status: "loaded", records: 1 })]);
      expect(await ledger(bystander.id)).toEqual([expect.objectContaining({ batch_no: 1, status: "loaded", records: 4 })]);
    } finally {
      await rm(elsewhere, { recursive: true, force: true });
    }
  });

  it("loads a file through a symlink that stays inside the tenant's directory", async () => {
    const tenant = tenantFor("symlink_inside");
    await register(tenant);
    const tenantDir = join(root, "fixtures", tenant.id);
    await put(tenant, "archive/refunds.csv", refunds(3));
    await mkdir(join(tenantDir, "refunds"), { recursive: true });
    await symlink(join(tenantDir, "archive/refunds.csv"), join(tenantDir, "refunds/batch_01.csv"));

    expect(await load(tenant, [entry(tenant, "refunds", 1, "refunds/batch_01.csv")])).toEqual([
      expect.objectContaining({ batch: 1, status: "loaded", rowCount: 3 }),
    ]);
    expect(await ledger(tenant.id)).toEqual([expect.objectContaining({ batch_no: 1, status: "loaded", records: 3 })]);
  });

  it("keeps loading when the onResult observer throws", async () => {
    const tenant = tenantFor("observer");
    await register(tenant);
    await put(tenant, "refunds/batch_01.csv", refunds(1));
    await put(tenant, "refunds/batch_02.csv", refunds(2));

    let calls = 0;
    const results = await loadBatches({
      tenants: [tenant],
      manifest: {
        batches: [entry(tenant, "refunds", 1, "refunds/batch_01.csv"), entry(tenant, "refunds", 2, "refunds/batch_02.csv")],
      },
      rootDir: root,
      onResult: () => {
        calls++;
        throw new Error("EPIPE");
      },
    });
    expect(calls).toBe(2);
    expect(results.map((r) => [r.batch, r.status, r.rowCount])).toEqual([
      [1, "loaded", 1],
      [2, "loaded", 2],
    ]);
    expect(await ledger(tenant.id)).toEqual([
      expect.objectContaining({ batch_no: 1, status: "loaded", records: 1 }),
      expect.objectContaining({ batch_no: 2, status: "loaded", records: 2 }),
    ]);
  });

  it("rejects a manifest entry outside the tenant's directory before loading anything", async () => {
    const tenant = tenantFor("escape");
    await register(tenant);
    await put(tenant, "refunds/batch_01.csv", refunds(1));
    const outside = { ...entry(tenant, "refunds", 2, "refunds/batch_01.csv"), path: `drift_${suffix}/refunds/batch_01.csv` };

    await expect(load(tenant, [entry(tenant, "refunds", 1, "refunds/batch_01.csv"), outside])).rejects.toThrow(
      /is outside fixtures\/escape_/,
    );
    expect(await ledger(tenant.id)).toEqual([]);
  });
});
