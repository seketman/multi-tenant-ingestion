import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { loadManifest, type ManifestEntry, manifestSchema } from "../src/ingest/manifest.ts";

const entry = (batch: number, covers_from: string, covers_to: string): ManifestEntry => ({
  tenant: "northwind",
  source: "orders",
  batch,
  path: `northwind/orders/batch_0${batch}.csv`,
  covers_from,
  covers_to,
});

describe("manifestSchema", () => {
  it("rejects a batch whose covers_from is after covers_to", () => {
    const parsed = manifestSchema.safeParse({ batches: [entry(1, "2026-01-11", "2026-01-06")] });
    expect(parsed.success).toBe(false);
    expect(parsed.error?.issues).toEqual([
      expect.objectContaining({
        path: ["batches", 0],
        message: "batch northwind/orders/1 covers 2026-01-11 to 2026-01-06: covers_from is after covers_to",
      }),
    ]);
  });

  it("accepts a one-day window", () => {
    const parsed = manifestSchema.safeParse({ batches: [entry(1, "2026-01-06", "2026-01-06")] });
    expect(parsed.success).toBe(true);
  });

  it("still rejects a batch listed twice", () => {
    const parsed = manifestSchema.safeParse({
      batches: [entry(1, "2026-01-06", "2026-01-11"), entry(1, "2026-01-12", "2026-01-17")],
    });
    expect(parsed.success).toBe(false);
    expect(parsed.error?.issues).toEqual([
      expect.objectContaining({ path: ["batches", 1], message: "batch northwind/orders/1 is listed twice" }),
    ]);
  });

  it("loads the supplied fixtures manifest", async () => {
    const manifest = await loadManifest("fixtures/manifest.json");
    expect(manifest.batches.length).toBeGreaterThan(0);
  });

  it("names the file on a JSON syntax error", async () => {
    const dir = await mkdtemp(join(tmpdir(), "manifest-"));
    const path = join(dir, "manifest.json");
    try {
      await writeFile(path, '{ "batches": [], }');
      await expect(loadManifest(path)).rejects.toThrow(`Invalid manifest ${path}: `);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
