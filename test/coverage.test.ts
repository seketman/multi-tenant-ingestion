import { describe, expect, it } from "vitest";
import { covers, type IncompleteWindow, incompleteWindows, type LedgerAttempt } from "../src/ingest/coverage.ts";
import type { ManifestEntry } from "../src/ingest/manifest.ts";

const entry = (source: ManifestEntry["source"], batch: number): ManifestEntry => ({
  tenant: "t",
  source,
  batch,
  path: `t/${source}/batch_0${batch}.csv`,
  covers_from: "2026-01-06",
  covers_to: "2026-01-11",
});

const window = (source: ManifestEntry["source"], batch: number, status: IncompleteWindow["status"]): IncompleteWindow => ({
  source,
  batch,
  path: `t/${source}/batch_0${batch}.csv`,
  coversFrom: "2026-01-06",
  coversTo: "2026-01-11",
  status,
});

describe("incompleteWindows", () => {
  it("reports a batch with no ledger row as not received", () => {
    expect(incompleteWindows([entry("orders", 1)], [])).toEqual([window("orders", 1, "not_received")]);
  });

  it("reports a batch with only quarantined attempts as quarantined", () => {
    const ledger: LedgerAttempt[] = [
      { source: "orders", batch_no: 1, status: "quarantined" },
      { source: "orders", batch_no: 1, status: "quarantined" },
    ];
    expect(incompleteWindows([entry("orders", 1)], ledger)).toEqual([window("orders", 1, "quarantined")]);
  });

  it("counts a batch as covered once any attempt loaded, even with a later quarantined redelivery", () => {
    const loaded: LedgerAttempt = { source: "orders", batch_no: 1, status: "loaded" };
    const redelivery: LedgerAttempt = { source: "orders", batch_no: 1, status: "quarantined" };
    expect(incompleteWindows([entry("orders", 1)], [loaded])).toEqual([]);
    expect(incompleteWindows([entry("orders", 1)], [loaded, redelivery])).toEqual([]);
    expect(incompleteWindows([entry("orders", 1)], [redelivery, loaded])).toEqual([]);
  });

  it("matches ledger rows on both source and batch", () => {
    // Loaded rows for the same batch number of another source, and another batch of the same source.
    const ledger: LedgerAttempt[] = [
      { source: "refunds", batch_no: 2, status: "loaded" },
      { source: "orders", batch_no: 1, status: "loaded" },
      { source: "refunds", batch_no: 1, status: "quarantined" },
    ];
    expect(incompleteWindows([entry("orders", 1), entry("orders", 2), entry("refunds", 1)], ledger)).toEqual([
      window("orders", 2, "not_received"),
      window("refunds", 1, "quarantined"),
    ]);
  });
});

describe("covers", () => {
  const w = window("orders", 1, "not_received");

  it("includes both bounds", () => {
    expect(covers(w, "2026-01-06")).toBe(true);
    expect(covers(w, "2026-01-08")).toBe(true);
    expect(covers(w, "2026-01-11")).toBe(true);
  });

  it("excludes the days just outside", () => {
    expect(covers(w, "2026-01-05")).toBe(false);
    expect(covers(w, "2026-01-12")).toBe(false);
  });
});
