import type { SourceName } from "../config/sources.ts";
import type { ManifestEntry } from "./manifest.ts";

/** One ledger row (ops.batch_file) as far as coverage is concerned. */
export interface LedgerAttempt {
  source: SourceName;
  batch_no: number;
  status: "loaded" | "quarantined";
}

/**
 * A manifest batch with no loaded ledger row: the days it covers are missing that
 * source's data, so any number built from that source for those days is incomplete.
 * - not_received: no ledger row at all.
 * - quarantined: attempted, never loaded.
 */
export interface IncompleteWindow {
  source: SourceName;
  batch: number;
  path: string;
  /** YYYY-MM-DD, inclusive. */
  coversFrom: string;
  /** YYYY-MM-DD, inclusive. */
  coversTo: string;
  status: "not_received" | "quarantined";
}

/**
 * The manifest batches among `entries` that no ledger row loaded. A batch counts as
 * covered once any attempt of it loaded, even if a later redelivery was quarantined
 * (`pnpm check` reports that case as conflicting_redelivery). Pure: the caller reads the
 * ledger in the tenant's scope and passes only that tenant's manifest entries.
 */
export function incompleteWindows(entries: readonly ManifestEntry[], ledger: readonly LedgerAttempt[]): IncompleteWindow[] {
  return entries.flatMap((entry): IncompleteWindow[] => {
    const attempts = ledger.filter((r) => r.source === entry.source && r.batch_no === entry.batch);
    if (attempts.some((r) => r.status === "loaded")) return [];
    return [
      {
        source: entry.source,
        batch: entry.batch,
        path: entry.path,
        coversFrom: entry.covers_from,
        coversTo: entry.covers_to,
        status: attempts.length === 0 ? "not_received" : "quarantined",
      },
    ];
  });
}

/** True when the YYYY-MM-DD `day` falls inside `window`, bounds included. */
export const covers = (window: IncompleteWindow, day: string): boolean => day >= window.coversFrom && day <= window.coversTo;
