import { SOURCE_COLUMNS, type SourceName } from "../config/sources.ts";
import { clip, type QuarantineReason } from "./reasons.ts";

/** A tenant's declared aliases for one source: canonical column -> other header names. */
export type ColumnAliases = Partial<Record<string, readonly string[]>>;

export type HeaderResolution =
  | { ok: true; /** raw header -> canonical column */ columns: Record<string, string> }
  | { ok: false; reasons: QuarantineReason[] };

/**
 * Maps each raw header (CSV) or key (NDJSON) onto a canonical column of the source.
 * A header resolves when it is the canonical name itself or an alias the tenant declared.
 * The file is rejected on an unknown header, a repeated header, two headers resolving to
 * the same column (for example both `spend` and its alias `cost_usd`), or a missing column,
 * because any of those would make the canonical value of a row ambiguous or absent.
 * Header names are clipped in messages; the caller bounds how many reasons it keeps.
 */
export function resolveHeaders(
  source: SourceName,
  headers: readonly string[],
  aliases: ColumnAliases = {},
): HeaderResolution {
  const canonical: readonly string[] = SOURCE_COLUMNS[source];
  const aliasOwner = new Map<string, string>();
  for (const [column, names] of Object.entries(aliases)) {
    for (const name of names ?? []) aliasOwner.set(name, column);
  }

  const reasons: QuarantineReason[] = [];
  const columns: Record<string, string> = {};
  const seen = new Set<string>();
  const resolvedFrom = new Map<string, string>();
  for (const header of headers) {
    if (seen.has(header)) {
      reasons.push({ code: "duplicate_header", message: `header "${clip(header)}" appears more than once` });
      continue;
    }
    seen.add(header);
    const column = canonical.includes(header) ? header : aliasOwner.get(header);
    if (column === undefined) {
      reasons.push({
        code: "unknown_header",
        message: `header "${clip(header)}" is neither a column of ${source} nor a declared alias`,
      });
      continue;
    }
    const first = resolvedFrom.get(column);
    if (first !== undefined) {
      reasons.push({
        code: "ambiguous_column",
        message: `headers "${clip(first)}" and "${clip(header)}" both map to column "${column}"`,
      });
      continue;
    }
    resolvedFrom.set(column, header);
    columns[header] = column;
  }
  for (const column of canonical) {
    if (!resolvedFrom.has(column)) {
      reasons.push({ code: "missing_column", message: `column "${column}" is missing` });
    }
  }
  return reasons.length === 0 ? { ok: true, columns } : { ok: false, reasons };
}
