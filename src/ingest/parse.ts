import { CsvError, type Info, parse } from "csv-parse/sync";
import type { SourceName } from "../config/sources.ts";
import { type ColumnAliases, resolveHeaders } from "./headers.ts";
import { capReasons, clip, type QuarantineReason, reasonList } from "./reasons.ts";

/** Each source arrives in one format. */
export const SOURCE_FORMAT = {
  orders: "csv",
  email_events: "ndjson",
  ad_spend: "csv",
  refunds: "csv",
} as const satisfies Record<SourceName, "csv" | "ndjson">;

/** One data line of a batch file, verbatim: original keys, CSV values as strings. */
export interface RawRecord {
  /** Physical line in the file: the CSV header is line 1, NDJSON starts at line 1. */
  lineNo: number;
  payload: Record<string, unknown>;
}

export type ParseResult =
  | { ok: true; /** raw header -> canonical column */ columns: Record<string, string>; records: RawRecord[] }
  | { ok: false; reasons: QuarantineReason[] };

const fail = (reason: QuarantineReason): ParseResult => ({ ok: false, reasons: [reason] });

/**
 * Parses a batch file and resolves its headers against the tenant's aliases. Any problem
 * rejects the whole file: a partially loaded batch would be harder to reason about than
 * a quarantined one. Trailing empty lines are ignored; an empty line anywhere else is not.
 */
export function parseBatchFile(bytes: Uint8Array, source: SourceName, aliases: ColumnAliases = {}): ParseResult {
  let text: string;
  try {
    // The decoder drops a leading byte order mark, for CSV and NDJSON alike.
    text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    return fail({ code: "invalid_encoding", message: "file is not valid UTF-8" });
  }
  text = text.replace(/(?:\r?\n)+$/, "");
  if (text === "") {
    return fail({ code: "empty_file", message: "file has no content" });
  }
  return SOURCE_FORMAT[source] === "csv" ? parseCsv(text, source, aliases) : parseNdjson(text, source, aliases);
}

function parseCsv(text: string, source: SourceName, aliases: ColumnAliases): ParseResult {
  let rows: { record: string[]; info: Info }[];
  try {
    // Every record must have as many fields as the header (csv-parse's default), and
    // empty lines are kept so they fail that check instead of vanishing.
    rows = parse(text, { info: true, skip_empty_lines: false }) as unknown as { record: string[]; info: Info }[];
  } catch (error) {
    if (error instanceof CsvError && error.code === "CSV_RECORD_INCONSISTENT_FIELDS_LENGTH") {
      const line = typeof error.lines === "number" ? error.lines : undefined;
      return fail({ code: "column_count", message: clip(error.message), ...(line === undefined ? {} : { line }) });
    }
    return fail({ code: "malformed_csv", message: clip(error instanceof Error ? error.message : String(error)) });
  }

  const [header, ...data] = rows;
  if (header === undefined) {
    return fail({ code: "empty_file", message: "file has no header row" });
  }
  const resolution = resolveHeaders(source, header.record, aliases);
  if (!resolution.ok) return { ok: false, reasons: capReasons(resolution.reasons) };

  // info.lines is the line a record ends on; a quoted field may span lines, so a record
  // starts on the line after the previous one ended.
  let previousEnd = header.info.lines;
  const records = data.map(({ record, info }) => {
    const lineNo = previousEnd + 1;
    previousEnd = info.lines;
    return { lineNo, payload: Object.fromEntries(header.record.map((name, i) => [name, record[i]])) };
  });
  return { ok: true, columns: resolution.columns, records };
}

function parseNdjson(text: string, source: SourceName, aliases: ColumnAliases): ParseResult {
  const { add: report, list: reasons } = reasonList();

  // Keys may differ from line to line (canonical on some, an alias on others); the union
  // is still one unambiguous raw -> canonical map because every alias has a single owner.
  // Null prototype, as in resolveHeaders: Object.assign sets keys with plain assignment, so
  // a `__proto__` key would otherwise replace this object's prototype instead of being copied.
  const columns = Object.create(null) as Record<string, string>;
  const records: RawRecord[] = [];
  text.split(/\r?\n/).forEach((line, index) => {
    const lineNo = index + 1;
    if (line.trim() === "") {
      report({ code: "empty_line", message: "empty line", line: lineNo });
      return;
    }
    let value: unknown;
    try {
      value = JSON.parse(line);
    } catch (error) {
      // V8 quotes the offending input in the message.
      report({ code: "invalid_json", message: clip((error as Error).message), line: lineNo });
      return;
    }
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      report({ code: "not_an_object", message: "line is not a JSON object", line: lineNo });
      return;
    }
    const resolution = resolveHeaders(source, Object.keys(value), aliases);
    if (!resolution.ok) {
      for (const reason of resolution.reasons) report({ ...reason, line: lineNo });
      return;
    }
    Object.assign(columns, resolution.columns);
    records.push({ lineNo, payload: value as Record<string, unknown> });
  });

  const problems = reasons();
  return problems.length === 0 ? { ok: true, columns, records } : { ok: false, reasons: problems };
}
