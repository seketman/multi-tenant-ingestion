/** Why a batch file was quarantined instead of loaded. Stored verbatim in ops.batch_file.detail. */
export interface QuarantineReason {
  code:
    | "unknown_header"
    | "missing_column"
    | "ambiguous_column"
    | "duplicate_header"
    | "column_count"
    | "malformed_csv"
    | "invalid_json"
    | "not_an_object"
    | "empty_line"
    | "empty_file"
    | "invalid_encoding"
    | "source_not_configured"
    | "batch_conflict"
    | "duplicate_content"
    | "truncated";
  message: string;
  /** Physical line number in the file, when the problem belongs to one line. */
  line?: number;
}

// Enough to diagnose a broken file without copying every bad line into the ledger.
const MAX_REASONS = 20;
const MAX_QUOTED_LENGTH = 100;

/**
 * Collects the reasons for one file, keeping the first MAX_REASONS and counting the rest
 * into a final `truncated` reason, so a file's ledger detail stays small however broken it is.
 */
export function reasonList(): { add: (reason: QuarantineReason) => void; list: () => QuarantineReason[] } {
  const reasons: QuarantineReason[] = [];
  let dropped = 0;
  return {
    add: (reason) => {
      if (reasons.length < MAX_REASONS) reasons.push(reason);
      else dropped++;
    },
    list: () =>
      dropped === 0 ? [...reasons] : [...reasons, { code: "truncated", message: `${dropped} more problems not listed` }],
  };
}

/** Caps an already collected list of reasons the same way `reasonList` does. */
export function capReasons(reasons: readonly QuarantineReason[]): QuarantineReason[] {
  const capped = reasonList();
  for (const reason of reasons) capped.add(reason);
  return capped.list();
}

/**
 * Shortens text taken from a batch file (a header name, a JSON key, a parser message
 * that quotes the input) before it goes into a reason message.
 */
export function clip(text: string): string {
  if (text.length <= MAX_QUOTED_LENGTH) return text;
  // Never cut a surrogate pair in half.
  return `${text.slice(0, MAX_QUOTED_LENGTH).replace(/[\uD800-\uDBFF]$/, "")}…`;
}
