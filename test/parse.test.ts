import { describe, expect, it } from "vitest";
import { parseBatchFile } from "../src/ingest/parse.ts";

const bytes = (text: string) => new TextEncoder().encode(text);
const REFUNDS_HEADER = "refund_id,refunded_at,order_id,amount,currency";
const event = (overrides: Record<string, unknown> = {}) =>
  JSON.stringify({
    event_id: "e-1",
    type: "open",
    email: "a@example.invalid",
    campaign_id: "c-1",
    occurred_at: "2026-01-06T00:00:00Z",
    ...overrides,
  });

describe("CSV batch files", () => {
  it("numbers records by physical line, the header being line 1", () => {
    const text = `${REFUNDS_HEADER}\nrf-1,2026-01-06T00:00:00Z,o-1,10.50,USD\nrf-2,2026-01-07T00:00:00Z,o-2,3.00,USD\n`;
    expect(parseBatchFile(bytes(text), "refunds")).toEqual({
      ok: true,
      columns: Object.fromEntries(REFUNDS_HEADER.split(",").map((c) => [c, c])),
      records: [
        {
          lineNo: 2,
          payload: { refund_id: "rf-1", refunded_at: "2026-01-06T00:00:00Z", order_id: "o-1", amount: "10.50", currency: "USD" },
        },
        {
          lineNo: 3,
          payload: { refund_id: "rf-2", refunded_at: "2026-01-07T00:00:00Z", order_id: "o-2", amount: "3.00", currency: "USD" },
        },
      ],
    });
  });

  it("counts lines spanned by a quoted field", () => {
    const text = `${REFUNDS_HEADER}\nrf-1,"2026-01-06\nT00:00:00Z",o-1,1,USD\nrf-2,x,o-2,2,USD`;
    const result = parseBatchFile(bytes(text), "refunds");
    expect(result.ok && result.records.map((r) => r.lineNo)).toEqual([2, 4]);
  });

  it("keeps the original header names in the payload when an alias is used", () => {
    const text = "date,campaign_id,platform,cost_usd\n2026-01-24,c-1,Meta,364.01\n";
    const result = parseBatchFile(bytes(text), "ad_spend", { spend: ["cost_usd"] });
    expect(result).toMatchObject({
      ok: true,
      columns: { cost_usd: "spend" },
      records: [{ lineNo: 2, payload: { date: "2026-01-24", campaign_id: "c-1", platform: "Meta", cost_usd: "364.01" } }],
    });
  });

  it("quarantines a row whose field count differs from the header", () => {
    const text = `${REFUNDS_HEADER}\nrf-1,2026-01-06T00:00:00Z,o-1,10.50,USD\nrf-2,2026-01-07T00:00:00Z,o-2,3.00\n`;
    expect(parseBatchFile(bytes(text), "refunds")).toEqual({
      ok: false,
      reasons: [{ code: "column_count", message: expect.stringMatching(/expect 5, got 4/), line: 3 }],
    });
  });

  it("reports the physical line of a bad row that follows a multi-line quoted record", () => {
    const text = `${REFUNDS_HEADER}\nrf-1,"first\nsecond\nthird",o-1,1,USD\nrf-2,x,o-2,2,USD\nrf-3,x,o-3,3\n`;
    expect(parseBatchFile(bytes(text), "refunds")).toEqual({
      ok: false,
      reasons: [{ code: "column_count", message: expect.stringMatching(/expect 5, got 4/), line: 6 }],
    });
  });

  it("caps the reasons listed for a header with many unknown columns", () => {
    const extra = Array.from({ length: 30 }, (_, i) => `extra_${i}`);
    const result = parseBatchFile(bytes([REFUNDS_HEADER, ...extra].join(",")), "refunds");
    const reasons = result.ok ? [] : result.reasons;
    expect(reasons).toHaveLength(21);
    expect(reasons.slice(0, 20).every((r) => r.code === "unknown_header")).toBe(true);
    expect(reasons.at(-1)).toEqual({ code: "truncated", message: "10 more problems not listed" });
  });

  it("quarantines an empty line in the middle but ignores trailing ones", () => {
    const row = "rf-1,2026-01-06T00:00:00Z,o-1,10.50,USD";
    expect(parseBatchFile(bytes(`${REFUNDS_HEADER}\n\n${row}\n`), "refunds")).toMatchObject({
      ok: false,
      reasons: [{ code: "column_count", line: 2 }],
    });
    expect(parseBatchFile(bytes(`${REFUNDS_HEADER}\n${row}\n\n\r\n`), "refunds")).toMatchObject({ ok: true });
  });

  it("strips a byte order mark before reading the header", () => {
    const text = `﻿${REFUNDS_HEADER}\r\nrf-1,2026-01-06T00:00:00Z,o-1,10.50,USD\r\n`;
    const result = parseBatchFile(bytes(text), "refunds");
    expect(result).toMatchObject({ ok: true, records: [{ lineNo: 2, payload: { refund_id: "rf-1", currency: "USD" } }] });
  });

  it("quarantines a file that is not UTF-8", () => {
    const invalid = new Uint8Array([...bytes(`${REFUNDS_HEADER}\nrf-1,x,o-1,1,`), 0xff, 0x0a]);
    expect(parseBatchFile(invalid, "refunds")).toMatchObject({ ok: false, reasons: [{ code: "invalid_encoding" }] });
  });

  it("quarantines an empty file", () => {
    expect(parseBatchFile(bytes("\n"), "refunds")).toMatchObject({ ok: false, reasons: [{ code: "empty_file" }] });
  });
});

describe("NDJSON batch files", () => {
  it("keeps parsed values verbatim and numbers lines from 1", () => {
    const text = `${event()}\n${event({ event_id: "e-2", campaign_id: null })}\n`;
    expect(parseBatchFile(bytes(text), "email_events")).toMatchObject({
      ok: true,
      records: [
        { lineNo: 1, payload: { event_id: "e-1" } },
        { lineNo: 2, payload: { event_id: "e-2", campaign_id: null } },
      ],
    });
  });

  it("accepts the canonical key on some lines and a declared alias on others", () => {
    const { type, ...rest } = JSON.parse(event()) as Record<string, unknown>;
    const aliased = JSON.stringify({ ...rest, event_id: "e-2", event_type: type });
    const result = parseBatchFile(bytes(`${event()}\n${aliased}\n`), "email_events", { type: ["event_type"] });
    expect(result).toMatchObject({
      ok: true,
      columns: { type: "type", event_type: "type" },
      records: [
        { lineNo: 1, payload: { event_id: "e-1", type: "open" } },
        { lineNo: 2, payload: { event_id: "e-2", event_type: "open" } },
      ],
    });
  });

  it("quarantines the whole file when one line has a bad key set", () => {
    const text = [event(), event({ utm_source: "x" }), event()].join("\n");
    expect(parseBatchFile(bytes(text), "email_events")).toEqual({
      ok: false,
      reasons: [{ code: "unknown_header", message: expect.stringContaining('"utm_source"'), line: 2 }],
    });
  });

  it("quarantines invalid JSON, non-object lines and inner empty lines, each with its line", () => {
    const text = [event(), "{not json", "[1,2]", "", event()].join("\n");
    const result = parseBatchFile(bytes(text), "email_events");
    expect(result.ok ? [] : result.reasons.map((r) => [r.code, r.line])).toEqual([
      ["invalid_json", 2],
      ["not_an_object", 3],
      ["empty_line", 4],
    ]);
  });

  it("caps the reasons listed for a file with many bad lines", () => {
    const text = Array.from({ length: 25 }, () => "{").join("\n");
    const result = parseBatchFile(bytes(text), "email_events");
    expect(result.ok ? [] : result.reasons).toHaveLength(21);
    expect(result.ok ? undefined : result.reasons.at(-1)).toEqual({ code: "truncated", message: "5 more problems not listed" });
  });
});
