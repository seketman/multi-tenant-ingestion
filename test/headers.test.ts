import { describe, expect, it } from "vitest";
import { resolveHeaders } from "../src/ingest/headers.ts";

const codes = (result: ReturnType<typeof resolveHeaders>) => (result.ok ? [] : result.reasons.map((r) => r.code));

describe("header resolution", () => {
  it("maps canonical headers onto themselves, in any order", () => {
    expect(resolveHeaders("ad_spend", ["spend", "date", "platform", "campaign_id"])).toEqual({
      ok: true,
      columns: { spend: "spend", date: "date", platform: "platform", campaign_id: "campaign_id" },
    });
  });

  it("maps a declared alias onto its canonical column", () => {
    const result = resolveHeaders("ad_spend", ["date", "campaign_id", "platform", "cost_usd"], {
      spend: ["cost_usd"],
    });
    expect(result).toEqual({
      ok: true,
      columns: { date: "date", campaign_id: "campaign_id", platform: "platform", cost_usd: "spend" },
    });
  });

  it("rejects a header that is neither canonical nor a declared alias", () => {
    const result = resolveHeaders("ad_spend", ["date", "campaign_id", "platform", "spend", "notes"]);
    expect(result).toEqual({
      ok: false,
      reasons: [{ code: "unknown_header", message: expect.stringContaining('"notes"') }],
    });
  });

  it("clips a very long header name in the message", () => {
    const long = "x".repeat(10_000);
    const result = resolveHeaders("ad_spend", ["date", "campaign_id", "platform", "spend", long]);
    const message = result.ok ? "" : (result.reasons[0]?.message ?? "");
    expect(message).toContain(`"${"x".repeat(100)}…"`);
    expect(message.length).toBeLessThan(200);
  });

  it("does not accept an alias the tenant did not declare", () => {
    expect(codes(resolveHeaders("ad_spend", ["date", "campaign_id", "platform", "cost_usd"]))).toEqual([
      "unknown_header",
      "missing_column",
    ]);
  });

  it("rejects a missing canonical column", () => {
    const result = resolveHeaders("refunds", ["refund_id", "refunded_at", "order_id", "amount"]);
    expect(result).toEqual({
      ok: false,
      reasons: [{ code: "missing_column", message: 'column "currency" is missing' }],
    });
  });

  it("rejects an alias and its canonical column in the same file", () => {
    const result = resolveHeaders("ad_spend", ["date", "campaign_id", "platform", "spend", "cost_usd"], {
      spend: ["cost_usd"],
    });
    expect(result).toEqual({
      ok: false,
      reasons: [{ code: "ambiguous_column", message: 'headers "spend" and "cost_usd" both map to column "spend"' }],
    });
  });

  it("rejects an undeclared header named __proto__ instead of dropping it", () => {
    const result = resolveHeaders("ad_spend", ["date", "campaign_id", "platform", "spend", "__proto__"]);
    expect(result).toEqual({
      ok: false,
      reasons: [{ code: "unknown_header", message: expect.stringContaining('"__proto__"') }],
    });
  });

  it("maps a declared alias named __proto__ as an own key", () => {
    const result = resolveHeaders("ad_spend", ["date", "campaign_id", "platform", "__proto__"], {
      spend: ["__proto__"],
    });
    expect(result.ok).toBe(true);
    const columns = result.ok ? result.columns : {};
    expect(Object.hasOwn(columns, "__proto__")).toBe(true);
    expect(columns["__proto__"]).toBe("spend");
    expect(JSON.parse(JSON.stringify(columns))).toEqual(
      JSON.parse('{"date":"date","campaign_id":"campaign_id","platform":"platform","__proto__":"spend"}'),
    );
  });

  it("rejects a repeated header", () => {
    expect(codes(resolveHeaders("ad_spend", ["date", "campaign_id", "platform", "spend", "date"]))).toEqual([
      "duplicate_header",
    ]);
  });
});
