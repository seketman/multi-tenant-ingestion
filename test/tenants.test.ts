import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { loadTenants, tenantConfigSchema } from "../src/config/tenants.ts";

describe("tenant config", () => {
  it("loads and validates every tenant file", async () => {
    const tenants = await loadTenants();
    expect(tenants.map((t) => [t.id, t.currency])).toEqual([
      ["lumen", "EUR"],
      ["northwind", "USD"],
    ]);
    for (const tenant of tenants) {
      expect((await stat(tenant.fixturesDir)).isDirectory()).toBe(true);
    }
  });

  it("rejects an alias for a column the source does not have", () => {
    const result = tenantConfigSchema.safeParse({
      id: "acme",
      displayName: "Acme",
      currency: "USD",
      fixturesDir: "fixtures/acme",
      sources: { ad_spend: { columnAliases: { not_a_column: ["x"] } } },
    });
    expect(result.success).toBe(false);
  });

  const withSources = (sources: unknown, fixturesDir = "fixtures/acme") => ({
    id: "acme",
    displayName: "Acme",
    currency: "USD",
    fixturesDir,
    sources,
  });
  const issues = (config: unknown) => {
    const result = tenantConfigSchema.safeParse(config);
    return result.success ? [] : result.error.issues.map((i) => i.message);
  };

  it("rejects one alias mapped to two canonical columns", () => {
    const messages = issues(
      withSources({ orders: { columnAliases: { gross: ["amount"], channel: ["amount"] } } }),
    );
    expect(messages).toEqual([expect.stringMatching(/"amount" is mapped to both "gross" and "channel"/)]);
  });

  it("rejects an alias that is another canonical column's name", () => {
    const messages = issues(withSources({ orders: { columnAliases: { gross: ["currency"] } } }));
    expect(messages).toEqual([expect.stringMatching(/"currency" for "gross" is itself a column of orders/)]);
  });

  it("rejects a config without any source", () => {
    expect(issues(withSources({}))).toEqual([expect.stringMatching(/at least one source is required/)]);
  });

  it("rejects an alias that repeats its own canonical name", () => {
    const messages = issues(withSources({ ad_spend: { columnAliases: { spend: ["spend"] } } }));
    expect(messages).toEqual([expect.stringMatching(/repeats the canonical name/)]);
  });

  it("allows the same alias in different sources", () => {
    const config = withSources({
      orders: { columnAliases: { gross: ["total"] } },
      refunds: { columnAliases: { amount: ["total"] } },
    });
    expect(issues(config)).toEqual([]);
  });

  it.each(["/etc/acme", "C:\\data\\acme", "../outside", "fixtures/../../outside", "fixtures\\..\\x"])(
    "rejects fixturesDir %s",
    (dir) => {
      expect(issues(withSources({ refunds: {} }, dir))).not.toEqual([]);
    },
  );

  it("accepts a relative fixturesDir inside the repository", () => {
    expect(issues(withSources({ refunds: {} }, "fixtures/acme..v2"))).toEqual([]);
  });

  it("rejects a file whose name does not match its id", async () => {
    const dir = await mkdtemp(join(tmpdir(), "tenants-"));
    try {
      const lumen = await readFile("tenants/lumen.json", "utf8");
      await writeFile(join(dir, "other.json"), lumen);
      await expect(loadTenants(dir)).rejects.toThrow(/file name must match the id/);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
