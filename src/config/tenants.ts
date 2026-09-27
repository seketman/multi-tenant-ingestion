import { readdir, readFile } from "node:fs/promises";
import { basename, isAbsolute, join, win32 } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import { canonicalValues, SOURCE_COLUMNS, SOURCE_NAMES, type SourceName } from "./sources.ts";

const DEFAULT_TENANTS_DIR = fileURLToPath(new URL("../../tenants/", import.meta.url));

/**
 * Per-source mapping from a tenant's raw export onto the canonical columns.
 * - columnAliases: canonical column -> other header names that mean the same thing.
 *   The canonical name itself is always accepted, so listing it is rejected as redundant.
 *   Every alias must resolve to exactly one canonical column: an alias may appear only
 *   once across the source and may not be another canonical column's name.
 * - valueMaps: canonical column -> raw value -> canonical value. For a column with closed
 *   semantics (CANONICAL_VALUES), every target must be one of its canonical values: a typo
 *   would otherwise load fine and never be counted.
 */
const sourceMappingSchema = (source: SourceName) => {
  const columns: readonly string[] = SOURCE_COLUMNS[source];
  const closed = canonicalValues(source);
  const column = z.enum(SOURCE_COLUMNS[source]);
  return z
    .object({
      columnAliases: z.partialRecord(column, z.array(z.string().min(1)).min(1)).default({}),
      valueMaps: z.partialRecord(column, z.record(z.string(), z.string())).optional(),
    })
    .strict()
    .superRefine(({ columnAliases, valueMaps }, ctx) => {
      const owner = new Map<string, string>();
      for (const [canonical, aliases] of Object.entries(columnAliases) as [string, string[] | undefined][]) {
        for (const alias of aliases ?? []) {
          const path = ["columnAliases", canonical];
          if (alias === canonical) {
            ctx.addIssue({
              code: "custom",
              path,
              message: `alias "${alias}" repeats the canonical name, which is always accepted`,
            });
          } else if (columns.includes(alias)) {
            ctx.addIssue({
              code: "custom",
              path,
              message: `alias "${alias}" for "${canonical}" is itself a column of ${source}, so that header would be ambiguous`,
            });
          } else if (owner.has(alias)) {
            const first = owner.get(alias);
            ctx.addIssue({
              code: "custom",
              path,
              message:
                first === canonical
                  ? `alias "${alias}" is listed twice for "${canonical}"`
                  : `alias "${alias}" is mapped to both "${first}" and "${canonical}"; a header must map to one column`,
            });
          } else {
            owner.set(alias, canonical);
          }
        }
      }
      for (const [canonical, map] of Object.entries(valueMaps ?? {}) as [string, Record<string, string> | undefined][]) {
        const allowed = closed[canonical];
        if (allowed === undefined) continue;
        for (const [raw, target] of Object.entries(map ?? {})) {
          if (!allowed.includes(target)) {
            ctx.addIssue({
              code: "custom",
              path: ["valueMaps", canonical, raw],
              message: `value map target "${target}" for ${source}.${canonical} is not one of ${allowed.join(", ")}`,
            });
          }
        }
      }
    });
};

/** A path inside the repository: relative, and never climbing out with "..". */
const fixturesDirSchema = z
  .string()
  .min(1)
  .refine((dir) => !isAbsolute(dir) && !win32.isAbsolute(dir), "must be a relative path")
  .refine((dir) => !dir.split(/[\\/]/).includes(".."), 'must not contain ".." segments');

export const tenantConfigSchema = z
  .object({
    id: z.string().regex(/^[a-z][a-z0-9_]{1,62}$/, "lowercase letters, digits and underscores"),
    displayName: z.string().min(1),
    currency: z.string().regex(/^[A-Z]{3}$/, "ISO 4217 code"),
    fixturesDir: fixturesDirSchema,
    sources: z
      .object(Object.fromEntries(SOURCE_NAMES.map((s) => [s, sourceMappingSchema(s).optional()])) as {
        [S in SourceName]: z.ZodOptional<ReturnType<typeof sourceMappingSchema>>;
      })
      .strict()
      .refine((sources) => Object.values(sources).some(Boolean), "at least one source is required"),
  })
  .strict();

export type TenantConfig = z.infer<typeof tenantConfigSchema>;

/** Loads and validates every `*.json` file in the tenants directory. */
export async function loadTenants(dir: string = process.env.TENANTS_DIR ?? DEFAULT_TENANTS_DIR): Promise<TenantConfig[]> {
  const files = (await readdir(dir)).filter((f) => f.endsWith(".json")).sort();
  const tenants: TenantConfig[] = [];
  for (const file of files) {
    const raw: unknown = JSON.parse(await readFile(join(dir, file), "utf8"));
    const parsed = tenantConfigSchema.safeParse(raw);
    if (!parsed.success) {
      throw new Error(`Invalid tenant config ${file}:\n${z.prettifyError(parsed.error)}`);
    }
    if (parsed.data.id !== basename(file, ".json")) {
      throw new Error(`Tenant config ${file} declares id "${parsed.data.id}"; the file name must match the id`);
    }
    tenants.push(parsed.data);
  }
  return tenants;
}
