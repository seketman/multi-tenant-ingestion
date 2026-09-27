import { readFile } from "node:fs/promises";
import { z } from "zod";
import { SOURCE_NAMES } from "../config/sources.ts";

const isoDate = z.string().regex(/^\d{4}-\d{2}-\d{2}$/, "YYYY-MM-DD");

const manifestEntrySchema = z.object({
  tenant: z.string().min(1),
  source: z.enum(SOURCE_NAMES),
  batch: z.number().int().positive(),
  /** Relative to the fixtures root, e.g. "northwind/orders/batch_01.csv". */
  path: z.string().min(1),
  covers_from: isoDate,
  covers_to: isoDate,
});

/** The list of batch files the delivery is supposed to contain. */
export const manifestSchema = z
  .object({ batches: z.array(manifestEntrySchema) })
  .superRefine(({ batches }, ctx) => {
    const seen = new Set<string>();
    batches.forEach(({ tenant, source, batch }, index) => {
      const key = `${tenant}/${source}/${batch}`;
      if (seen.has(key)) {
        ctx.addIssue({ code: "custom", path: ["batches", index], message: `batch ${key} is listed twice` });
      }
      seen.add(key);
    });
  });

export type Manifest = z.infer<typeof manifestSchema>;
export type ManifestEntry = Manifest["batches"][number];

/** Reads and validates a manifest file. */
export async function loadManifest(path: string): Promise<Manifest> {
  const raw: unknown = JSON.parse(await readFile(path, "utf8"));
  const parsed = manifestSchema.safeParse(raw);
  if (!parsed.success) {
    throw new Error(`Invalid manifest ${path}:\n${z.prettifyError(parsed.error)}`);
  }
  return parsed.data;
}
