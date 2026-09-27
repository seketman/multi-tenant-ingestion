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
    batches.forEach(({ tenant, source, batch, covers_from, covers_to }, index) => {
      const key = `${tenant}/${source}/${batch}`;
      if (seen.has(key)) {
        ctx.addIssue({ code: "custom", path: ["batches", index], message: `batch ${key} is listed twice` });
      }
      seen.add(key);
      // An inverted window would never match a day in covers(), so the completeness gate
      // would withhold nothing for this batch. YYYY-MM-DD compares correctly as a string;
      // equal dates are a valid one-day window.
      if (covers_from > covers_to) {
        ctx.addIssue({
          code: "custom",
          path: ["batches", index],
          message: `batch ${key} covers ${covers_from} to ${covers_to}: covers_from is after covers_to`,
        });
      }
    });
  });

export type Manifest = z.infer<typeof manifestSchema>;
export type ManifestEntry = Manifest["batches"][number];

/** Reads and validates a manifest file. */
export async function loadManifest(path: string): Promise<Manifest> {
  const text = await readFile(path, "utf8");
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch (error) {
    throw new Error(`Invalid manifest ${path}: ${(error as Error).message}`, { cause: error });
  }
  const parsed = manifestSchema.safeParse(raw);
  if (!parsed.success) {
    throw new Error(`Invalid manifest ${path}:\n${z.prettifyError(parsed.error)}`);
  }
  return parsed.data;
}
