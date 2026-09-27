import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    // Tests share one real database; run files sequentially to keep them independent.
    fileParallelism: false,
    testTimeout: 15_000,
    allowOnly: false,
  },
});
