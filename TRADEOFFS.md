# Tradeoffs

Short version: the loading path (replay, drift, isolation) is built deep and tested against a real database (106 tests in 9 files). Modelling, late arrivals and missing-source detection are built on top of it. Currency conversion, scheduling and large-file streaming are not built, on purpose.

Built with AI assistance under my direction. Every change went through adversarial review lenses (risk, reliability, resilience) before commit. The history is committed as work units, each with its tests (`git log`).

## What I prioritised and why

In this order:

1. **Ingestion that is safe to rerun.** Every later layer trusts the raw layer. If a crash or a repeated delivery can double-count rows, no model on top can be trusted.
2. **Tenant isolation that is enforced, not intended.** A leak between clients is the one failure that cannot be fixed after the fact.
3. **Modelling:** typed, de-duplicated staging and daily marts.
4. **Late arrivals:** separating what a client was told from what the data now says.
5. **Missing-source detection:** knowing that a batch never arrived.

### Replay

| Mechanism | Where |
|-----------|-------|
| One transaction per file: ledger row and raw rows commit together or not at all | `loadBytes` in `src/ingest/loader.ts` |
| `ops.batch_file` is an attempt ledger: one row per attempt, loaded or quarantined, with sha256, row count and reasons | `migrations/003_ops_batch_file.sql` |
| Partial unique index `batch_file_loaded_once` on `(tenant_id, source, sha256) WHERE status = 'loaded'`: loaded bytes can never be loaded twice, but a quarantined file can be retried after a config fix | same migration |
| Transaction-scoped advisory lock per tenant and source, so the ledger check and the insert cannot interleave with another run | `loadBytes` |
| Fault injection with `LOADER_FAIL_AFTER_FILES`, used by the crash test | `loadBatches` |

Each file is classified against what is already loaded for that tenant and source:

| Incoming file | Outcome |
|---------------|---------|
| Same bytes, same batch number | `skipped` |
| Same batch number, different bytes | `quarantined`, `batch_conflict` |
| Same bytes, different batch number | `quarantined`, `duplicate_content` |
| New batch, headers resolve | `loaded` |
| New batch, headers do not resolve | `quarantined`, with the header reasons |

The crash test (`test/loader.test.ts`) stops the run inside the 8th file's transaction, checks that the 7 committed files have exactly the rows on disk and that no raw rows of the 8th file remain, then reruns (7 skipped, 32 loaded, 1 missing) and runs a third time (39 skipped).

Overlapping exports are a different problem from replays. Northwind's orders batch 3 repeats 14 orders dated 2026-01-17 that batch 2 already delivered. The bytes are different, so the loader correctly loads both files, and the overlap is resolved in staging: one row per `order_id`, latest batch wins. Daily gross then matches `finance_summary.csv` to the cent. The test asserts both directions: it matches with de-duplication and does not match without it.

### Schema drift

Only drift the config declares is adapted. Both tenants' `ad_spend` column `spend` is renamed to `cost_usd` from batch 4, and both tenant files declare `"spend": ["cost_usd"]`. Any other change quarantines the whole file with reasons in the ledger: an undeclared header, a missing column, two headers resolving to one column, or a repeated header.

Noticing drift is structural, not a heuristic. Every header must resolve to exactly one canonical column and every canonical column must be present, so an unexpected change cannot pass silently. The resolved header map is stored per file in the ledger (`detail.columns`), and staging reads columns through it. A later alias change therefore never reinterprets a file that is already loaded.

### Missing sources

`pnpm check` compares each tenant's manifest with its ledger and reports `not_received`, `quarantined` and `stale` batches, a loaded batch whose later redelivery with other content was quarantined (`conflicting_redelivery`), and mismatches between config and manifest (`no_manifest_entries`, `source_not_configured`). Exit codes are 0 healthy, 2 findings, 1 check failed, so a scheduler can alert on them.

Lumen's `ad_spend` batch 3 is listed in the manifest and absent from the fixtures, and the check reports it. A freshness check alone would miss it, because batches 4 and 5 loaded and the source looks current.

### Late arrivals

The marts are live views over everything loaded, so a late file changes them silently. What a client was told is kept separately (`src/report/publish.ts`, `migrations/009_published_reports.sql`):

- `pnpm report` publishes each (mart, day, dimensions) key as a version in `ops.published_metric`. A new key gets version 1. A changed key gets the next version plus an `ops.restatement` row with `before`, `after` and `caused_by`, the batch files behind the change. An unchanged key gets nothing, and a run with no changes writes nothing.
- `marts.reported_metric` is the latest version of each key: what the client was told.
- `caused_by` is the loaded files feeding that day now that the previous version's run had not seen (`ops.report_run` records which files each run saw). When a change comes only from rows leaving a day, it falls back to every newer file of the mart's sources, which is approximate; the JSDoc says so.
- Metrics are compared as exact numeric strings. One transaction per tenant, under a per-tenant advisory lock, so a publication is complete or absent. The tables are append-only for the application role: `SELECT` and `INSERT` only.

Evidence from the fixtures (`test/report.test.ts`: batches 1-4, publish, batch 5, publish):

- Northwind: 18 restatements. Six are `daily_email_engagement` for exactly 2026-01-12 to 2026-01-17, all caused by `email_events/batch_05.ndjson`, which carries 24 events for days batch 2 already covered. For example, 2026-01-13 opens went from 18 to 20, clicks from 10 to 11, unsubscribes from 15 to 16. The other 12 are `daily_revenue`.
- Lumen: 7 restatements, all `daily_revenue`. Refunds are dated after their batch window in every refunds file, so a day can first be published with refunds only, and is restated when its orders arrive.
- Days batch 5 did not touch are never restated.

### Tenant isolation

- Every tenant table has `FORCE ROW LEVEL SECURITY` with one policy keyed on `current_setting('app.tenant_id', true)`. `withTenant` sets it with `set_config(..., true)`, so it is transaction-local and cannot leak through a pooled connection.
- The owner role that runs migrations is neither superuser nor `BYPASSRLS` (`docker/initdb/001_owner.sql`), so `FORCE` applies to it too. Seeding writes each tenant's rows inside that tenant's scope.
- The app role owns nothing, and only has `SELECT` or `INSERT` where it needs it. It cannot write the tenant registry or value maps, and it cannot update or delete ledger rows.
- Every staging and marts view is `security_invoker`, so RLS is checked as the querying role, not as the view owner.
- Value maps are tenant data (`ops.value_map`), not code. No SQL or TypeScript branches on a tenant name.
- Tests prove that an unscoped connection sees zero rows through every table and view, that a scoped tenant never sees another's rows, and that writes for another tenant are rejected (`test/isolation.test.ts`, `test/staging.test.ts`).

## What I deliberately did not build and why

| Not built | Why |
|-----------|-----|
| Currency conversion (FX) | The data does not say which rate or which date to use, and lumen's labels are wrong anyway (see below). Amounts are reported in the tenant's configured currency, unconverted. |
| Ingesting `finance_summary.csv` | It is the client's claim, not a source of events. It is used as a reconciliation target in tests. Its `net_reported` cannot be explained (see below). |
| Scheduler and alerting | `pnpm check` exposes exit codes and a stable text format; wiring that to a scheduler is environment-specific. |
| Streaming large files | Files are read whole into memory. The fixtures are small, and per-file atomicity was the priority. Inserts are already chunked (`INSERT_CHUNK_SIZE`). |
| Accepting a `batch_conflict` as a restatement of a whole batch | It needs an operator decision. Today the new bytes are held in quarantine with the reason. |
| Flags for unmapped values | A value with no map entry passes through unchanged. An unknown email event type is then not counted by any engagement column, and nothing flags it. |
| Materialized marts, indexes for volume | Views are correct and fast enough for fixture-sized data. |

## How the third client gets added

Configuration and data only; no code, no model changes, no branching on names. The step-by-step is in [docs/adding-a-tenant.md](docs/adding-a-tenant.md).

1. Add `tenants/<id>.json`. It is zod-validated: `id`, `displayName`, `currency`, `fixturesDir`, and per source `columnAliases` and `valueMaps`.
2. Put the batch files under the tenant's `fixturesDir`, and add one entry per batch to `fixtures/manifest.json`.
3. `pnpm migrate` (seeds the tenant row and its value maps), then `pnpm load`, `pnpm check`, `pnpm report`.

The test "a third tenant added by configuration only" in `test/staging.test.ts` does exactly this: a GBP tenant with its own aliases (`total_amount`, `event`, `cost`) and value maps, loaded through the same loader and read through the same views.

## What I would do with another week

In rough priority order:

1. Publish only days up to what `pnpm check` reports as fresh, so a day that so far has only refunds (dated ahead of their batch window) is not published before its orders arrive. All 7 of lumen's restatements are this case.
2. An operator flow that accepts a `batch_conflict` as a restatement of a whole batch, which today stays in quarantine and is flagged as `conflicting_redelivery`.
3. Flag unmapped values in staging (for example in `staging.invalid_rows`), so a new channel or event type is visible instead of silently uncounted.
4. Store `covers_from` and `covers_to` in the ledger, so the check does not depend on the current manifest to know what a loaded batch covered.
5. Stream large files instead of buffering them whole.
6. Set `lock_timeout` and TCP keepalive on the pools, add `'error'` listeners on pooled clients, and handle `EPIPE` on stdout in the CLIs.
7. `ALTER DEFAULT PRIVILEGES` so future views get their grants without a migration having to remember.
8. Indexes and materialized marts once data volume needs them.
9. Scheduler and alerting integration around `pnpm check` and `pnpm report`.
10. FX conversion, once there is a decided rate source.

## The hardest thing I hit

Deciding what "I have already seen this file" means.

A sha256 per file makes replay look solved: same hash, skip. It is not, because the hash and the batch number can agree or disagree independently, and each combination means something different: a replay, a client changing a batch it already delivered, or a re-send that would count every row twice (the classification table under [Replay](#replay)). And there is a fourth case that is not a replay at all: an export whose rows overlap an earlier batch inside different bytes.

That fourth case was the one that mattered most for the numbers. Northwind's orders batch 3 repeats 14 orders from batch 2 inside different bytes, so no file-level check can catch it; it has to be resolved where rows have keys. Daily gross matches `finance_summary.csv` only after that de-duplication, and the tests assert both directions.

My first version got the re-send case wrong: it matched on the hash alone, so a batch that re-sent another batch's bytes was reported as `skipped` and left no trace in the ledger. A review pass caught it before it was committed. The fix was to require both the hash and the batch number for a skip, quarantine the other cases with a reason, and serialize attempts per tenant and source so two runs cannot race past the check.

The principle I ended up with: every outcome is a row in the ledger with a reason, never only a log line, and the unique index guards only loaded bytes, so a quarantined file can still be retried after a config fix.

## Decisions the data could not settle

| Question | What the data shows | Decision |
|----------|--------------------|----------|
| Which currency is lumen in? | Lumen orders say `EUR`; its `finance_summary.csv` labels the same amounts `USD` | Currency comes from tenant config (`EUR`). File labels are kept as `reported_currency` but not trusted. No conversion. |
| What is `net_reported`? | It cannot be derived from gross minus refunds: on 17 of 30 days per tenant it is greater than gross | Not reconciled. Our `net` is gross minus non-orphan refunds, on a cash basis (the day of the refund). |
| Is `cost_usd` in USD? | The `ad_spend` column was renamed to `cost_usd` for both tenants, and lumen is EUR | Treated as a rename, not a conversion. Spend is assumed to be in the tenant currency; flagged here and in `migrations/007_marts.sql`. |
| What are refunds for order `NO-00000000` / `LU-00000000`? | 6 refunds per tenant point to an order that does not exist | Kept, excluded from refunds and net, reported separately as `orphan_refund_count` and `orphan_refund_amount`. |
| Are refunds in scope? | The brief names three sources; the fixtures include `refunds/` | Ingested as a fourth source, since net revenue needs them. |
