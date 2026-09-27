# Tradeoffs

Short version: the loading path (replay, drift, isolation) is built deep and tested against a real database (over 120 tests). Modelling, late arrivals and missing-source detection are built on top of it and tested, with the known gaps listed [below](#known-gaps). Currency conversion, scheduling and large-file streaming are not built, on purpose.

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

Declared drift is adapted, but not silently: `pnpm load` names each header read through an alias (`loaded: lumen/ad_spend/batch 4 (18 rows; header cost_usd read as spend)`), and `pnpm check` prints a note from the ledger (`note: ad_spend batches 4-5 read header "cost_usd" as spend (declared alias)`). Notes never change the exit code, because the drift is expected.

### Missing sources

`pnpm check` compares each tenant's manifest with its ledger and reports `not_received`, `quarantined` and `stale` batches, a loaded batch whose later redelivery with other content was quarantined (`conflicting_redelivery`), and mismatches between config and manifest (`no_manifest_entries`, `source_not_configured`). It also reads staging for two data-quality findings: `invalid_rows` (values that are missing or fail their cast, in lines that still reach the marts) and `uncounted_values` (event types outside `delivered`, `open`, `click`, `unsubscribe`, which the marts never count). A bad line a later batch superseded is a note that never changes the exit code, so a corrected row clears the finding; `staging.invalid_rows` keeps the full history. An order with an unreadable `gross` is left out of `orders` as well as the sums, although a day where every order is unreadable still shows a `daily_revenue` row with `orders` 0 and `gross` 0. Exit codes are 0 healthy, 2 findings, 1 check failed, so a scheduler can alert on them.

Lumen's `ad_spend` batch 3 is listed in the manifest and absent from the fixtures, and the check reports it. A freshness check alone would miss it, because batches 4 and 5 loaded and the source looks current.

`pnpm report` uses the same comparison as a completeness gate (`src/ingest/coverage.ts`): it withholds a mart key whose day falls inside the window of a manifest batch with no loaded ledger row (not received, or only quarantined), for a source in that mart's lineage. For lumen that is `daily_channel_performance` for 2026-01-18 to 2026-01-23, 24 keys, which would otherwise have been published with spend 0 and ROAS `NULL`. Revenue and email engagement for those days do not depend on `ad_spend` and are published. Each run records the windows and keys it held back in `ops.report_run.withheld` (`migrations/010_report_run_withheld.sql`).

### Late arrivals

The marts are live views over everything loaded, so a late file changes them silently. What a client was told is kept separately (`src/report/publish.ts`, `migrations/009_published_reports.sql`):

- `pnpm report` publishes each (mart, day, dimensions) key as a version in `ops.published_metric`. A new key gets version 1. A changed key gets the next version plus an `ops.restatement` row with `before`, `after` and `caused_by`, the batch files behind the change. An unchanged key gets nothing, and a run with no changes writes nothing.
- `marts.reported_metric` is the latest version of each key: what the client was told.
- `caused_by` is the loaded files feeding that day now that the previous version's run had not seen (`ops.report_run` records which files each run saw). When a change comes only from rows leaving a day, it falls back to every newer file of the mart's sources, which is approximate; the JSDoc says so.
- Metrics are compared as exact numeric strings. One transaction per tenant, under a per-tenant advisory lock, so a publication is complete or absent. The tables are append-only for the application role: `SELECT` and `INSERT` only.

Withholding and restating answer different questions. A restatement corrects a number that was complete when it was published and changed because a late record arrived. A withheld key is one whose data is known to be incomplete right now, because a batch covering its day has not loaded. Publishing it and restating it later would tell the client a wrong number first, such as no ad spend, so it gets no version, no restatement and no tombstone until the batch loads, and then is published as usual: version 1 if it was never published, a restatement if its numbers moved. The gate cannot undo a number published before its window became incomplete; that version stays the latest until the batch loads (see [Known gaps](#known-gaps)).

Evidence from the fixtures (`test/report.test.ts`: batches 1-4, publish, batch 5, publish; each report is given the manifest of what had been delivered by then):

- Northwind: 18 restatements. Six are `daily_email_engagement` for exactly 2026-01-12 to 2026-01-17, all caused by `email_events/batch_05.ndjson`, which carries 24 events for days batch 2 already covered. For example, 2026-01-13 opens went from 18 to 20, clicks from 10 to 11, unsubscribes from 15 to 16. The other 12 are `daily_revenue`.
- Lumen: 7 restatements, all `daily_revenue`. Its 24 `daily_channel_performance` keys for 2026-01-18 to 2026-01-23 are withheld in both runs, because `ad_spend` batch 3 never arrives.
- Days batch 5 did not touch are never restated.

The revenue restatements follow from how refunds are dated. `daily_revenue` puts a refund on the day it was refunded (cash basis), and a refunds batch is not limited to its window: lumen's `refunds/batch_05.csv` covers 2026-01-30 to 02-04 but carries refunds dated January 9, 10, 11 and 28. Earlier refund files also carry refunds dated after their window, for orders that only arrive in orders batch 5; until then those refunds have no order and count as orphans. I derived the breakdown from the fixture files with a read-only script (same de-duplication and orphan rule as the views), and it matches the test's counts:

| Why the day changed | Lumen | Northwind |
|---------------------|-------|-----------|
| A late refund for an order already reported: `refunds` and `net` change | 2026-01-09 (+70.44, order `LU-29461135` from orders batch 1), 2026-01-28 (+101.62) | 2026-01-22 (+28.61) |
| A late orphan refund (order `*-00000000`): the orphan count and amount change | 2026-01-10, 2026-01-11 | 2026-01-10, 2026-01-11 |
| Batch 5 orders arrive for a day published with refunds only; on most of these days, refunds counted as orphans move into `refunds` | 2026-02-02, 2026-02-03 | 2026-01-30, 2026-01-31, 2026-02-02, 2026-02-03, 2026-02-04 |
| A refund-only day after the last order date; its refund stops being an orphan once batch 5 brings the order | 2026-02-10 | 2026-02-05, 2026-02-07 (plus a batch 5 refund), 2026-02-09, 2026-02-11 |
| Total | 7 | 12 |

The first two rows are the case this feature exists for: a number the client was already told changed because a genuinely late record arrived. The last two rows show a modelling gap: before batch 5, "orphan" meant both "this order does not exist" and "this order has not arrived yet". The orphan counts published for those days were wrong at the time, not just incomplete.

Why restate and keep an audit trail rather than the alternatives. Posting the change as an adjustment in the current period keeps old reports frozen, but the daily marts are per-day facts that a client reconciles against their own daily finance summary, so the day itself should be right. Closing periods and never touching a closed day is what finance teams do eventually, but it needs someone to decide when a period closes. So the day is corrected, and `ops.published_metric` and `ops.restatement` keep what the client was told and why it changed. A period close would be the next step, as a closed flag on `ops.report_run` or `ops.published_metric` that turns a restatement of a closed day into an adjustment instead.

### Tenant isolation

- Every tenant table has `FORCE ROW LEVEL SECURITY` with one policy keyed on `current_setting('app.tenant_id', true)`. `withTenant` sets it with `set_config(..., true)`, so it is transaction-local and cannot leak through a pooled connection.
- The owner role that runs migrations is neither superuser nor `BYPASSRLS` (`docker/initdb/001_owner.sql`), so `FORCE` applies to it too. Seeding writes each tenant's rows inside that tenant's scope.
- The app role owns nothing, and only has `SELECT` or `INSERT` where it needs it. It cannot write the tenant registry or value maps, and it cannot update or delete ledger rows.
- Every staging and marts view is `security_invoker`, so RLS is checked as the querying role, not as the view owner.
- Value maps are tenant data (`ops.value_map`), not code. No SQL or TypeScript branches on a tenant name.
- Tests prove that an unscoped connection sees zero rows through every table and view, that a scoped tenant never sees another's rows, and that writes for another tenant are rejected (`test/isolation.test.ts`, `test/staging.test.ts`).

### Who uses what

| Actor | What it does | Tenants it acts for |
|-------|--------------|---------------------|
| The pipeline (`pnpm load`, `check`, `report`) | Ingests, checks and publishes, as the app role | All of them: one service serves every tenant |
| The operator (data team) | Runs the pipeline and acts on `pnpm check` | All of them |
| Whoever onboards a client | Writes `tenants/<id>.json`, the fixtures and manifest entries | The new one |
| The client (the brand) | Consumes its numbers, which is who the marts and `marts.reported_metric` are for | Only its own |

RLS guarantees that every query is scoped and that a scoped session cannot read or write another tenant's rows. It does not decide who may choose the scope: whoever can connect as the app role can set any `app.tenant_id`. That is acceptable only because the app role is used by the pipeline, which serves every tenant by design and derives the tenant of each file from the manifest and tenant config, with manifest paths checked against the tenant's `fixturesDir`.

So clients must never connect as the app role. Client access is not built here; the brief asks for the ingestion and modelling layer. It needs one of two designs:

- A database role per client, with policies bound to the login identity (`current_user`) rather than to a setting the client could change.
- An access layer (an API or a BI tool) that derives the tenant from the authenticated identity, never from a parameter the client sends.

## What I deliberately did not build and why

| Not built | Why |
|-----------|-----|
| Currency conversion (FX) | The data does not say which rate or which date to use, and lumen's labels are wrong anyway (see below). Amounts are reported in the tenant's configured currency, unconverted. |
| Ingesting `finance_summary.csv` | It is the client's claim, not a source of events. It is used as a reconciliation target in tests. Its `net_reported` cannot be explained (see below). |
| Scheduler and alerting | `pnpm check` exposes exit codes and a stable text format; wiring that to a scheduler is environment-specific. |
| Streaming large files | Files are read whole into memory. The fixtures are small, and per-file atomicity was the priority. Inserts are already chunked (`INSERT_CHUNK_SIZE`). |
| Accepting a `batch_conflict` as a restatement of a whole batch | It needs an operator decision. Today the new bytes are held in quarantine with the reason. |
| Materialized marts, indexes for volume | Views are correct and fast enough for fixture-sized data. |

### Known gaps

These are built far enough to work on the fixtures, but each has a hole I know about and did not close in the time I had.

| Gap | What happens today | What would close it |
|-----|--------------------|---------------------|
| A number published before its window became incomplete stays visible | `pnpm report` withholds keys inside the window of a batch that has not loaded, but it cannot take back a version published earlier: that version stays the latest until the batch loads. The gate also reads the manifest (`fixtures/manifest.json` or `MANIFEST=`), so a tenant absent from it gets no withholding, and the ledger does not record what a loaded batch covered. | A "retract" or "incomplete" marker version for a key whose window became incomplete, and `covers_from` and `covers_to` stored in the ledger. |
| A missing value-map entry is caught only after loading | Config validation rejects a non-canonical target for `email_events.type` (`value map target "opened" for email_events.type is not one of delivered, open, click, unsubscribe`), but it cannot know which raw values a file will carry. A missing entry shows up as `uncounted_values` in `pnpm check`. `channel` and `platform` are open on purpose, so a new channel stays configuration only, and a typo there becomes a channel of its own without a finding. | Config alone cannot: for `email_events.type`, `pnpm check` is the guard. For open columns, a finding for a value seen for the first time. |
| A day of unreadable orders is published as zero | An order with an unreadable `gross` is left out of `orders` and the sums, and `pnpm check` reports it. If every order of a day is unreadable, `daily_revenue` still has a row for that day with `orders` 0 and `gross` 0, and `pnpm report` publishes it as if nothing was sold. | Have the completeness gate also withhold days whose orders are all invalid, the same way it withholds days of a batch that never loaded. |
| Drift beyond headers | An added column, even a harmless one, quarantines the whole file: a strict policy, chosen so that nothing unexpected loads, but with no way to accept additive columns. A column that keeps its name but changes meaning or unit (`cost_usd` for a tenant billed in EUR) cannot be detected from the data; it is recorded as a decision in [Decisions the data could not settle](#decisions-the-data-could-not-settle). | A per-source policy to accept and record additive columns, and a first-seen finding for new values in open columns (see the missing value-map entry row). |
| The pipeline chooses the tenant it acts for | The database blocks unscoped and cross-tenant queries, but any session of the app role can set any `app.tenant_id`. That is the trust boundary of a single service that serves every tenant (see [Who uses what](#who-uses-what)), not a hole in the policies. | Per-tenant credentials held by separate per-tenant workers, so one tenant's worker cannot authenticate as another's. `SET ROLE` per tenant from one shared login would not close it, because that login could still switch to any tenant. It is a deployment change (workers, secrets), which is why I stopped at the database guard. |
| A value-map change restates history without a cause | Editing a value map changes staging, so the next `pnpm report` restates past days with an empty `caused_by`: the change came from config, not from a file. | Record config versions per report run and cite them in `caused_by`. |
| "Orphan" also means "not arrived yet" | A refund whose order is in a later batch counts as an orphan until that batch loads (see [Late arrivals](#late-arrivals)). | Report refunds for unknown orders as pending until the order's window has been checked as complete. |

## How the third client gets added

Configuration and data only; no code, no model changes, no branching on names. The step-by-step is in [docs/adding-a-tenant.md](docs/adding-a-tenant.md).

1. Add `tenants/<id>.json`. It is zod-validated: `id`, `displayName`, `currency`, `fixturesDir`, and per source `columnAliases` and `valueMaps`.
2. Put the batch files under the tenant's `fixturesDir`, and add one entry per batch to `fixtures/manifest.json`.
3. `pnpm migrate` (seeds the tenant row and its value maps), then `pnpm load`, `pnpm check`, `pnpm report`.

The test "a third tenant added by configuration only" in `test/staging.test.ts` does exactly this: a GBP tenant with its own aliases (`total_amount`, `event`, `cost`) and value maps, loaded through the same loader and read through the same views.

## What I would do with another week

The first items on the earlier version of this list are built, with the gaps above still open: the completeness gate on publishing, value-map target validation with the `invalid_rows` and `uncounted_values` findings, and reporting of headers read through a declared alias. What remains, in rough priority order:

1. Close the completeness gate's remaining hole: a "retract" or "incomplete" marker for a key published before its window became incomplete, and `covers_from` and `covers_to` stored in the ledger, so the gate and the check no longer depend on the current manifest to know what a loaded batch covered. (Publishing only "fresh" days, which I had planned first, would hold back the refund-only days at the end of the window, but not the late refunds for days weeks back. Those are exactly what restatements are for.)
2. An operator flow that accepts a `batch_conflict` as a restatement of a whole batch, which today stays in quarantine and is flagged as `conflicting_redelivery`.
3. Cite config changes in `caused_by`.
4. Stream large files instead of buffering them whole.
5. Set `lock_timeout` and TCP keepalive on the pools, add `'error'` listeners on pooled clients, and handle `EPIPE` on stdout in the CLIs.
6. `ALTER DEFAULT PRIVILEGES` so future views get their grants without a migration having to remember.
7. Indexes and materialized marts once data volume needs them.
8. Scheduler and alerting integration around `pnpm check` and `pnpm report`.
9. FX conversion, once there is a decided rate source.

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
